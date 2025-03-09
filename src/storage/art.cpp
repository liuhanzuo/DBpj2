#include "storage/art.hpp"

#include <cassert>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <stdexcept>
#include <algorithm>
#include <emmintrin.h> // SSE2 intrinsics
#include <vector>

namespace babydb {

namespace ART {

enum NodeType : uint8_t {
    NodeType4   = 0,
    NodeType16  = 1,
    NodeType48  = 2,
    NodeType256 = 3
};

static const uint32_t maxPrefixLength = 9;
static const uint8_t emptyMarker = 48;

// Shared structure for each type of tree nodes on ART
struct Node {
    uint32_t prefixLength;
    uint16_t count;
    NodeType type;
    uint8_t  prefix[maxPrefixLength];

    Node(NodeType t) : prefixLength(0), count(0), type(t) {}
};

struct Node4 : Node {
    uint8_t key[4];
    Node* child[4];

    Node4() : Node(NodeType4) {
        std::memset(key, 0, sizeof(key));
        std::memset(child, 0, sizeof(child));
    }
};

struct Node16 : Node {
    uint8_t key[16];
    Node* child[16];

    Node16() : Node(NodeType16) {
        std::memset(key, 0, sizeof(key));
        std::memset(child, 0, sizeof(child));
    }
};

struct Node48 : Node {
    uint8_t childIndex[256];
    Node* child[48];

    Node48() : Node(NodeType48) {
        std::memset(childIndex, emptyMarker, sizeof(childIndex));
        std::memset(child, 0, sizeof(child));
    }
};

struct Node256 : Node {
    Node* child[256];

    Node256() : Node(NodeType256) {
        std::memset(child, 0, sizeof(child));
    }
};

inline Node* makeLeaf(uintptr_t tid) {
    return reinterpret_cast<Node*>((tid << 1) | 1);
}
inline uintptr_t getLeafValue(Node* node) {
    return reinterpret_cast<uintptr_t>(node) >> 1;
}
inline bool isLeaf(Node* node) {
    return reinterpret_cast<uintptr_t>(node) & 1;
}

uint8_t flipSign(uint8_t keyByte) {
    return keyByte ^ 128;
}

void loadKey(uintptr_t tid, uint8_t key[8]) {
    reinterpret_cast<uint64_t*>(key)[0] = __builtin_bswap64(tid);
}

static inline uint32_t ctz(uint16_t x) {
#ifdef __GNUC__
    return __builtin_ctz(x);
#else
    uint32_t n = 1;
    if ((x & 0xFF) == 0) { n += 8; x >>= 8; }
    if ((x & 0x0F) == 0) { n += 4; x >>= 4; }
    if ((x & 0x03) == 0) { n += 2; x >>= 2; }
    return n - (x & 1);
#endif
}

Node** findChild(Node* n, uint8_t keyByte) {
    switch (n->type) {
        case NodeType4: {
            Node4* node = static_cast<Node4*>(n);
            for (idx_t i = 0; i < node->count; i++) {
                if (node->key[i] == keyByte) {
                    return &node->child[i];
                }
            }
            break;
        }
        case NodeType16: {
            Node16* node = static_cast<Node16*>(n);
            __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(keyByte)),
                                         _mm_loadu_si128(reinterpret_cast<__m128i*>(node->key)));
            uint32_t bitfield = _mm_movemask_epi8(cmp) & ((1 << node->count) - 1);
            if (bitfield) {
                return &node->child[ctz(bitfield)];
            }
            break;
        }
        case NodeType48: {
            Node48* node = static_cast<Node48*>(n);
            if (node->childIndex[keyByte] != emptyMarker) {
                return &node->child[node->childIndex[keyByte]];
            }
            break;
        }
        case NodeType256: {
            Node256* node = static_cast<Node256*>(n);
            return &node->child[keyByte];
        }
        default: {
            B_ASSERT_MSG(false, "Invalid Node Type in ART");
        }
    }
    static Node* nullNode = nullptr;
    return &nullNode;
}

Node* minimum(Node* node) {
    if (node == nullptr) {
        return nullptr;
    }
    if (isLeaf(node)) {
        return node;
    }
    switch (node->type) {
        case NodeType4: {
            Node4* n = static_cast<Node4*>(node);
            return minimum(n->child[0]);
        }
        case NodeType16: {
            Node16* n = static_cast<Node16*>(node);
            return minimum(n->child[0]);
        }
        case NodeType48: {
            Node48* n = static_cast<Node48*>(node);
            uint32_t pos = 0;
            while (n->childIndex[pos] == emptyMarker) {
                pos++;
            }
            return minimum(n->child[n->childIndex[pos]]);
        }
        case NodeType256: {
            Node256* n = static_cast<Node256*>(node);
            uint32_t pos = 0;
            while (!n->child[pos]) {
                pos++;
            }
            return minimum(n->child[pos]);
        }
    }
    return nullptr;
}

Node* maximum(Node* node) {
    if (node == nullptr) {
        return nullptr;
    }
    if (isLeaf(node)) {
        return node;
    }
    switch(node->type) {
        case NodeType4: {
            Node4* n = static_cast<Node4*>(node);
            return maximum(n->child[n->count - 1]);
        }
        case NodeType16: {
            Node16* n = static_cast<Node16*>(node);
            return maximum(n->child[n->count - 1]);
        }
        case NodeType48: {
            Node48* n = static_cast<Node48*>(node);
            uint32_t pos = 255;
            while (n->childIndex[pos] == emptyMarker) {
                pos--;
            }
            return maximum(n->child[n->childIndex[pos]]);
        }
        case NodeType256: {
            Node256* n = static_cast<Node256*>(node);
            uint32_t pos = 255;
            while (!n->child[pos]) {
                pos--;
            }
            return maximum(n->child[pos]);
        }
    }
    return nullptr;
}

bool leafMatches(Node* leaf, uint8_t key[8], uint32_t keyLength, uint32_t depth, uint32_t maxKeyLength) {
    if (depth != keyLength) {
        uint8_t leafKey[maxKeyLength];
        loadKey(getLeafValue(leaf), leafKey);
        for (idx_t i = depth; i < keyLength; i++) {
            if (leafKey[i] != key[i]) {
                return false;
            }
        }
    }
    return true;
}

uint32_t prefixMismatch(Node* node, uint8_t key[8], uint32_t depth, uint32_t maxKeyLength) {
    uint32_t pos;
    if (node->prefixLength > maxPrefixLength) {
        for (pos = 0; pos < maxPrefixLength; pos++) {
            if (key[depth + pos] != node->prefix[pos]) {
                return pos;
            }
        }
        uint8_t minKey[maxKeyLength];
        loadKey(getLeafValue(minimum(node)), minKey);
        for (; pos < node->prefixLength; pos++) {
            if (key[depth + pos] != minKey[depth + pos]) {
                return pos;
            }
        }
    } else {
        for (pos = 0; pos < node->prefixLength; pos++) {
            if (key[depth + pos] != node->prefix[pos]) {
                return pos;
            }
        }
    }
    return pos;
}

Node* lookup(Node* node, uint8_t key[8], uint32_t keyLength, uint32_t depth, uint32_t maxKeyLength) {
    bool skippedPrefix = false;
    while (node != nullptr) {
        if (isLeaf(node)) {
            if (!skippedPrefix && depth == keyLength) {
                return node;
            }
            if (depth != keyLength) {
                uint8_t leafKey[maxKeyLength];
                loadKey(getLeafValue(node), leafKey);
                for (idx_t i = (skippedPrefix ? 0 : depth); i < keyLength; i++) {
                    if (leafKey[i] != key[i]) {
                        return nullptr;
                    }
                }
            }
            return node;
        }
        if (node->prefixLength) {
            if (node->prefixLength < maxPrefixLength) {
                for (uint32_t pos = 0; pos < node->prefixLength; pos++) {
                    if (key[depth + pos] != node->prefix[pos]) {
                        return nullptr;
                    }
                }
            } else {
                skippedPrefix = true;
            }
            depth += node->prefixLength;
        }
        Node** child = findChild(node, key[depth]);
        node = *child;
        depth++;
    }
    return nullptr;
}


void insert(Node* node, Node** nodeRef, uint8_t key[8], uint32_t depth, uintptr_t value, uint32_t maxKeyLength);
void insertNode4(Node4* node, Node** nodeRef, uint8_t keyByte, Node* child);
void insertNode16(Node16* node, Node** nodeRef, uint8_t keyByte, Node* child);
void insertNode48(Node48* node, Node** nodeRef, uint8_t keyByte, Node* child);
void insertNode256(Node256* node, Node** nodeRef, uint8_t keyByte, Node* child);

void insert(Node* node, Node** nodeRef, uint8_t key[8], uint32_t depth, uintptr_t value, uint32_t maxKeyLength) {
    if (node == nullptr) {
        *nodeRef = makeLeaf(value);
        return;
    }
    if (isLeaf(node)) {
        uint8_t existingKey[maxKeyLength];
        loadKey(getLeafValue(node), existingKey);
        uint32_t newPrefixLength = 0;
        while (existingKey[depth + newPrefixLength] == key[depth + newPrefixLength]) {
            newPrefixLength++;
        }
        Node4* newNode = new Node4();
        newNode->prefixLength = newPrefixLength;
        std::memcpy(newNode->prefix, key + depth, std::min(newPrefixLength, maxPrefixLength));
        *nodeRef = newNode;
        insertNode4(newNode, nodeRef, existingKey[depth + newPrefixLength], node);
        insertNode4(newNode, nodeRef, key[depth + newPrefixLength], makeLeaf(value));
        return;
    }
    if (node->prefixLength) {
        uint32_t mismatchPos = prefixMismatch(node, key, depth, maxKeyLength);
        if (mismatchPos != node->prefixLength) {
            Node4* newNode = new Node4();
            *nodeRef = newNode;
            newNode->prefixLength = mismatchPos;
            std::memcpy(newNode->prefix, node->prefix, std::min(mismatchPos, maxPrefixLength));
            if (node->prefixLength < maxPrefixLength) {
                insertNode4(newNode, nodeRef, node->prefix[mismatchPos], node);
                node->prefixLength -= (mismatchPos + 1);
                std::memmove(node->prefix, node->prefix + mismatchPos + 1, std::min(node->prefixLength, maxPrefixLength));
            } else {
                node->prefixLength -= (mismatchPos + 1);
                uint8_t minKey[maxKeyLength];
                loadKey(getLeafValue(minimum(node)), minKey);
                insertNode4(newNode, nodeRef, minKey[depth + mismatchPos], node);
                std::memmove(node->prefix, minKey + depth + mismatchPos + 1, std::min(node->prefixLength, maxPrefixLength));
            }
            insertNode4(newNode, nodeRef, key[depth + mismatchPos], makeLeaf(value));
            return;
        }
        depth += node->prefixLength;
    }
    Node** child = findChild(node, key[depth]);
    if (*child) {
        insert(*child, child, key, depth + 1, value, maxKeyLength);
        return;
    }
    Node* newNode = makeLeaf(value);
    switch (node->type) {
        case NodeType4:
            insertNode4(static_cast<Node4*>(node), nodeRef, key[depth], newNode);
            break;
        case NodeType16:
            insertNode16(static_cast<Node16*>(node), nodeRef, key[depth], newNode);
            break;
        case NodeType48:
            insertNode48(static_cast<Node48*>(node), nodeRef, key[depth], newNode);
            break;
        case NodeType256:
            insertNode256(static_cast<Node256*>(node), nodeRef, key[depth], newNode);
            break;
    }
}

void insertNode4(Node4* node, Node** nodeRef, uint8_t keyByte, Node* child) {
    if (node->count < 4) {
        uint32_t pos;
        for (pos = 0; (pos < node->count) && (node->key[pos] < keyByte); pos++);
        std::memmove(node->key + pos + 1, node->key + pos, node->count - pos);
        std::memmove(node->child + pos + 1, node->child + pos, (node->count - pos) * sizeof(uintptr_t));
        node->key[pos] = keyByte;
        node->child[pos] = child;
        node->count++;
    } else {
        Node16* newNode = new Node16();
        *nodeRef = newNode;
        newNode->count = 4;
        std::memcpy(newNode->prefix, node->prefix, std::min(node->prefixLength, maxPrefixLength));
        for (idx_t i = 0; i < 4; i++)
            newNode->key[i] = flipSign(node->key[i]);
        std::memcpy(newNode->child, node->child, node->count * sizeof(uintptr_t));
        delete node;
        insertNode16(newNode, nodeRef, keyByte, child);
    }
}

void insertNode16(Node16* node, Node** nodeRef, uint8_t keyByte, Node* child) {
    if (node->count < 16) {
        uint8_t keyByteFlipped = flipSign(keyByte);
        __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped),
                                      _mm_loadu_si128(reinterpret_cast<__m128i*>(node->key)));
        uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - node->count));
        uint32_t pos = bitfield ? ctz(bitfield) : node->count;
        std::memmove(node->key + pos + 1, node->key + pos, node->count - pos);
        std::memmove(node->child + pos + 1, node->child + pos, (node->count - pos) * sizeof(uintptr_t));
        node->key[pos] = keyByteFlipped;
        node->child[pos] = child;
        node->count++;
    } else {
        Node48* newNode = new Node48();
        *nodeRef = newNode;
        std::memcpy(newNode->child, node->child, node->count * sizeof(uintptr_t));
        for (idx_t i = 0; i < node->count; i++) {
            newNode->childIndex[flipSign(node->key[i])] = i;
        }
        std::memcpy(newNode->prefix, node->prefix, std::min(node->prefixLength, maxPrefixLength));
        newNode->count = node->count;
        delete node;
        insertNode48(newNode, nodeRef, keyByte, child);
    }
}

void insertNode48(Node48* node, Node** nodeRef, uint8_t keyByte, Node* child) {
    if (node->count < 48) {
        uint32_t pos = node->count;
        while (node->child[pos]) {
            pos++;
        }
        node->child[pos] = child;
        node->childIndex[keyByte] = pos;
        node->count++;
    } else {
        Node256* newNode = new Node256();
        for (idx_t i = 0; i < 256; i++) {
            if (node->childIndex[i] != emptyMarker) {
                newNode->child[i] = node->child[node->childIndex[i]];
            }
        }
        newNode->count = node->count;
        std::memcpy(newNode->prefix, node->prefix, std::min(node->prefixLength, maxPrefixLength));
        *nodeRef = newNode;
        delete node;
        insertNode256(newNode, nodeRef, keyByte, child);
    }
}

void insertNode256(Node256* node, Node** nodeRef, uint8_t keyByte, Node* child) {
    node->count++;
    node->child[keyByte] = child;
}

void erase(Node* node, Node** nodeRef, uint8_t key[8], uint32_t keyLength, uint32_t depth, uint32_t maxKeyLength);
void eraseNode4(Node4* node, Node** nodeRef, Node** leafPlace);
void eraseNode16(Node16* node, Node** nodeRef, Node** leafPlace);
void eraseNode48(Node48* node, Node** nodeRef, uint8_t keyByte);
void eraseNode256(Node256* node, Node** nodeRef, uint8_t keyByte);

void erase(Node* node, Node** nodeRef, uint8_t key[8], uint32_t keyLength, uint32_t depth, uint32_t maxKeyLength) {
    if (node == nullptr)
        return;
    if (isLeaf(node)) {
        if (leafMatches(node, key, keyLength, depth, maxKeyLength)) {
            *nodeRef = nullptr;
        }
        return;
    }
    if (node->prefixLength) {
        if (prefixMismatch(node, key, depth, maxKeyLength) != node->prefixLength) {
            return;
        } else {
            depth += node->prefixLength;
        }
    }
    Node** child = findChild(node, key[depth]);
    if (child && isLeaf(*child) && leafMatches(*child, key, keyLength, depth, maxKeyLength)) {
        switch (node->type) {
            case NodeType4:
                eraseNode4(static_cast<Node4*>(node), nodeRef, child);
                break;
            case NodeType16:
                eraseNode16(static_cast<Node16*>(node), nodeRef, child);
                break;
            case NodeType48:
                eraseNode48(static_cast<Node48*>(node), nodeRef, key[depth]);
                break;
            case NodeType256:
                eraseNode256(static_cast<Node256*>(node), nodeRef, key[depth]);
                break;
        }
    } else {
        erase(*child, child, key, keyLength, depth + 1, maxKeyLength);
    }
}

void eraseNode4(Node4* node, Node** nodeRef, Node** leafPlace) {
    uint32_t pos = leafPlace - node->child;
    std::memmove(node->key + pos, node->key + pos + 1, node->count - pos - 1);
    std::memmove(node->child + pos, node->child + pos + 1, (node->count - pos - 1) * sizeof(uintptr_t));
    node->count--;
    if (node->count == 1) {
        Node* child = node->child[0];
        if (!isLeaf(child)) {
            uint32_t l1 = node->prefixLength;
            if (l1 < maxPrefixLength) {
                node->prefix[l1] = node->key[0];
                l1++;
            }
            if (l1 < maxPrefixLength) {
                uint32_t l2 = std::min(child->prefixLength, maxPrefixLength - l1);
                std::memcpy(node->prefix + l1, child->prefix, l2);
                l1 += l2;
            }
            std::memcpy(child->prefix, node->prefix, std::min(l1, maxPrefixLength));
            child->prefixLength += node->prefixLength + 1;
        }
        *nodeRef = child;
        delete node;
    }
}

void eraseNode16(Node16* node, Node** nodeRef, Node** leafPlace) {
    uint32_t pos = leafPlace - node->child;
    std::memmove(node->key + pos, node->key + pos + 1, node->count - pos - 1);
    std::memmove(node->child + pos, node->child + pos + 1, (node->count - pos - 1) * sizeof(uintptr_t));
    node->count--;
    if (node->count == 3) {
        Node4* newNode = new Node4();
        newNode->count = node->count;
        std::memcpy(newNode->prefix, node->prefix, std::min(node->prefixLength, maxPrefixLength));
        for (idx_t i = 0; i < newNode->count; i++) {
            newNode->key[i] = flipSign(node->key[i]);
        }
        std::memcpy(newNode->child, node->child, newNode->count * sizeof(uintptr_t));
        *nodeRef = newNode;
        delete node;
    }
}

void eraseNode48(Node48* node, Node** nodeRef, uint8_t keyByte) {
    node->child[node->childIndex[keyByte]] = nullptr;
    node->childIndex[keyByte] = emptyMarker;
    node->count--;
    if (node->count == 12) {
        Node16* newNode = new Node16();
        *nodeRef = newNode;
        for (idx_t b = 0; b < 256; b++) {
            if (node->childIndex[b] != emptyMarker) {
                newNode->key[newNode->count] = flipSign(b);
                newNode->child[newNode->count] = node->child[node->childIndex[b]];
                newNode->count++;
            }
        }
        delete node;
    }
}

void eraseNode256(Node256* node, Node** nodeRef, uint8_t keyByte) {
    node->child[keyByte] = nullptr;
    node->count--;
    if (node->count == 37) {
        Node48* newNode = new Node48();
        *nodeRef = newNode;
        for (idx_t b = 0; b < 256; b++) {
            if (node->child[b]) {
                newNode->childIndex[b] = newNode->count;
                newNode->child[newNode->count] = node->child[b];
                newNode->count++;
            }
        }
        delete node;
    }
}

void rangeScan(Node* node, uint8_t lowerKey[8], uint8_t upperKey[8], uint32_t keyLength,
               bool contain_start, bool contain_end, std::vector<babydb::idx_t>& row_ids) {
    // Add your code here
}

void destroy(Node* node) {
    if (!node) {
        return;
    }
    if (isLeaf(node)) {
        return;
    }
    switch (node->type) {
        case NodeType4: {
            Node4* n4 = static_cast<Node4*>(node);
            for (idx_t i = 0; i < n4->count; i++) {
                destroy(n4->child[i]);
            }
            break;
        }
        case NodeType16: {
            Node16* n16 = static_cast<Node16*>(node);
            for (idx_t i = 0; i < n16->count; i++) {
                destroy(n16->child[i]);
            }
            break;
        }
        case NodeType48: {
            Node48* n48 = static_cast<Node48*>(node);
            for (idx_t i = 0; i < 256; i++) {
                if (n48->childIndex[i] != emptyMarker) {
                    destroy(n48->child[n48->childIndex[i]]);
                }
            }
            break;
        }
        case NodeType256: {
            Node256* n256 = static_cast<Node256*>(node);
            for (idx_t i = 0; i < 256; i++) {
                if (n256->child[i]) {
                    destroy(n256->child[i]);
                }
            }
            break;
        }
    }
    delete node;
}

} // end ART namespace

using namespace ART;

ArtIndex::ArtIndex(const std::string &name, Table &table, const std::string &key_name)
    : RangeIndex(name, table, key_name), root_(nullptr) {
    auto read_guard = table.GetReadTableGuard();
    auto &rows = read_guard.Rows();
    auto key_attr = table.schema_.GetKeyAttr(key_name);
    for (idx_t row_id = 0; row_id < rows.size(); row_id++) {
        auto &row = rows[row_id];
        if (!row.tuple_meta_.is_deleted_) {
            data_t key = row.tuple_.KeyFromTuple(key_attr);
            InsertEntry(key, row_id);
        }
    }
}

ArtIndex::~ArtIndex() {
    destroy(reinterpret_cast<Node*>(root_));
}

void ArtIndex::InsertEntry(const data_t &key, idx_t /*row_id*/, idx_t /*start_ts*/) {
    if (ScanKey(key) != INVALID_ID) {
        throw std::logic_error("duplicated key");
    }
    uint8_t keyBytes[8];
    loadKey(key, keyBytes);
    insert(reinterpret_cast<Node*>(root_), reinterpret_cast<Node**>(&root_), keyBytes, 0, key, 8);
}

void ArtIndex::EraseEntry(const data_t &key, idx_t /*row_id*/, idx_t /*start_ts*/, idx_t /*end_ts*/) {
    uint8_t keyBytes[8];
    loadKey(key, keyBytes);
    erase(reinterpret_cast<Node*>(root_), reinterpret_cast<Node**>(&root_), keyBytes, 8, 0, 8);
}

idx_t ArtIndex::ScanKey(const data_t &key, idx_t /*start_ts*/, idx_t /*end_ts*/) {
    uint8_t keyBytes[8];
    loadKey(key, keyBytes);
    Node* leaf = lookup(reinterpret_cast<Node*>(root_), keyBytes, 8, 0, 8);
    if (leaf == nullptr || !isLeaf(leaf) || getLeafValue(leaf) != key)
        return INVALID_ID;
    return static_cast<idx_t>(getLeafValue(leaf));
}

void ArtIndex::ScanRange(const RangeInfo &range, std::vector<idx_t> &row_ids, idx_t /*start_ts*/, idx_t /*end_ts*/) {
    row_ids.clear();
    uint8_t lowerKey[8], upperKey[8];
    loadKey(range.start, lowerKey);
    loadKey(range.end, upperKey);

    rangeScan(reinterpret_cast<Node*>(root_), lowerKey, upperKey, 8, range.contain_start, range.contain_end, row_ids);
}

} // namespace babydb
