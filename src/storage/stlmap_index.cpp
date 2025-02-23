#include "storage/stlmap_index.hpp"

namespace babydb {

StlmapIndex::StlmapIndex(const std::string &name, Table &table, idx_t key_position)
    : Index(name, table, key_position) {
    auto read_guard = table.GetReadTableGuard();
    auto &rows = read_guard.rows_;
    for (idx_t row_id = 0; row_id < rows.size(); row_id++) {
        auto &row = rows[row_id];
        if (!row.tuple_meta_.is_deleted_) {
            InsertEntry(row.tuple_.KeyFromTuple(key_position_), row_id);
        }
    }
}

void StlmapIndex::InsertEntry(const data_t &key, idx_t row_id) {
    if (index_.find(key) != index_.end()) {
        throw std::logic_error("duplicated key");
    }
    index_[key] = row_id;
};

void StlmapIndex::EraseEntry(const data_t &key, idx_t row_id) {
    index_.erase(key);
};

idx_t StlmapIndex::ScanKey(const data_t &key) {
    auto ite = index_.find(key);
    if (ite == index_.end()) {
        return INVALID_ID;
    }
    return ite->second;
}

StlmapIndex::Iterator StlmapIndex::Lowerbound(data_t key) {
    return index_.lower_bound(key);
}

StlmapIndex::Iterator StlmapIndex::End() {
    return index_.end();
}

}