#include "storage/stlmap_index.hpp"

namespace babydb {

StlmapIndex::StlmapIndex(const std::string &name, Table &table, idx_t key_position)
    : Index(name, table, key_position) {
    auto read_guard = table.GetReadTableGuard();
    auto &rows = read_guard.rows_;
    for (idx_t row_id = 0; row_id < rows.size(); row_id++) {
        auto &row = rows[row_id];
        if (!row.tuple_meta_.is_deleted_) {
            InsertEntry(row.tuple_, row_id);
        }
    }
}

bool StlmapIndex::InsertEntry(const Tuple &tuple, idx_t row_id) {
    if (index_.find(tuple[key_position_]) != index_.end()) {
        return false;
    }
    index_[tuple[key_position_]] = row_id;
    return true;
};

void StlmapIndex::EraseEntry(const Tuple &tuple, idx_t row_id) {
    index_.erase(tuple[key_position_]);
};

StlmapIndex::Iterator StlmapIndex::Lowerbound(data_t key) {
    return index_.lower_bound(key);
}

StlmapIndex::Iterator StlmapIndex::End() {
    return index_.end();
}

}