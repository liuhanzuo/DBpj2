#include "storage/stlmap_index.hpp"

namespace babydb {

StlmapIndex::StlmapIndex(const std::string &name, Table &table, idx_t key_attr)
    : RangeIndex(name, table, key_attr) {
    auto read_guard = table.GetReadTableGuard();
    auto &rows = read_guard.Rows();
    for (idx_t row_id = 0; row_id < rows.size(); row_id++) {
        auto &row = rows[row_id];
        if (!row.tuple_meta_.is_deleted_) {
            InsertEntry(row.tuple_.KeyFromTuple(key_attr_), row_id);
        }
    }
}

void StlmapIndex::InsertEntry(const data_t &key, idx_t row_id) {
    if (index_.find(key) != index_.end()) {
        throw std::logic_error("duplicated key");
    }
    index_[key] = row_id;
};

void StlmapIndex::EraseEntry(const data_t &key, [[maybe_unused]] idx_t row_id) {
    index_.erase(key);
};

idx_t StlmapIndex::ScanKey(const data_t &key) {
    auto ite = index_.find(key);
    if (ite == index_.end()) {
        return INVALID_ID;
    }
    return ite->second;
}

void StlmapIndex::ScanRange(const RangeInfo &range, std::vector<idx_t> &row_ids) {
    row_ids.clear();
    std::map<data_t, idx_t>::iterator start_ite;
    std::map<data_t, idx_t>::iterator end_ite;
    if (range.contain_start) {
        start_ite = index_.lower_bound(range.start);
    } else {
        start_ite = index_.upper_bound(range.start);
    }
    if (range.contain_end) {
        end_ite = index_.upper_bound(range.end);
    } else {
        end_ite = index_.lower_bound(range.end);
    }
    for (auto ite = start_ite; ite != end_ite; ite++) {
        row_ids.push_back(ite->second);
    }
}

}