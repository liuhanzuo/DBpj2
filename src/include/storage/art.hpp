#pragma once

#include "common/typedefs.hpp"
#include "storage/index.hpp"

#include <string>
#include <vector>

namespace babydb {

class ArtIndex : public RangeIndex {
public:
    explicit ArtIndex(const std::string &name, Table &table, const std::string &key_name);
    ~ArtIndex() override;

    void InsertEntry(const data_t &key, idx_t row_id, idx_t start_ts = 0) override;
    void EraseEntry(const data_t &key, idx_t row_id, idx_t start_ts = 0, idx_t end_ts = 0) override;
    idx_t ScanKey(const data_t &key, idx_t start_ts = 0, idx_t end_ts = 0) override;
    void ScanRange(const RangeInfo &range, std::vector<idx_t> &row_ids, idx_t start_ts = 0, idx_t end_ts = 0) override;

private:
    // ART树的根节点，内部实现细节在cpp中隐藏，使用 void* 保存不暴露结构
    void* root_;
};

} // namespace babydb
