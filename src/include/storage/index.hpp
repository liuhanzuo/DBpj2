#pragma once

#include "common/typedefs.hpp"
#include "common/macro.hpp"
#include "storage/table.hpp"

#include <string>

namespace babydb {

//! We only support index with the primary key.
//! Index may be not thread-safe, so you should use indexes with the table guard.
class Index {
public:
    const std::string name_;

    const std::string table_name_;

    const idx_t key_attr_;

public:
    Index(const std::string &name, Table &table, idx_t key_attr)
        : name_(name), table_name_(table.name_), key_attr_(key_attr) {}

    virtual ~Index() = default;

    DISALLOW_COPY_AND_MOVE(Index);

    virtual void InsertEntry(const data_t &key, idx_t row_id) = 0;

    virtual void EraseEntry(const data_t &key, idx_t row_id) = 0;

    virtual idx_t ScanKey(const data_t &key) = 0;

friend class Catalog;
};

struct RangeInfo {
    const data_t &start;
    const data_t &end;
    bool contain_start = true;
    bool contain_end = true;
};

class RangeIndex : public Index {
public:
    using Index::Index;

    virtual void ScanRange(const RangeInfo &range, std::vector<idx_t> &row_ids) = 0;
};

}