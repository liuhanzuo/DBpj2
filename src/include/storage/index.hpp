#pragma once

#include "babydb.hpp"

#include "common/macro.hpp"
#include "storage/table.hpp"

#include <string>

namespace babydb {

//! We only support index with one column as the key.
//! Index may be not thread-safe, so you should use indexes with the table guard.
class Index {
public:
    const std::string name_;

    const std::string table_name_;

    const idx_t key_position_;

public:
    Index(const std::string &name, Table &table, idx_t key_position)
        : name_(name), table_name_(table.name_), key_position_(key_position) {}

    virtual ~Index() = default;

    DISALLOW_COPY_AND_MOVE(Index);

    virtual bool InsertEntry(const Tuple &tuple, idx_t row_id) = 0;

    virtual void EraseEntry(const Tuple &tuple, idx_t row_id) = 0;

private:

friend class Catalog;
};

}