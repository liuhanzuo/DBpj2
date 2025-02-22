#pragma once

#include "babydb.hpp"

#include "storage/index.hpp"

#include <map>

namespace babydb {

class StlmapIndex : public Index {
public:
    StlmapIndex(const std::string &name, Table &table, idx_t key_position);

    ~StlmapIndex() override {};

    bool InsertEntry(const Tuple &tuple, idx_t row_id) override;

    void EraseEntry(const Tuple &tuple, idx_t row_id) override;

    typedef std::map<data_t, idx_t>::iterator Iterator;

    Iterator Lowerbound(data_t key);

    Iterator End();

private:

    std::map<data_t, idx_t> index_;
};

}