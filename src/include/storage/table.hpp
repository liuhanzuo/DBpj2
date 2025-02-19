#pragma once

#include "babydb.hpp"

#include <mutex>
#include <set>
#include <string>
#include <shared_mutex>
#include <vector>

namespace babydb {

typedef std::vector<data_t> Tuple;

struct TupleMeta {
    bool is_deleted_;
};

struct Row {
    Tuple tuple_;
    TupleMeta tuple_meta_;
};

//! Since babydb only has one type, the schema of the table is just the column names.
typedef std::vector<std::string> Schema;

class ReadTableGuard;
class WriteTableGuard;

class Table {
public:
    Table(Schema schema, const std::string &name)
        : schema_(schema), name_(name) {}

    ReadTableGuard GetReadTableGuard();

    WriteTableGuard GetWriteTableGuard();

    const std::string& GetName() const {
        return name_;
    }

    const std::set<std::string> GetIndexes() const {
        return indexes_;
    }

private:

    Schema schema_;

    std::vector<Row> rows_;

    std::string name_;

    std::set<std::string> indexes_; 

    std::shared_mutex latch_;

friend class ReadTableGuard;
friend class WriteTableGuard;
friend class Catalog;
};

class ReadTableGuard {
public:
    ~ReadTableGuard();

    void Drop();

    Row FetchRow(idx_t row_id) const {
        return table_.rows_[row_id];
    }

    idx_t Count() const {
        return static_cast<idx_t>(table_.rows_.size());
    }

private:
    ReadTableGuard(Table &table);

private:
    Table &table_;

    bool drop_tag_;

friend class Table;
};

class WriteTableGuard {
public:
    ~WriteTableGuard();

    void Drop();

    idx_t InsertRow(const Row &row) const {
        table_.rows_.push_back(row);
        return table_.rows_.size() - 1;
    }

    Row& FetchRow(idx_t row_id) const {
        return table_.rows_[row_id];
    }

    idx_t Count() const {
        return static_cast<idx_t>(table_.rows_.size());
    }

private:
    WriteTableGuard(Table &table);

private:
    Table &table_;

    bool drop_tag_;

friend class Table;
};

}