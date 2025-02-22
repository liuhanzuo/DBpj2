#pragma once

#include "babydb.hpp"

#include "common/macro.hpp"

#include <mutex>
#include <set>
#include <string>
#include <shared_mutex>
#include <vector>

namespace babydb {

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

/** 
 * When access the table's rows, you should hold the latch of the table.
 * We design the table guard. When you hold the table guard, you can safely use the rows.
 * Since it's only a latch, you need to make sure,
 * 1. At any time, you can hold at most 1 table guard.
 * 2. When you get the table guard, drop it after finite instructions
 *    (so you should not require another latch during holding the guard).
 * 3. When you hold the table guard, you should keep using the table,
 *    otherwise you should drop it and require it later.
 */
class Table {
public:
    const std::string name_;

    const Schema schema_;

public:
    Table(const Schema &schema, const std::string &name)
        : schema_(schema), name_(name) {}

    DISALLOW_COPY_AND_MOVE(Table);
    //! Get the read permission to the table.
    ReadTableGuard GetReadTableGuard();
    //! Get the read and write permission to the table.
    WriteTableGuard GetWriteTableGuard();

    const std::string GetIndex() const {
        return index_name_;
    }

private:

    std::vector<Row> rows_;
    //! Empty string means no index. To simplify, a table can have at most 1 index.
    std::string index_name_; 

    std::shared_mutex latch_;

friend class ReadTableGuard;
friend class WriteTableGuard;
friend class Catalog;
};

class ReadTableGuard {
public:
    ~ReadTableGuard();

    DISALLOW_COPY(ReadTableGuard);

    void Drop();

    const std::vector<Row> &rows_;

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

    DISALLOW_COPY(WriteTableGuard);

    void Drop();

    std::vector<Row> &rows_;

private:
    WriteTableGuard(Table &table);

private:
    Table &table_;

    bool drop_tag_;

friend class Table;
};

}