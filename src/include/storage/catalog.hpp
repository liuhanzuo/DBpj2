#pragma once

#include "babydb.hpp"

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

namespace babydb {

class Table;
class Index;

class Catalog {
public:

    void CreateTable(std::unique_ptr<Table> table);

    void DropTable(const std::string &table_name);

    void CreateIndex(std::unique_ptr<Index> index);

    void DropIndex(const std::string &index_name);

    std::shared_lock<std::shared_mutex> GetLock() {
        return std::shared_lock<std::shared_mutex>(latch_);
    }

    Table* FetchTable(const std::string &table_name);

    Index* FetchIndex(const std::string &index_name);

private:

    std::map<std::string, std::unique_ptr<Table>> tables_;

    std::map<std::string, std::unique_ptr<Index>> indexes_;

    std::shared_mutex latch_;
};

}