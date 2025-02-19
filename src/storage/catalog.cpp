#include "storage/catalog.hpp"

#include "storage/index.hpp"
#include "storage/table.hpp"

#include <mutex>

namespace babydb {

void Catalog::CreateTable(std::unique_ptr<Table> table) {
    std::unique_lock guard(latch_);
    if (tables_.find(table->name_) != tables_.end()) {
        throw Exception("CREATE TABLE: table already exists");
    }
    tables_.insert(std::make_pair(table->name_, std::move(table)));
}

void Catalog::DropTable(const std::string &table_name) {
    std::unique_lock guard(latch_);
    auto position = tables_.find(table_name);
    if (position== tables_.end()) {
        throw Exception("DROP TABLE: table does not exist");
    }
    for (auto index_name : position->second->indexes_) {
        indexes_.erase(index_name);
    }
    tables_.erase(position);
}

void Catalog::CreateIndex(std::unique_ptr<Index> index) {
    std::unique_lock guard(latch_);
    if (indexes_.find(index->name_) != indexes_.end()) {
        throw Exception("CREATE INDEX: index already exists");
    }
    auto table_position = tables_.find(index->table_name_);
    if (table_position == tables_.end()) {
        throw Exception("CREATE INDEX: table does not exist");
    }
    indexes_.insert(std::make_pair(index->name_, std::move(index)));
    table_position->second->indexes_.insert(index->name_);
}

void Catalog::DropIndex(const std::string &index_name) {
    std::unique_lock guard(latch_);
    auto position = indexes_.find(index_name);
    if (position == indexes_.end()) {
        throw Exception("DROP INDEX: index does not exist");
    }
    auto table_position = tables_.find(position->second->table_name_);
    table_position->second->indexes_.erase(index_name);
    indexes_.erase(position);
}

Table* Catalog::FetchTable(const std::string &table_name) {
    auto position = tables_.find(table_name);
    if (position == tables_.end()) {
        throw Exception("error: table does not exist");
    }
    return position->second.get();
}

Index* Catalog::FetchIndex(const std::string &index_name) {
    auto position = indexes_.find(index_name);
    if (position == indexes_.end()) {
        throw Exception("error: index does not exist");
    }
    return position->second.get();
}

}