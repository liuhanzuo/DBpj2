#pragma once

#include "babydb.hpp"

#include <string>

namespace babydb {

//! TODO

class Index {
public:
    Index(std::string name, std::string table_name)
        : name_(name), table_name_(table_name) {}

    virtual ~Index() = 0;

    const std::string& GetName() const {
        return name_;
    }

    const std::string& GetTableName() const {
        return name_;
    }

private:

    std::string name_;

    std::string table_name_;

friend class Catalog;
};

}