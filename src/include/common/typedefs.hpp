#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <vector>

namespace babydb {

//! babydb's only data type
typedef int64_t data_t;
//! babydb's index type
typedef uint64_t idx_t;

const std::string INVALID_NAME = "";
const idx_t INVALID_ID = static_cast<idx_t>(-1);

class Tuple : public std::vector<data_t> {
public:
    using std::vector<data_t>::vector;

    std::vector<data_t> KeysFromTuple(const std::vector<idx_t> &key_attrs) const {
        std::vector<data_t> result;
        for (auto position : key_attrs) {
            result.push_back(this->operator[](position));
        }
        return result;
    }

    data_t KeyFromTuple(idx_t key_attr) const {
        return this->operator[](key_attr);
    }
};

//! Since babydb only has one type, the schema of the table is just the column names.
class Schema : public std::vector<std::string> {
public:
    using std::vector<std::string>::vector;

    std::vector<idx_t> GetKeyAttrs(const Schema &key_schema) const {
        std::vector<idx_t> result;
        for (auto cname : key_schema) {
            auto position = this->find(cname);
            if (position == INVALID_ID) {
                throw std::logic_error("Invalid key schema");
            }
            result.push_back(position);
        }
        return result;
    }

private:
    idx_t find(const std::string &cname) const {
        for (idx_t i = 0; i < cname.size(); i++) {
            if (this->operator[](i) == cname) {
                return i;
            }
        }
        return INVALID_ID;
    }
};

}