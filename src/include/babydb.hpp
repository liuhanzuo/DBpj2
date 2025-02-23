#pragma once

#include <exception>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <string>
#include <vector>

//! babydb's only data type
typedef int64_t data_t;
//! babydb's index type
typedef int64_t idx_t;

const std::string INVALID_NAME = "";
const idx_t INVALID_ID = -1;

class Tuple : public std::vector<data_t> {
public:
    using std::vector<data_t>::vector;

    data_t KeyFromTuple(idx_t key_position) const {
        return (*this)[key_position];
    }
};

class Exception : public std::exception {
public:
    Exception(std::string reason) : reason_(reason) {}

    const char* what() {
        return reason_.c_str();
    }

private:
    std::string reason_;
};