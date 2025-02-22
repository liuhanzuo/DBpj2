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

typedef std::vector<data_t> Tuple;

class Exception : public std::exception {
public:
    Exception(std::string reason) : reason_(reason) {}

    const char* what() {
        return reason_.c_str();
    }

private:
    std::string reason_;
};