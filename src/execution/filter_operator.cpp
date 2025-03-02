#include "execution/filter_operator.hpp"

namespace babydb {

OperatorState FilterOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();
    Chunk input_chunk;
    auto result = child_operators_[0]->Next(input_chunk);
    for (auto &data : input_chunk) {
        if (filter_->Check(data.first)) {
            output_chunk.push_back(std::move(data));
        }
    }
    return result;
}

void FilterOperator::SelfCheck() {
    if (filter_ == nullptr) {
        throw std::logic_error("FilterOperator: No Filter");
    }
    filter_->Init(output_schema_);
}

void FilterOperator::SelfInit() {
    filter_->Init(output_schema_);
}

}