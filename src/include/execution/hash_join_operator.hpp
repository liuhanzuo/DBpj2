#pragma once

#include "execution/operator_base.hpp"

#include <string>
#include <unordered_map>

namespace babydb {

class HashJoinOperator : public Operator {
public:
    HashJoinOperator(const ExecutionContext &execute_context,
                     const std::shared_ptr<Operator> &probe_child_operator,
                     const std::shared_ptr<Operator> &build_child_operator,
                     const std::string &probe_column_name,
                     const std::string &build_column_name);

    ~HashJoinOperator() override = default;
    
    OperatorState Next(Chunk &output_chunk) override;

    void SelfInit() override;

    void SelfCheck() override;

private:
    void BuildHashTable();

private:
    std::string probe_column_name_;

    std::string build_column_name_;

    std::unordered_multimap<data_t, Tuple> hash_table_;

    Chunk buffer_;

    idx_t buffer_ptr_;

    bool probe_child_exhausted_;
};

}