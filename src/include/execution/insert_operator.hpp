#pragma once

#include "execution/operator_base.hpp"

#include <string>

namespace babydb {

class InsertOperator : public Operator {
public:
    InsertOperator(const ExecutionContext &execute_context, const std::shared_ptr<Operator> &child_operator,
                   const std::string &table_name);

    ~InsertOperator() override = default;

    OperatorState Next(Chunk &) override;

    void SelfInit() override {}

    void SelfCheck() override;

private:
    std::string table_name_;
};

}