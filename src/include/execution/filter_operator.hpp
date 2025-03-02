#pragma once

#include "execution/expression/filter.hpp"
#include "execution/operator.hpp"

namespace babydb {

/**
 * Filter Operator
 * The output schema is the same as the input.
 */
class FilterOperator : public Operator {
public:
    FilterOperator(const ExecutionContext &exec_ctx,
                   const std::shared_ptr<Operator> &probe_child_operator,
                   std::unique_ptr<Filter> &&filter)
        : Operator(exec_ctx, {probe_child_operator}, probe_child_operator->GetOutputSchema()),
          filter_(std::move(filter)) {}

    ~FilterOperator() = default;

    OperatorState Next(Chunk &output_chunk) override;

private:
    void SelfInit() override;

    void SelfCheck() override;

private:
    std::unique_ptr<Filter> filter_;
};

}