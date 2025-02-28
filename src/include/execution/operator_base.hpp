#pragma once

#include "common/macro.hpp"
#include "common/typedefs.hpp"

#include "memory"

namespace babydb {

typedef std::vector<std::pair<Tuple, idx_t>> Chunk;

enum OperatorState {
    HAVE_MORE_OUTPUT,
    EXHAUSETED
};

class Transaction;
class Catalog;
struct ExecutionContext {
    Transaction &txn_;
    const Catalog &catalog_;
};

class Operator {
public:
    virtual ~Operator() = default;

    DISALLOW_COPY(Operator);

    Operator(std::vector<std::shared_ptr<Operator>> &&child_operators, const Schema &output_schema,
             const ExecutionContext &execute_context)
        : child_operators_(std::move(child_operators)), output_schema_(output_schema),
          execute_context_(execute_context) {}

    Operator(std::vector<std::shared_ptr<Operator>> &&child_operators, const ExecutionContext &execute_context)
        : child_operators_(std::move(child_operators)), output_schema_{}, execute_context_(execute_context) {
        for (auto &child_operator : child_operators_) {
            auto child_schema = child_operator->GetOutputSchema();
            output_schema_.insert(output_schema_.end(), child_schema.begin(), child_schema.end());
        }
    }

    virtual OperatorState Next(Chunk &output_chunk) = 0;

    virtual void SelfInit() {}

    virtual void Init() {
        for (auto &child_operator : child_operators_) {
            child_operator->Init();
        }
        SelfInit();
    }

    const Schema& GetOutputSchema() {
        return output_schema_;
    }

protected:
    std::vector<std::shared_ptr<Operator>> child_operators_;

    Schema output_schema_;

    ExecutionContext execute_context_;
};

}