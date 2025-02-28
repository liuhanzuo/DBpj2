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
    
    Operator(std::vector<std::unique_ptr<Operator>> &&child_operators, const Schema &output_schema,
             const ExecutionContext &execute_context)
        : child_operators_(std::move(child_operators)), output_schema_(output_schema),
          execute_context_(execute_context) {}

    virtual OperatorState Next(Chunk &output_chunk) = 0;

    virtual void Init() = 0;

    const Schema& GetOutputSchema() {
        return output_schema_;
    }

private:
    std::vector<std::unique_ptr<Operator>> child_operators_;

    Schema output_schema_;

    ExecutionContext execute_context_;
};

}