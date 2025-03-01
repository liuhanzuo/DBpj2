#include "execution/insert_operator.hpp"

#include "storage/catalog.hpp"
#include "storage/index.hpp"
#include "storage/table.hpp"

namespace babydb {

InsertOperator::InsertOperator(const ExecutionContext &exec_ctx, const std::shared_ptr<Operator> &child_operator,
                               const std::string &table_name)
    : Operator(exec_ctx, {child_operator}, Schema{}), table_name_(table_name) {}

void InsertOperator::SelfCheck() {
    auto &child_schema = child_operators_[0]->GetOutputSchema();
    auto table = exec_ctx_.catalog_.FetchTable(table_name_);
    if (table == nullptr) {
        throw std::logic_error("InsertOperator: Table " + table_name_ + " does not exists");
    }
    if (child_schema != table->schema_) {
        throw std::logic_error("InsertOperator: Different schema for the input and the table");
    }
}

OperatorState InsertOperator::Next(Chunk &) {
    auto table = exec_ctx_.catalog_.FetchTable(table_name_);
    Index *index = nullptr;
    idx_t key_attr = INVALID_ID;
    if (table->GetIndex() != INVALID_NAME) {
        index = exec_ctx_.catalog_.FetchIndex(table->GetIndex());
        key_attr = table->schema_.GetKeyAttr(index->key_name_);
    }

    Chunk insert_chunk;
    auto child_state = OperatorState::HAVE_MORE_OUTPUT;
    while (child_state != EXHAUSETED) {
        child_state = child_operators_[0]->Next(insert_chunk);
        auto write_guard = table->GetWriteTableGuard();
        for (auto &insert_data : insert_chunk) {
            TupleMeta tuple_meta{false};
            write_guard.Rows().push_back({insert_data.first, tuple_meta});

            if (index != nullptr) {
                idx_t row_id = write_guard.Rows().size() - 1;
                index->InsertEntry(insert_data.first.KeyFromTuple(key_attr), row_id);
            }
        }
    }
    return EXHAUSETED;
}

}