#include "execution/update_operator.hpp"

#include "execution/execution_common.hpp"
#include "storage/catalog.hpp"
#include "storage/index.hpp"
#include "storage/table.hpp"

namespace babydb {

UpdateOperator::UpdateOperator(const ExecutionContext &exec_ctx, const std::shared_ptr<Operator> &child_operator,
                               const std::string &table_name)
    : Operator(exec_ctx, {child_operator}, Schema{}), table_name_(table_name), input_schema_(std::nullopt) {}

UpdateOperator::UpdateOperator(const ExecutionContext &exec_ctx, const std::shared_ptr<Operator> &child_operator,
                               const std::string &table_name, const Schema &input_schema)
    : Operator(exec_ctx, {child_operator}, Schema{}), table_name_(table_name), input_schema_(input_schema) {}

void UpdateOperator::SelfCheck() {
    auto &child_schema = child_operators_[0]->GetOutputSchema();
    auto table = exec_ctx_.catalog_.FetchTable(table_name_);
    if (table == nullptr) {
        throw std::logic_error("UpdateOperator: Table " + table_name_ + " does not exists");
    }

    if (input_schema_.has_value()) {
        if (input_schema_->size() != table->schema_.size()) {
            throw std::logic_error("UpdateOperator: The schema of the table and the input do not match");
        }
        child_schema.GetKeyAttrs(*input_schema_);
    } else {
        if (child_schema.size() != table->schema_.size()) {
            throw std::logic_error("UpdateOperator: The schema of the table and the input do not match");
        }
    }
}

OperatorState UpdateOperator::Next(Chunk &) {
    auto table = exec_ctx_.catalog_.FetchTable(table_name_);
    std::vector<idx_t> key_attrs;
    if (input_schema_.has_value()) {
        key_attrs = child_operators_[0]->GetOutputSchema().GetKeyAttrs(*input_schema_);
    }

    Index *index = nullptr;
    idx_t index_key_attr = INVALID_ID;
    if (table->GetIndex() != INVALID_NAME) {
        index = exec_ctx_.catalog_.FetchIndex(table->GetIndex());
        index_key_attr = table->schema_.GetKeyAttr(index->key_name_);
    }

    Chunk update_chunk;
    auto child_state = OperatorState::HAVE_MORE_OUTPUT;
    while (child_state != EXHAUSETED) {
        child_state = child_operators_[0]->Next(update_chunk);
        auto write_guard = table->GetWriteTableGuard();
        for (auto &[update_tuple, row_id] : update_chunk) {
            if (input_schema_.has_value()) {
                update_tuple = update_tuple.KeysFromTuple(key_attrs);
            }

            auto &[target_tuple, target_meta] = write_guard.Rows()[row_id];
            if (target_meta.is_deleted_) {
                throw std::logic_error("Why the tuple has been deleted?");
            }

            bool update_inplace = true;
            if (index != nullptr) {
                auto new_key = update_tuple.KeyFromTuple(index_key_attr);
                auto old_key = target_tuple.KeyFromTuple(index_key_attr);
                if (new_key != old_key) {
                    update_inplace = false;
                    // Delete and Insert
                    DeleteRow(write_guard, row_id);
                    InsertRowWithIndex(write_guard, std::move(update_tuple), index, new_key);
                }
            }

            if (update_inplace) {
                target_tuple = std::move(update_tuple);
            }
        }
    }
    return EXHAUSETED;
}

}