#include "execution/execution_common.hpp"

#include "storage/index.hpp"
#include "storage/table.hpp"

#include "execution/execution_context.hpp"

namespace babydb {

void InsertRow(WriteTableGuard &write_guard, Tuple &&tuple, Index *index, const data_t &key, ExecutionContext &exec_ctx) {
    // Project 2: Implement it
    idx_t rid = write_guard.Rows().size(); // now rowid
    write_guard.Rows().push_back(Row{std::move(tuple), TupleMeta{}});
    try {
        index->InsertEntry(key, rid, exec_ctx);
    }
    catch (TaintedException &e){
        throw e;
    }
}
}