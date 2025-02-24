#include "concurrency/transaction.hpp"

namespace babydb {

Transaction::~Transaction() {
    if (state_ != COMMITED && state_ != ABORTED) {
        abort();
    }
}

void Transaction::Done() {
    db_lock_.release();
}

}