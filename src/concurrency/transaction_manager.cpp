#include "concurrency/transaction_manager.hpp"
#include "concurrency/version_link.hpp"
#include <iostream>

namespace babydb {

std::shared_ptr<Transaction> TransactionManager::CreateTxn(std::shared_lock<std::shared_mutex> &&db_lock) {
    std::unique_lock lock(txn_map_latch_);
    // Update gcts to be the minimum read_ts_ among all active transactions
    idx_t min_read_ts = last_commit_ts_; // Initialize with last commit timestamp
    for (const auto& [txn_id, txn] : txn_map_) {
        if (txn->state_ == RUNNING && txn->read_ts_ < min_read_ts) {
            min_read_ts = txn->read_ts_;
        }
    }
    auto result = std::make_shared<Transaction>(next_txn_id_, last_commit_ts_, min_read_ts, std::move(db_lock));
    txn_map_.insert(std::make_pair(next_txn_id_, result));
    next_txn_id_++;
    return result;
}

bool TransactionManager::VerifyTxn([[maybe_unused]] Transaction &txn)
{
    if (isolation_level_ == IsolationLevel::SNAPSHOT)
    {
        return true;
    }

    for (auto i = txn.read_rows_.begin(); i != txn.read_rows_.end(); i++)
    {
        if ((*i)->lastcommitts > txn.read_ts_)
        {
            return false;
        }
    }

    return true;
}
bool TransactionManager::Commit(Transaction &txn) {
    if (txn.state_ != RUNNING) {
        throw std::logic_error("Try to commit a not running transaction."); 
    }
    std::unique_lock commit_lock(commit_latch_);
    if (!VerifyTxn(txn)) {
        commit_lock.unlock();
        Abort(txn);
        return false;
    }
    // Project 2: Commit the txn
    txn.commit_ts_ = last_commit_ts_ + 1;
    for (auto rid = txn.modified_rows_.begin(); rid != txn.modified_rows_.end(); rid++)
    {
        (*rid)->commit(txn.commit_ts_);
    }
    // gc 
    for (auto rid = txn.read_rows_.begin(); rid != txn.read_rows_.end(); rid++) {
        (*rid)->garbage_collect(txn.gc_ts_);
    }
    
    std::unique_lock map_lock(txn_map_latch_);
    last_commit_ts_++;
    txn.state_ = COMMITED;
    txn.Done();
    txn_map_.erase(txn.txn_id_);
    return true;
}
//! The txn should roll back. We do not implement it.
void TransactionManager::Abort(Transaction &txn) {
    if (txn.state_ != RUNNING && txn.state_ != TAINTED) {
        throw std::logic_error("Try to abort a not running or tainted transaction."); 
    }
    // Project 2: Rollback the txn
    std::unique_lock abort_lock(commit_latch_);

    for (auto rid = txn.modified_rows_.begin(); rid != txn.modified_rows_.end(); rid++)
    {
        (*rid)->rollback(txn.txn_id_);
    }

    std::unique_lock map_lock(txn_map_latch_);
    txn.state_ = ABORTED;
    txn.Done();
    txn_map_.erase(txn.txn_id_);

    //throw std::logic_error("Txn Abort is not implemented.");
}

}