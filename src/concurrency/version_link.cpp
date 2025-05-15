#include "concurrency/version_link.hpp"

#include <atomic>
#include <random>
#include <iostream>
#include <mutex>

namespace babydb {

// START: Do not modify this part.

std::atomic<idx_t> current_nodes{0};
std::atomic<idx_t> max_nodes{0};

void RegisterVersionNode() {
    auto update = current_nodes.fetch_add(1, std::memory_order_relaxed) + 1;
    auto expected = max_nodes.load(std::memory_order_relaxed);
    while (update > expected && 
           !max_nodes.compare_exchange_weak(expected, update, std::memory_order_relaxed)) {
        // loop until success
    }
}
void UnregisterVersionNode() {
    current_nodes.fetch_sub(1, std::memory_order_relaxed);
}

// END: Do not modify this part.

void VersionSkipList::insert_list(data_t data_in, idx_t ts, idx_t txn_id) {
    std::random_device rd;
    std::mt19937 generator(rd());
    int i = 0;
    while (i < MAXLEVEL - 1 && generator() & 1) {
        i++;
    }

    Datalist* x = data[MAXLEVEL - 1];
    int level = MAXLEVEL - 1;
    Datalist* newterm = new Datalist(ts, data_in, txn_id);

    if (!x) {
        for (int i = 0; i < MAXLEVEL; i++) {
            data[i] = newterm;
        }
        return;
    }

    while (x->ts > ts) {
        if (level <= i) {
            newterm->ptr[level] = x;
            data[level] = newterm;
            if (level == 0) return;
        }
        level--;
        x = data[level];
    }
    Datalist* y = x;
    while (true) {
        x = x->ptr[level];
        while (!x || (x->ts > ts)) {
            if (level <= i) {
                newterm->ptr[level] = x;
                y->ptr[level] = newterm;
                if (level == 0) return;
            }
            level--;
            x = y->ptr[level];
        }
        y = x;
    }
}

void VersionSkipList::insert_uncommitted_list(data_t data_in, idx_t ts, idx_t txn_id)
{
    std::unique_lock listlock(list_latch_);
    if ((uncommitted && (uncommitted->txn_id != txn_id)) || (lastcommitts > ts)) {
        listlock.unlock(); 
        throw TaintedException("Write conflict");
    }
    Datalist* newterm = new Datalist(ts, data_in, txn_id);
    if (uncommitted) {
        delete uncommitted;
        //UnregisterVersionNode();
    } 
    uncommitted = newterm;
    RegisterVersionNode();
}

void VersionSkipList::commit(idx_t ts)
{
    std::unique_lock listlock(list_latch_);
    if (uncommitted){
        insert_list(uncommitted->data, ts, uncommitted->txn_id);
        delete uncommitted;
        uncommitted = nullptr;
        lastcommitts = ts; // update lastcommitts
    }
}

void VersionSkipList::rollback(idx_t txn_id)
{
    std::unique_lock listlock(list_latch_);
    if (uncommitted && (uncommitted->txn_id == txn_id)) {
        delete uncommitted;
        uncommitted = nullptr;
        //UnregisterVersionNode();
    }
}


data_t VersionSkipList::search_list(idx_t ts, idx_t txn_id)
{
    std::shared_lock listlock(list_latch_);
    int level = MAXLEVEL - 1;
    if (!data[level]) return INVALID_ID; // empty
    if (uncommitted && (uncommitted->txn_id == txn_id)) return uncommitted->data; // should use locally uncommited

    while (data[level]->ts > ts) {level--;}
    Datalist* datanode = data[level];
    while (1) {
        while (!(datanode ->ptr[level]) || (datanode ->ptr[level]->ts > ts)) {
            if (level == 0) return datanode ->data;
            level--;
        }
        datanode  = datanode ->ptr[level];
    }
}

void VersionSkipList::garbage_collect(idx_t gc_ts) {
    return ;
    std::unique_lock lock(list_latch_);
    
    if (!data[MAXLEVEL-1]) {
        return; 
    }

    Datalist* first_to_keep = data[MAXLEVEL-1];
    std::vector<Datalist*> nodes_to_delete;
    if(first_to_keep == nullptr){
        return;
    }
    while (first_to_keep && first_to_keep->ts < gc_ts) {
        nodes_to_delete.push_back(first_to_keep);
        first_to_keep = first_to_keep->ptr[0];
        if (!first_to_keep) {
            break;
        }
    }


    if (!first_to_keep) {
        first_to_keep = nodes_to_delete.back(); 
        nodes_to_delete.pop_back(); 
    }

    for (int level = MAXLEVEL-1; level >= 0; --level) {
        Datalist* current = data[level];
        Datalist* prev = nullptr;
        
        while (current && current->ts < gc_ts) {
            prev = current;
            current = current->ptr[level];
        }

        if (prev) {
            prev->ptr[level] = first_to_keep;
            if (level == 0) {
                data[level] = first_to_keep; 
            }
        } else if (current != data[level]) {
            data[level] = first_to_keep; 
        }
    }

    for (Datalist* node : nodes_to_delete) {
        for (int i = 0; i < MAXLEVEL; ++i) {
            node->ptr[i] = nullptr;
        }
        delete node;
    }
}



void destroy_list(Datalist* head)
{
    if (head) {
        Datalist* headptr = head;
        Datalist* ptr0 = headptr->ptr[0];
        while (ptr0) {
            delete headptr;
            headptr = ptr0;
            ptr0 = ptr0->ptr[0];
        }
        delete headptr;
    }
}

}