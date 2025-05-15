#pragma once

#include "common/typedefs.hpp"
#include <shared_mutex>

namespace babydb {

void RegisterVersionNode();

void UnregisterVersionNode();

const int MAXLEVEL = 18;

struct Datalist {
    data_t data;
    idx_t ts;
    Datalist* ptr[MAXLEVEL];
    idx_t txn_id;

    Datalist(idx_t ts, data_t data, idx_t txn_id) : data(data), ts(ts), txn_id(txn_id) {for (int i = 0; i < MAXLEVEL; i++) ptr[i] = nullptr;}
    ~Datalist() {UnregisterVersionNode();}
};

void destroy_list(Datalist* head);

class VersionSkipList {
public:
    data_t key;
    Datalist* data[MAXLEVEL];
    Datalist* uncommitted;

    idx_t lastcommitts;
    VersionSkipList(data_t key, Datalist* uncommitted) : key(key), uncommitted(uncommitted) {for (int i = 0; i < MAXLEVEL; i++) {data[i] = nullptr;}}

    void insert_list(data_t data_in, idx_t ts, idx_t txn_id);
    void insert_uncommitted_list(data_t data_in, idx_t ts, idx_t txn_id);
    void commit(idx_t ts);
    void rollback(idx_t txn_id);
    void garbage_collect(idx_t ts);
    data_t search_list(idx_t ts, idx_t txn_id);

    ~VersionSkipList() {destroy_list(data[0]);if (uncommitted) {delete uncommitted;UnregisterVersionNode();}}

private:
    std::shared_mutex list_latch_;

};



}