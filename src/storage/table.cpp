#include "storage/table.hpp"

namespace babydb {

ReadTableGuard::ReadTableGuard(Table &table) : table_(table), drop_tag_(false), rows_(table.rows_) {
    table_.latch_.lock_shared();
}

ReadTableGuard::~ReadTableGuard() {
    Drop();
}

void ReadTableGuard::Drop() {
    if (!drop_tag_) {
        table_.latch_.unlock_shared();
        drop_tag_ = true;
    }
}

WriteTableGuard::WriteTableGuard(Table &table) : table_(table), drop_tag_(false), rows_(table.rows_) {
    table_.latch_.lock();
}

WriteTableGuard::~WriteTableGuard() {
    Drop();
}

void WriteTableGuard::Drop() {
    if (!drop_tag_) {
        table_.latch_.unlock();
        drop_tag_ = true;
    }
}

ReadTableGuard Table::GetReadTableGuard() {
    return ReadTableGuard(*this);
}

WriteTableGuard Table::GetWriteTableGuard() {
    return WriteTableGuard(*this);
}

}