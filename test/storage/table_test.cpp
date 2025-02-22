#include "gtest/gtest.h"

#include "storage/table.hpp"

#include <thread>

namespace babydb {

TEST(TableTest, BasicTest) {
    Schema schema{"c0", "c1"};
    Table table(schema, "table");

    auto write_guard = table.GetWriteTableGuard();
    write_guard.rows_.emplace_back(Tuple{0, 1}, TupleMeta{false});
    write_guard.rows_.emplace_back(Tuple{2, 3}, TupleMeta{false});
    write_guard.Drop();

    auto read_guard_0 = table.GetReadTableGuard();
    auto read_guard_1 = table.GetReadTableGuard();

    EXPECT_EQ(read_guard_0.rows_[0].tuple_, (Tuple{0, 1}));
    EXPECT_EQ(read_guard_0.rows_[0].tuple_meta_.is_deleted_, false);
}

TEST(TableTest, ConcurrentTest) {
    Schema schema{"c0", "c1"};
    Table table(schema, "table");

    const idx_t thread_count = 4;
    const idx_t operation_count = 5000;

    std::vector<std::thread> thread_list;

    for (idx_t thread_id = 0; thread_id < thread_count; thread_id++) {
        thread_list.emplace_back([](idx_t start_id, idx_t end_id, Table &table) {
            for (idx_t element_id = start_id; element_id < end_id; element_id++) {
                auto write_guard = table.GetWriteTableGuard();
                auto before_length = write_guard.rows_.size();
                write_guard.rows_.emplace_back(Tuple{element_id, element_id * 2}, TupleMeta{false});
                write_guard.Drop();

                auto read_guard = table.GetReadTableGuard();
                auto cur_length = read_guard.rows_.size();
                EXPECT_GT(cur_length, before_length);
                EXPECT_EQ(read_guard.rows_[before_length].tuple_[0], element_id);
            }
        }, thread_id * operation_count, (thread_id + 1) * operation_count, table);
    }

    auto read_guard = table.GetReadTableGuard();
    EXPECT_EQ(read_guard.rows_.size(), operation_count * thread_count);
    int64_t expected_sum2 = 0;
    int64_t sum2 = 0;
    for (idx_t i = 0; i < read_guard.rows_.size(); i++) {
        expected_sum2 += i * i;
        sum2 += read_guard.rows_[i].tuple_[0] * read_guard.rows_[i].tuple_[0];
    }
    EXPECT_EQ(expected_sum2, sum2);
}

}