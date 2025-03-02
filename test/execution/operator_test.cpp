#include "gtest/gtest.h"

#include "babydb.hpp"
#include "execution/hash_join_operator.hpp"
#include "execution/filter_operator.hpp"
#include "execution/insert_operator.hpp"
#include "execution/seq_scan_operator.hpp"
#include "execution/value_operator.hpp"
#include "storage/catalog.hpp"
#include "storage/index.hpp"
#include "storage/table.hpp"

#include <algorithm>

namespace babydb {

static ConfigGroup TestConfig() {
    ConfigGroup config;
    config.CHUNK_SUGGEST_SIZE = 2;
    return config;
}

static std::vector<Tuple> RunOperator(Operator &test_operator, bool sort_output = true) {
    test_operator.Check();
    test_operator.Init();
    std::vector<Tuple> results;
    Chunk chunk;
    auto operator_state = OperatorState::HAVE_MORE_OUTPUT;
    while (operator_state != EXHAUSETED) {
        operator_state = test_operator.Next(chunk);
        for (auto &row : chunk) {
            results.push_back(row.first);
        }
    }
    if (sort_output) {
        std::sort(results.begin(), results.end());
    }
    return results;
}

TEST(OperatorTest, ValueBasicTest) {
    BabyDB test_db(TestConfig());
    auto txn = test_db.CreateTxn();
    auto exec_ctx = test_db.GetExecutionContext(txn);
    std::vector<Tuple> tuples;
    tuples.push_back({0, 1});
    tuples.push_back({2, 3});
    tuples.push_back({4, 5});
    tuples.push_back({6, 7});
    auto tuples_copy = tuples;
    ValueOperator test_operator(exec_ctx, {"c0", "c1"}, std::move(tuples_copy));

    EXPECT_EQ(RunOperator(test_operator), tuples);
    test_db.Commit(*txn);
}

TEST(OperatorTest, HashJoinBasicTest) {
    BabyDB test_db(TestConfig());
    auto txn = test_db.CreateTxn();
    auto exec_ctx = test_db.GetExecutionContext(txn);

    std::vector<Tuple> build_tuples;
    build_tuples.push_back({0, 1});
    build_tuples.push_back({2, 3});
    build_tuples.push_back({2, 4});
    build_tuples.push_back({4, 5});

    std::vector<Tuple> probe_tuples;
    probe_tuples.push_back({0, 0});
    probe_tuples.push_back({2, 2});
    probe_tuples.push_back({3, 3});

    std::vector<Tuple> answers;
    answers.push_back({0, 0, 0, 1});
    answers.push_back({2, 2, 2, 3});
    answers.push_back({2, 2, 2, 4});

    auto build_operator = std::make_shared<ValueOperator>(exec_ctx, Schema{"b0", "b1"}, std::move(build_tuples));
    auto probe_operator = std::make_shared<ValueOperator>(exec_ctx, Schema{"p0", "p1"}, std::move(probe_tuples));
    auto test_operator = HashJoinOperator(exec_ctx, probe_operator, build_operator, "p0", "b0");

    EXPECT_EQ(RunOperator(test_operator), answers);
    test_db.Commit(*txn);
}

TEST(OperatorTest, InsertAndScanBasicTest) {
    BabyDB test_db(TestConfig());
    Schema schema{"c0", "c1"};
    test_db.CreateTable("table0", schema);
    test_db.CreateIndex("index0_0", "table0", "c0", IndexType::Stlmap);
    auto txn = test_db.CreateTxn();
    auto exec_ctx = test_db.GetExecutionContext(txn);

    std::vector<Tuple> insert_tuples;
    insert_tuples.push_back({0, 1});
    insert_tuples.push_back({2, 3});
    insert_tuples.push_back({4, 5});

    std::vector<Tuple> insert_tuples_copy = insert_tuples;

    auto value_operator = std::make_shared<ValueOperator>(exec_ctx, Schema{"c0", "c1"}, std::move(insert_tuples_copy));
    auto insert_operator = InsertOperator(exec_ctx, value_operator, "table0");

    EXPECT_EQ(RunOperator(insert_operator), std::vector<Tuple>{});

    std::vector<Tuple> results_table;
    auto table = exec_ctx.catalog_.FetchTable("table0");
    auto read_guard = table->GetReadTableGuard();
    for (auto &row : read_guard.Rows()) {
        results_table.push_back(row.tuple_);
    }
    std::sort(results_table.begin(), results_table.end());
    EXPECT_EQ(results_table, insert_tuples);

    std::vector<Tuple> results_index;
    auto index = exec_ctx.catalog_.FetchIndex("index0_0");
    for (data_t key = -1; key <= 5; key++) {
        auto row_id = index->ScanKey(key);
        if (row_id != INVALID_ID) {
            results_index.push_back(read_guard.Rows()[row_id].tuple_);
            EXPECT_EQ(results_index.back()[0], key);
        }
    }
    std::sort(results_index.begin(), results_index.end());
    EXPECT_EQ(results_index, insert_tuples);

    read_guard.Drop();

    EXPECT_ANY_THROW(RunOperator(insert_operator));
    auto write_guard = table->GetWriteTableGuard();
    for (auto &row : write_guard.Rows()) {
        row.tuple_meta_.is_deleted_ = true;
    }
    write_guard.Drop();
    EXPECT_NO_THROW(RunOperator(insert_operator));

    auto seq_scan_operator = SeqScanOperator(exec_ctx, "table0", {"c0", "c1"});
    EXPECT_EQ(RunOperator(seq_scan_operator), insert_tuples);

    test_db.Commit(*txn);
}

TEST(OperatorTest, FilterTest) {
    BabyDB test_db(TestConfig());
    auto txn = test_db.CreateTxn();
    auto exec_ctx = test_db.GetExecutionContext(txn);

    std::vector<Tuple> tuples;
    tuples.push_back({0, 1});
    tuples.push_back({2, 3});
    tuples.push_back({4, 4});

    auto value_operator = std::make_shared<ValueOperator>(exec_ctx, Schema{"c0", "c1"}, std::move(tuples));

    auto equal_filter = std::make_unique<EqualFilter>("c0", 0);
    auto equal_filter_operator = FilterOperator(exec_ctx, value_operator, std::move(equal_filter));
    EXPECT_EQ(RunOperator(equal_filter_operator), (std::vector<Tuple>{Tuple{0, 1}}));

    auto range_filter = std::make_unique<RangeFilter>("c0", RangeInfo{1, 4});
    auto range_filter_operator = FilterOperator(exec_ctx, value_operator, std::move(range_filter));
    EXPECT_EQ(RunOperator(range_filter_operator), (std::vector<Tuple>{Tuple{2, 3}, Tuple{4, 4}}));

    auto udfilter = std::make_unique<UDFilter>(Schema{"c0", "c1"}, [](const Tuple &keys) {
        return keys[0] == keys[1];
    });
    auto udfilter_operator = FilterOperator(exec_ctx, value_operator, std::move(udfilter));
    EXPECT_EQ(RunOperator(udfilter_operator), (std::vector<Tuple>{Tuple{4, 4}}));
}

}