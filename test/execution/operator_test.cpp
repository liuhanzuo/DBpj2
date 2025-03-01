#include "gtest/gtest.h"

#include "babydb.hpp"
#include "execution/hash_join_operator.hpp"
#include "execution/value_operator.hpp"

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
    while (test_operator.Next(chunk) != EXHAUSETED) {
        for (auto &row : chunk) {
            results.push_back(row.first);
        }
    }
    std::sort(results.begin(), results.end());
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

}