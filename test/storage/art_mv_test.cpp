#include "gtest/gtest.h"
#include "storage/art.hpp"
#include "storage/table.hpp"      
#include "storage/index.hpp"     
#include "common/typedefs.hpp"    
#include <algorithm>
#include <random>
#include <vector>
#include <unordered_map>
#include <functional>
#include <limits>

using babydb::Table;
using babydb::Schema;
using babydb::Tuple;
using babydb::TupleMeta;
using babydb::data_t;
using babydb::idx_t;
const idx_t INVALID_ID = std::numeric_limits<idx_t>::max();


void BuildSortedTable(Table &table, idx_t count) {
    auto write_guard = table.GetWriteTableGuard();
    for (idx_t i = 0; i < count; i++) {
        write_guard.Rows().push_back({Tuple{static_cast<data_t>(i), static_cast<data_t>(i)}, TupleMeta()});
    }
    write_guard.Drop();
}

void BuildRandomTable(Table &table, idx_t count, int seed = 42) {
    std::vector<data_t> keys;
    keys.reserve(count);
    for (idx_t i = 1; i <= count; i++) {
        keys.push_back(i);
    }
    std::mt19937 rng(seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    auto write_guard = table.GetWriteTableGuard();
    for (idx_t i = 0; i < count; i++) {
        write_guard.Rows().push_back({Tuple{static_cast<data_t>(keys[i]), static_cast<data_t>(i)}, TupleMeta()});
    }
    write_guard.Drop();
}

void BuildSparseTable(Table &table, idx_t count, int gap = 1000, int seed = 42) {
    std::vector<data_t> keys;
    keys.reserve(count);
    for (idx_t i = 1; i <= count; i++) {
        keys.push_back(static_cast<data_t>(i) * gap);
    }
    std::mt19937 rng(seed);
    std::shuffle(keys.begin(), keys.end(), rng);
    
    auto write_guard = table.GetWriteTableGuard();
    for (idx_t i = 0; i < count; i++) {
        write_guard.Rows().push_back({Tuple{keys[i], static_cast<data_t>(i)}, TupleMeta()});
    }
    write_guard.Drop();
}


std::unordered_map<data_t, idx_t> BuildKeyMapping(Table &table) {
    std::unordered_map<data_t, idx_t> mapping;
    auto read_guard = table.GetReadTableGuard();
    for (idx_t i = 0; i < read_guard.Rows().size(); i++) {
        data_t key = read_guard.Rows()[i].tuple_.KeyFromTuple(table.schema_.GetKeyAttr("c0"));
        mapping[key] = i; // row id is i
    }
    return mapping;
}

void VerifyRangeResult(const std::vector<idx_t>& result, const std::vector<idx_t>& expected) {
    std::vector<idx_t> sortedResult = result;
    std::vector<idx_t> sortedExpected = expected;
    std::sort(sortedResult.begin(), sortedResult.end());
    std::sort(sortedExpected.begin(), sortedExpected.end());
    EXPECT_EQ(sortedResult, sortedExpected);
}


namespace babydb {


TEST(Project1ArtIndexMVCC, SortedKeys_RangeQuery) {
    Schema schema{"c0", "c1"};
    Table table("sorted_range_query", schema);
    BuildSortedTable(table, 100000);
    ArtIndex index("art_sorted", table, "c0");
    auto mapping = BuildKeyMapping(table);
    std::vector<idx_t> result1;
    index.ScanRange({20000, 30000, true, true}, result1);
    std::vector<idx_t> expected1;
    for (idx_t i = 20000; i <= 30000; i++) {
        expected1.push_back(mapping[i]);
    }
    VerifyRangeResult(result1, expected1);

    std::vector<idx_t> result2;
    index.ScanRange({50000, 60000, false, false}, result2);
    std::vector<idx_t> expected2;
    for (idx_t i = 50000 + 1; i < 60000; i++) {
        expected2.push_back(mapping[i]);
    }
    VerifyRangeResult(result2, expected2);
}

TEST(Project1ArtIndexMVCC, SortedKeys_RangeQuery_MultipleRanges) {
    Schema schema{"c0", "c1"};
    Table table("sorted_multi_range", schema);
    BuildSortedTable(table, 100000);
    ArtIndex index("art_sorted_multi_range", table, "c0");
    auto mapping = BuildKeyMapping(table);

    for (idx_t round = 0; round < 10000; round++) {
        std::vector<idx_t> result;
        index.ScanRange({round * 10, round * 10 + 5, true, true}, result);
        std::vector<idx_t> expected;
        for (idx_t i = round * 10; i <= round * 10 + 5; i++) {
            expected.push_back(mapping[i]);
        }
        VerifyRangeResult(result, expected);
    }
}

TEST(Project1ArtIndexMVCC, RandomKeys_OnlyPointQuery) {
    Schema schema{"c0", "c1"};
    Table table("random_only_point", schema);
    BuildRandomTable(table, 100000, 123);
    ArtIndex index("art_random", table, "c0");
    auto mapping = BuildKeyMapping(table);
    std::vector<data_t> testKeys = {10, 50000, 100000};
    for (data_t k : testKeys) {
        EXPECT_EQ(index.LookupKey(k), mapping[k]);
    }
}

TEST(Project1ArtIndexMVCC, RandomKeys_RangeQuery) {
    Schema schema{"c0", "c1"};
    Table table("random_range_query", schema);
    BuildRandomTable(table, 100000, 456);
    ArtIndex index("art_random", table, "c0");
    auto mapping = BuildKeyMapping(table);
    std::vector<data_t> allKeys;
    {
        auto read_guard = table.GetReadTableGuard();
        for (const auto &row : read_guard.Rows())
            allKeys.push_back(row.tuple_.KeyFromTuple(table.schema_.GetKeyAttr("c0")));
    }
    std::sort(allKeys.begin(), allKeys.end());
    data_t low = allKeys.front() + 1000;
    data_t high = allKeys.front() + 5000;
    std::vector<idx_t> expected;
    for (data_t key : allKeys) {
        if (key >= low && key <= high)
            expected.push_back(mapping[key]);
    }
    std::vector<idx_t> result;
    index.ScanRange({low, high, true, true}, result);
    VerifyRangeResult(result, expected);
}

TEST(Project1ArtIndexMVCC, SparseKeys_OnlyPointQuery) {
    Schema schema{"c0", "c1"};
    Table table("sparse_only_point", schema);
    BuildSparseTable(table, 100000, 10000);
    ArtIndex index("art_sparse", table, "c0");
    auto mapping = BuildKeyMapping(table);
    for (idx_t i = 1; i <= 100000; i += 10000) {
        data_t key = static_cast<data_t>(i) * 10000;
        EXPECT_EQ(index.LookupKey(key), mapping[key]);
    }
}

TEST(Project1ArtIndexMVCC, SparseKeys_RangeQuery) {
    Schema schema{"c0", "c1"};
    Table table("sparse_range_query", schema);
    BuildSparseTable(table, 100000, 10000, 890);
    ArtIndex index("art_sparse", table, "c0");
    auto mapping = BuildKeyMapping(table);
    std::vector<idx_t> result;
    index.ScanRange({100000, 500000, true, true}, result);
    std::vector<idx_t> expected;
    {
        auto read_guard = table.GetReadTableGuard();
        for (const auto &row : read_guard.Rows()) {
            data_t k = row.tuple_.KeyFromTuple(table.schema_.GetKeyAttr("c0"));
            if (k >= 100000 && k <= 500000)
                expected.push_back(mapping[k]);
        }
    }
    VerifyRangeResult(result, expected);
}

TEST(Project1ArtIndexMVCC, MixedReadWrite_HighQueryRatio) {
    Schema schema{"c0", "c1"};
    Table table("mixed_high_query", schema);
    const idx_t count = 100000;
    BuildRandomTable(table, count, 789);
    ArtIndex index("art_mixed_high", table, "c0");
    auto mapping = BuildKeyMapping(table);
    std::vector<data_t> allKeys;
    {
        auto read_guard = table.GetReadTableGuard();
        for (const auto &row : read_guard.Rows())
            allKeys.push_back(row.tuple_.KeyFromTuple(table.schema_.GetKeyAttr("c0")));
    }
    std::sort(allKeys.begin(), allKeys.end());
    std::mt19937 rng(789);
    std::uniform_int_distribution<> dist(0, allKeys.size() - 1);
    for (idx_t i = 0; i < 100000; i++) {
        idx_t idx = dist(rng);
        data_t key = allKeys[idx];
        EXPECT_EQ(index.LookupKey(key), mapping[key]);
    }
}

TEST(Project1ArtIndexMVCC, RandomKeys_AlternateInsertQuery) {
    Schema schema{"c0", "c1"};
    Table table("random_alt_insert_query", schema);
    const idx_t count = 100000;
    std::vector<data_t> keys;
    for (idx_t i = 1; i <= count; i++) {
        keys.push_back(i);
    }
    std::mt19937 rng(1234);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (idx_t start = 0; start < count; start += 1000) {
        {
            auto write_guard = table.GetWriteTableGuard();
            for (idx_t i = start; i < start + 1000 && i < count; i++) {
                write_guard.Rows().push_back({Tuple{static_cast<data_t>(keys[i]), static_cast<data_t>(i)}, TupleMeta()});
            }
            write_guard.Drop();
        }
        {
            ArtIndex tempIndex("art_temp", table, "c0");
            auto mapping = BuildKeyMapping(table);
            idx_t cur = std::min(start + 1000 - 1, count - 1);
            EXPECT_EQ(tempIndex.LookupKey(keys[cur]), mapping[keys[cur]]);
        }
    }
    ArtIndex index("art_random_alt", table, "c0");
    auto mapping = BuildKeyMapping(table);
    for (idx_t i = 0; i < count; i += 5000) {
        EXPECT_EQ(index.LookupKey(keys[i]), mapping[keys[i]]);
    }
}

TEST(Project1ArtIndexMVCC, RandomKeys_BulkInsertThenBulkQuery) {
    Schema schema{"c0", "c1"};
    Table table("random_bulk", schema);
    BuildRandomTable(table, 100000, 567);
    ArtIndex index("art_random_bulk", table, "c0");
    auto mapping = BuildKeyMapping(table);
    std::vector<data_t> allKeys;
    {
        auto read_guard = table.GetReadTableGuard();
        for (const auto &row : read_guard.Rows())
            allKeys.push_back(row.tuple_.KeyFromTuple(table.schema_.GetKeyAttr("c0")));
    }
    std::sort(allKeys.begin(), allKeys.end());
    for (idx_t i = 0; i < allKeys.size(); i += 5000) {
        EXPECT_EQ(index.LookupKey(allKeys[i]), mapping[allKeys[i]]);
    }
}

TEST(Project1ArtIndexMVCC, SparseKeys_BulkInsertThenBulkQuery) {
    Schema schema{"c0", "c1"};
    Table table("sparse_bulk", schema);
    BuildSparseTable(table, 100000, 10000, 890);
    ArtIndex index("art_sparse_bulk", table, "c0");
    auto mapping = BuildKeyMapping(table);
    for (idx_t i = 1; i <= 100000; i += 2000) {
        data_t key = static_cast<data_t>(i) * 10000;
        EXPECT_EQ(index.LookupKey(key), mapping[key]);
    }
}

TEST(Project1ArtIndexMVCC, DenseKeys_WithUpdates_PointQuery) {
    Schema schema{"c0", "c1"};
    Table table("dense_updates_point", schema);
    BuildSortedTable(table, 100000);
    ArtIndex index("art_dense", table, "c0");
   
    index.InsertEntry(50000, 500000, 50);
    index.InsertEntry(50000, 500001, 100);
    index.InsertEntry(50000, 500002, 150);
    EXPECT_EQ(index.LookupKey(50000, 75), 500000);
    EXPECT_EQ(index.LookupKey(50000, 100), 500001);
    EXPECT_EQ(index.LookupKey(50000, 200), 500002);
    EXPECT_EQ(index.LookupKey(50000, 40), 50000);
}

TEST(Project1ArtIndexMVCC, LongVersionChain_RangeQuery_AllKeys) {
    Schema schema{"c0", "c1"};
    Table table("long_version_chain_all", schema);
    const idx_t count = 1000;

    BuildSortedTable(table, count);
    ArtIndex index("art_long_chain_all", table, "c0");
    
    const idx_t numUpdates = 10000;
   
    for (idx_t key = 200; key <= 600; key++) {
        for (idx_t i = 0; i < numUpdates; i++) {
            index.InsertEntry(key, key * 1000000 + i, 100 + i);
        }
    }

    idx_t queryTs = 100 + numUpdates / 2;

    for (idx_t key = 200; key <= 600; key++) {
        idx_t expected = key * 1000000 + (queryTs - 100);
        EXPECT_EQ(index.LookupKey(key, queryTs), expected);
    }


    std::vector<idx_t> result;
    index.ScanRange({200, 600, true, true}, result, queryTs);
    
    for (idx_t key = 200; key <= 600; key++) {
        idx_t expected = key * 1000000 + (queryTs - 100);
        bool found = false;
        for (auto v : result) {
            if (v == expected) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Key " << key << " expected version " << expected 
                           << " not found in range query result.";
    }
}

} // namespace babydb
