#include "gtest/gtest.h"
#include "storage/art.hpp"

namespace babydb {

TEST(ArtIndexTest, BasicTest) {
    const int N = 1000;
    Schema schema{"c0", "c1"};
    Table table("table", schema);
    {
        auto write_guard = table.GetWriteTableGuard();
        for (int i = 0; i < N; i++) {
            write_guard.Rows().push_back({Tuple{i, i + 100}, TupleMeta()});
        }
    }
    ArtIndex index("art_index", table, "c0");

    for (int i = 0; i < N; i += 100) {
        EXPECT_EQ(index.ScanKey(i), i);
    }
}

TEST(ArtIndexTest, ScanRangeTest) {
    const int N = 1000;
    Schema schema{"c0", "c1"};
    Table table("table", schema);
    {
        auto write_guard = table.GetWriteTableGuard();
        // Insert N rows, for row i, the key is just i, and the payload is i+100.
        for (int i = 0; i < N; i++) {
            write_guard.Rows().push_back({Tuple{i, i + 100}, TupleMeta()});
        }
    }
    // Construct ART index on the table, the construction function will insert the existing keys automaticly.
    ArtIndex index("art_index", table, "c0");

    std::vector<idx_t> result;
    // ScanRange Test: Query the keys in [200, 400] (with the borders).
    index.ScanRange({200, 400, true, true}, result);
    std::vector<idx_t> expected;
    for (int i = 200; i <= 400; i++) {
        expected.push_back(i);
    }
    EXPECT_EQ(result, expected);

    // ScanRange Test: Query the keys in (200, 400) (without the borders).
    index.ScanRange({200, 400, false, false}, result);
    expected.clear();
    for (int i = 201; i < 400; i++) {
        expected.push_back(i);
    }
    EXPECT_EQ(result, expected);
}

} // namespace babydb
