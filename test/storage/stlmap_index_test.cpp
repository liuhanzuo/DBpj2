#include "gtest/gtest.h"

#include "storage/stlmap_index.hpp"

namespace babydb {

TEST(StlmapIndexTest, BasicTest) {
    Schema schema{"c0", "c1"};
    Table table(schema, "table");
    {
    auto write_guard = table.GetWriteTableGuard();
    write_guard.rows_.push_back({Tuple{0, 1}, TupleMeta{false}});
    write_guard.Drop();
    }
    StlmapIndex index("index", table, 0);

    index.InsertEntry(2, 1);

    EXPECT_EQ(index.Lowerbound(3), index.End());
    EXPECT_EQ(index.Lowerbound(0)->second, 0);
    EXPECT_EQ(index.Lowerbound(1)->second, 1);

    EXPECT_ANY_THROW(index.InsertEntry(2, 2));

    EXPECT_EQ(index.ScanKey(0), 0);

    index.EraseEntry(0, 0);

    EXPECT_EQ(index.ScanKey(0), INVALID_ID);

    EXPECT_EQ(index.Lowerbound(0)->second, 1);
}

}