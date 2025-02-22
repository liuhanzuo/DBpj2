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

    index.InsertEntry(Tuple{2, 3}, 1);

    EXPECT_EQ(index.Lowerbound(3), index.End());
    EXPECT_EQ(index.Lowerbound(0)->second, 0);
    EXPECT_EQ(index.Lowerbound(1)->second, 1);

    EXPECT_EQ(index.InsertEntry(Tuple{2, 4}, 2), false);

    index.EraseEntry(Tuple{0, 1}, 0);

    EXPECT_EQ(index.Lowerbound(0)->second, 1);
}

}