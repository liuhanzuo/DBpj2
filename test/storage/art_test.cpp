#include "gtest/gtest.h"
#include "storage/art.hpp"

namespace babydb {

TEST(ArtIndexTest, LargeDatasetTest) {
    // 使用较大的数据量
    const int N = 1000;
    Schema schema{"c0", "c1"};
    Table table("table", schema);
    {
        auto write_guard = table.GetWriteTableGuard();
        // 插入 N 行数据，每行的 key 为 i，另一个字段取 i+100
        for (int i = 0; i < N; i++) {
            write_guard.Rows().push_back({Tuple{i, i + 100}, TupleMeta()});
        }
        write_guard.Drop();
    }
    // 构造 ART 索引，构造函数会根据表中的数据自动插入已有的 key
    ArtIndex index("art_index", table, "c0");

    // 测试 ScanKey: 对部分样例 key 应返回对应的 row_id
    for (int i = 0; i < N; i += 100) {
        EXPECT_EQ(index.ScanKey(i), i);
    }

    std::vector<idx_t> result;
    // 测试范围查询：查询 key 在 [200, 400] 之间（包含边界）
    index.ScanRange({200, 400, true, true}, result);
    std::vector<idx_t> expected;
    for (int i = 200; i <= 400; i++) {
        expected.push_back(i);
    }
    EXPECT_EQ(result, expected);

    // 测试范围查询：查询 key 在 (200, 400) 之间（不包含边界）
    index.ScanRange({200, 400, false, false}, result);
    expected.clear();
    for (int i = 201; i < 400; i++) {
        expected.push_back(i);
    }
    EXPECT_EQ(result, expected);
}

} // namespace babydb
