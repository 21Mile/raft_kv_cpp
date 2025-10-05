#include "m_utils.hpp"
#include "mlog.hpp"
#include "gtest/gtest.h"
#include "defer.hpp"

TEST(TESTCASE, test0)
{
    EXPECT_EQ(0, 0); // 第一个值为预期值，第二个值为进行的运算
    EXPECT_EQ("a", "a");
}

TEST(TESTCASE, DeferShouldExecuteOnScopeExit)
{
    bool executed = false;
    {
        DEFER
        {
            mlog::LOG("Defer函数运行测试");
            executed = true;
        };
        EXPECT_FALSE(executed);
    }
    EXPECT_TRUE(executed);
}

TEST(TESTCASE, DeferCanBeCanceled)
{
    bool executed = false;
    {
        auto guard = DeferTag{} + [&]()
        { executed = true; };
        guard.cancel();
    }
    EXPECT_FALSE(executed);
}