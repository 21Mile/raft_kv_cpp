#ifndef UTILS_H
#define UTILS_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <type_traits>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable> // pthread_condition_t
#include <functional>
#include <iostream>
#include <mutex> // pthread_mutex_t
#include <queue>
#include <random>
#include <sstream>
#include <thread>

#include "config.hpp"
#include "defer.hpp"


// @brief 动态打印函数
void DPrintf(const char *format, ...);
// @brief 自定义断言
void myAssert(bool condition, std::string message = "Assertion failed!");

// @class 模板类：动态字符串格式化
template <typename... Args>
std::string format(const char *format_str, Args... args)
{
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // "\0"
    if (size_s <= 0)
    {
        throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1); // remove '\0'
}
// @brief 函数定义 时间点：用于时间类的库

std::chrono::_V2::system_clock::time_point now();
// @brief 函数定义：随机获取一个选举超时时间
std::chrono::milliseconds getRandomizedElectionTimeout();
// @brief 函数定义：随机睡眠指定的时间，单位为毫秒
void sleepNMilliseconds(int N);


// 定义状态码字符串

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

// 获取可用的端口
bool isReleasePort(unsigned short usPort);
bool getReleasePort(short &port);

#endif // !UTILS_H