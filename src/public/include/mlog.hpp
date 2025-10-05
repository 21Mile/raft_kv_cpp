

#ifndef PUBLIC_MLOG
#define PUBLIC_MLOG

#include <iostream>
#include <chrono>
#include <iomanip>
#include "string"
namespace mlog
{
    // 获取当前时间字符串
    inline std::string getCurrentTime()
    {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        std::tm tm = *std::localtime(&time);

        char buffer[20];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
        return buffer;
    }

    // 日志函数
    inline void LOG(const std::string &msg)
    {
        std::cout << "[" << getCurrentTime() << "] LOG: " << msg << std::endl;
    }

    // 错误函数
    inline void ERROR(const std::string &msg)
    {
        std::cerr << "[" << getCurrentTime() << "] \033[31mERROR\033[0m: " << msg << std::endl;
    }

    // 警告函数
    inline void WARNING(const std::string &msg)
    {
        std::cerr << "[" << getCurrentTime() << "] \033[33mWARNING\033[0m: " << msg << std::endl;
    }

} // namespace  mlog

#endif // !PUBLIC_MLOG