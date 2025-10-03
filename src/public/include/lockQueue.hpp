
#pragma once
#ifndef LOCK_QUEUE
#define LOCK_QUEUE
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
// @class 异步写日志的日志队列

template <typename T>
class LockQueue // 每个 LockQueue 实例几乎只会装一条对应 raftIndex 的结果
{
public:
    // 多个worker线程都会写日志queue
    void Push(const T &data)
    {
        std::lock_guard<std::mutex> lock(m_mutex); // 使用lock_gurad，即RAII的思想保证锁正确释放
        m_queue.push(data);
        m_condvariable.notify_one(); // 条件变量，通知一个线程
    }

    // 一个线程读日志queue，写日志文件
    T Pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty())
        {
            // 日志队列为空，线程进入wait状态
            m_condvariable.wait(lock); // 这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
        }
        T data = m_queue.front();
        m_queue.pop();
        return data;
    }
    // 添加一个超时时间参数，默认为 50 毫秒;第二个参数为泛型数据
    bool timeOutPop(int timeout, T *ResData)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        // 获取当前时间点，并计算出超时时刻
        auto now = std::chrono::system_clock::now();
        auto timeout_time = now + std::chrono::milliseconds(timeout);

        // 在超时之前，不断检查队列是否为空
        while (m_queue.empty())
        {
            // 如果已经超时了，就返回一个空对象
            if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout)
            {
                return false;
            }
            else
            {
                continue;
            }
        }

        T data = m_queue.front();
        m_queue.pop();
        *ResData = data; // 结果数据，保存在这片内存地址
        return true;
    }

private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_condvariable;
};

// 两个对锁的管理用到了RAII的思想，防止中途出现问题而导致资源无法释放的问题！！！
// std::lock_guard 和 std::unique_lock 都是 C++11 中用来管理互斥锁的工具类，它们都封装了 RAII（Resource Acquisition Is
// Initialization）技术，使得互斥锁在需要时自动加锁，在不需要时自动解锁，从而避免了很多手动加锁和解锁的繁琐操作。
// std::lock_guard 是一个模板类，它的模板参数是一个互斥量类型。当创建一个 std::lock_guard
// 对象时，它会自动地对传入的互斥量进行加锁操作，并在该对象被销毁时对互斥量进行自动解锁操作。std::lock_guard
// 不能手动释放锁，因为其所提供的锁的生命周期与其绑定对象的生命周期一致。 std::unique_lock
// 也是一个模板类，同样的，其模板参数也是互斥量类型。不同的是，std::unique_lock 提供了更灵活的锁管理功能。可以通过
// lock()、unlock()、try_lock() 等方法手动控制锁的状态。当然，std::unique_lock 也支持 RAII
// 技术，即在对象被销毁时会自动解锁。另外， std::unique_lock 还支持超时等待和可中断等待的操作。

// @brief raft的command，也就是状态机kvserver传递给raft节点的命令
class Op
{
public:
    std::string Operation; //"Get" "Put" "Append"
    std::string Key;
    std::string Value;
    std::string ClientId; // 客户端号码
    int RequestId;        // 客户端号码请求的Request的序列号，为了保证线性一致性
    // 对象序列化
    std::string asString() const
    {
        std::stringstream ss;

        boost::archive::text_oarchive oa(ss);
        // 将对象序列化后，写入到stringstream，然后输出为字符串
        oa << *this;
        return ss.str();
    }
    // 将序列化的对象解码为对象
    bool parseFromString(std::string str)
    {
        std::stringstream iss(str);
        boost::archive::text_iarchive ia(iss);
        ia >> *this;
        return true;
    }
    // 友元函数 : 重载运算符，实现序列化方法
    friend std::ostream &operator<<(std::ostream &os, const Op &obj)
    {
        os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
                  obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}"; // 在这里实现自定义的输出格式
        return os;
    }

private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & Operation;
        ar & Key;
        ar & Value;
        ar & ClientId;
        ar & RequestId;
    }
};

#endif // !LOCK_QUEUE
