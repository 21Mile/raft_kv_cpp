#pragma once
#ifndef MUTEX_H
#define MUTEX_H

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#include "noncopyable.hpp"
#include "utils.hpp"

namespace monsoon
{
    // 1 信号量类
    class Semaphore : Nonecopyable
    {
    public:
        Semaphore(uint32_t count = 0);
        ~Semaphore();  // 析构函数
        void wait();   // 阻塞:无法获取信号量，阻塞当前线程
        void notify(); // 通知功能：在释放信号量的时候，通知其他等待线程
    private:
        sem_t semaphore_;
    };

    // 2 局部锁类模板 : 模板参数 T 允许同一锁机制适配多种底层锁实现，而不需要重写代码
    // ❗ T 必须提供 lock() 和 unlock() 成员函数
    // ❗ T 需要满足 BasicLockable 概念（C++标准要求）
    template <class T>
    struct ScopedLockImpl
    {
    public:
        ScopedLockImpl(T &mutex) : m_(mutex)
        {
            // std::cout << "n lock" << std::endl;
            m_.lock();
            isLocked_ = true;
        }

        void lock()
        {
            if (!isLocked_)
            {
                std::cout << "lock" << std::endl;
                m_.lock();
                isLocked_ = true;
            }
        }

        void unlock()
        {
            if (isLocked_)
            {
                // std::cout << "unlock" << std::endl;
                m_.unlock();
                isLocked_ = false;
            }
        }

        ~ScopedLockImpl()
        {
            // std::cout << "unlock" << std::endl;
            unlock();
        }

    private:
        // mutex
        T &m_;
        // 是否已经上锁
        bool isLocked_;
    };

    // 3 局部读锁类模板
    template <class T>
    struct ReadScopedLockImpl
    {
    public:
        ReadScopedLockImpl(T &mutex) : mutex_(mutex)
        {
            mutex_.rdlock();
            isLocked_ = true;
        }
        ~ReadScopedLockImpl() { unlock(); }
        void lock()
        {
            if (!isLocked_)
            {
                mutex_.rdlock();
                isLocked_ = true;
            }
        }
        void unlock()
        {
            if (isLocked_)
            {
                mutex_.unlock();
                isLocked_ = false;
            }
        }

    private:
        /// mutex
        T &mutex_;
        /// 是否已上锁
        bool isLocked_;
    };
    // 3 局部写锁类模板
    template <class T>
    struct WriteScopedLockImpl
    {
    public:
        WriteScopedLockImpl(T &mutex) : mutex_(mutex)
        {
            mutex_.wrlock();
            isLocked_ = true;
        }

        ~WriteScopedLockImpl() { unlock(); }
        void lock()
        {
            if (!isLocked_)
            {
                mutex_.wrlock();
                isLocked_ = true;
            }
        }
        void unlock()
        {
            if (isLocked_)
            {
                mutex_.unlock();
                isLocked_ = false;
            }
        }

    private:
        /// Mutex
        T &mutex_;
        /// 是否已上锁
        bool isLocked_;
    };

    // 3 Mutex模板类
    class Mutex : Nonecopyable
    {
    public:
        typedef ScopedLockImpl<Mutex> Lock;

        Mutex() { CondPanic(0 == pthread_mutex_init(&m_, nullptr), "lock init success"); }

        void lock() { CondPanic(0 == pthread_mutex_lock(&m_), "lock error"); }

        void unlock() { CondPanic(0 == pthread_mutex_unlock(&m_), "unlock error"); }

        ~Mutex() { CondPanic(0 == pthread_mutex_destroy(&m_), "destroy lock error"); }

    private:
        pthread_mutex_t m_;
    };

    class RWMutex : Nonecopyable
    {
    public:
        // 局部读锁
        typedef ReadScopedLockImpl<RWMutex> ReadLock;
        // 局部写锁
        typedef WriteScopedLockImpl<RWMutex> WriteLock;

        RWMutex() { pthread_rwlock_init(&m_, nullptr); }
        ~RWMutex() { pthread_rwlock_destroy(&m_); }
        // 读锁
        void rdlock() { pthread_rwlock_rdlock(&m_); }
        // 写锁
        void wrlock() { pthread_rwlock_wrlock(&m_); }
        // 解锁
        void unlock() { pthread_rwlock_unlock(&m_); }

    private:
        pthread_rwlock_t m_;
    };
};

#endif // !MUTEX_H
