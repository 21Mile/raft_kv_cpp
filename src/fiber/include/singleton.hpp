
#ifndef SINGLETON_H
#define SINGLETON_H

// @brief 单例模式
namespace monsoon
{
    // ​​泛型单例获取器​​的实现模板
    template <class T, class X, int N>
    T &GetInstanceX()
    {               // 模板函数：返回一个静态变量v
        static T v; // 利用 ​​C++ 静态局部变量特性​​：首次调用时初始化，后续调用返回同一实例
        return v;
    }
    template <class T, class X, int N>
    std::shared_ptr<T> GetInstancePtr()
    {
        static std::shared_ptr<T> v(new T);
        return v;
    }

    // @brief 模板类
    // 单例裸指针
    template <class T, class X = void, int N = 0>
    class Singleton
    {
    public:
        static T *GetInstance()
        {
            static T v;
            return &v;
        }
    };

    template <class T, class X = void, int N = 0>
    class SingletonPtr
    {
    public:
        static std::shared_ptr<T> GetInstancePtr()
        {
            static std::shared_ptr<T> v = std::make_shared<T>();
            return v;
        }
    };

} // namespace monsoon

#endif // !SINGLETON_H
