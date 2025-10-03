#ifndef NONCOPYABLE_H
#define NONCOPYABLE_H

// 禁止拷贝的一个类：可以用来实现单例模式
namespace monsoon {
    class Nonecopyable {
     public:
      Nonecopyable() = default; 
      ~Nonecopyable() = default;
      Nonecopyable(const Nonecopyable &) = delete; //禁用引用构造
      Nonecopyable operator=(const Nonecopyable) = delete; // 禁用赋值构造
    };
    }  // namespace monsoon

#endif // !NONCOPYABLE_H
