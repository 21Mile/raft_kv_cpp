#pragma once
#include <utility>
#include <type_traits>

struct DeferTag {};

template <class F>
class DeferGuard {
    F f_;
    bool active_ = true;
public:
    explicit DeferGuard(F&& f) : f_(std::forward<F>(f)) {}
    DeferGuard(const DeferGuard&) = delete;
    DeferGuard& operator=(const DeferGuard&) = delete;
    DeferGuard(DeferGuard&& other) noexcept
        : f_(std::move(other.f_)), active_(other.active_) { other.active_ = false; }
    void cancel() noexcept { active_ = false; }
    ~DeferGuard() { if (active_) f_(); }
};

template <class F>
inline DeferGuard<std::decay_t<F>> operator+(DeferTag, F&& f) {
    return DeferGuard<std::decay_t<F>>(std::forward<F>(f));
}

// 用在 “DEFER { ... };”
#define DEFER auto _defer_##__LINE__ = ::DeferTag{} + [&]()
