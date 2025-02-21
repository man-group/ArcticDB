#pragma once

#include <mutex>
#include <memory>

namespace arcticdb {
template<typename T>
class LazyInit {
  public:
    const std::shared_ptr<T>& instance() const {
        std::call_once(init_, [&]() { instance_ = std::make_shared<T>(); });
        return instance_;
    }

  private:
    mutable std::shared_ptr<T> instance_;
    mutable std::once_flag init_;
};

} // namespace arcticdb