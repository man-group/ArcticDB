#pragma once

// An internal implementation of folly's enumerate: https://github.com/facebook/folly/blob/main/folly/container/Enumerate.h

#include <iterator>
#include <arcticdb/util/preprocess.hpp>

namespace arcticdb {

namespace detail {

template <class T>
struct ConstType {
    using type = const T;
};

template <class T>
struct ConstType<T&> {
    using type = const T&;
};

template <class T>
struct ConstType<T*> {
    using type = const T*;
};

template <class Iterator>
class Enumerator {
public:
    constexpr explicit Enumerator(Iterator it) : it_(std::move(it)) {}

    class Proxy {
    public:
        using difference_type = ssize_t;
        using value_type = typename std::iterator_traits<Iterator>::value_type;
        using reference = typename std::iterator_traits<Iterator>::reference;
        using pointer = typename std::iterator_traits<Iterator>::pointer;
        using iterator_category = std::input_iterator_tag;

        ARCTICDB_FORCE_INLINE constexpr explicit Proxy(const Enumerator& enumerator) : 
            index(enumerator.idx_), 
            ref(*enumerator.it_) {}

        ARCTICDB_FORCE_INLINE constexpr reference operator*() { return ref; }
        ARCTICDB_FORCE_INLINE constexpr pointer operator->() {
            return std::addressof(ref);
        }

        ARCTICDB_FORCE_INLINE constexpr typename ConstType<reference>::type
        operator*() const {
            return ref;
        }
        ARCTICDB_FORCE_INLINE constexpr typename ConstType<pointer>::type operator->()
        const {
            return std::addressof(ref);
        }

        const size_t index;
        reference ref;
    };

    ARCTICDB_FORCE_INLINE constexpr Proxy operator*() const { return Proxy(*this); }

    ARCTICDB_FORCE_INLINE constexpr Enumerator& operator++() {
        ++it_;
        ++idx_;
        return *this;
    }

    template <typename OtherType>
    ARCTICDB_FORCE_INLINE constexpr bool operator==(
        const Enumerator<OtherType>& rhs) const {
        return it_ == rhs.it_;
    }

    template <typename OtherType>
    ARCTICDB_FORCE_INLINE constexpr bool operator!=(
        const Enumerator<OtherType>& rhs) const {
        return !(it_ == rhs.it_);
    }

private:
    template <typename OtherType>
    friend class Enumerator;

    Iterator it_;
    size_t idx_ = 0;
};

template <class RangeType>
class EnumeratorImpl {
    RangeType range_;
    using BeginType = decltype(std::declval<RangeType>().begin());
    using EndType = decltype(std::declval<RangeType>().end());

public:
    constexpr explicit EnumeratorImpl(RangeType&& r) : range_(std::forward<RangeType>(r)) {}

    constexpr Enumerator<BeginType> begin() {
        return Enumerator<BeginType>(range_.begin());
    }
    constexpr Enumerator<EndType> end() {
        return Enumerator<EndType>(range_.end());
    }
};

} // namespace detail

template <class RangeType>
constexpr detail::EnumeratorImpl<RangeType> enumerate(RangeType&& r) {
    return detail::EnumeratorImpl<RangeType>(std::forward<RangeType>(r));
}

} // namespace arcticdb