// Copyright 2023 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <bsoncxx/config/prelude.hpp>

#if defined(BSONCXX_POLY_USE_MNMLSTC)

#include <core/string.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

using ::core::basic_string_view;
using ::core::string_view;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#elif defined(BSONCXX_POLY_USE_BOOST)

#include <boost/version.hpp>

#if BOOST_VERSION >= 106100

#include <boost/utility/string_view.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

using ::boost::basic_string_view;
using ::boost::string_view;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#else

#include <boost/utility/string_ref.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

template <typename charT, typename traits = std::char_traits<charT>>
using basic_string_view = ::boost::basic_string_ref<charT, traits>;
using string_view = ::boost::string_ref;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#endif

#elif defined(BSONCXX_POLY_USE_STD_EXPERIMENTAL)

#include <experimental/string_view>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

using ::std::experimental::basic_string_view;
using ::std::experimental::string_view;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#elif defined(BSONCXX_POLY_USE_STD)

#include <string_view>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

using ::std::basic_string_view;
using ::std::string_view;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

#elif defined(BSONCXX_POLY_USE_IMPLS)

#include <algorithm>
#include <cstddef>
#include <ios>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

#include <bsoncxx/stdx/operators.hpp>
#include <bsoncxx/stdx/type_traits.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace stdx {

/**
 * @brief Implementation of std::string_view-like class template
 */
template <typename Char, typename Traits = std::char_traits<Char>>
class basic_string_view : bsoncxx::detail::equality_operators, bsoncxx::detail::ordering_operators {
   public:
    // Pointer to (non-const) character type
    using pointer = Char*;
    // Pointer to const-character type
    using const_pointer = const Char*;
    // Type representing the size of a string
    using size_type = std::size_t;
    // Type representing the offset within a string
    using difference_type = std::ptrdiff_t;
    // The type of the string character
    using value_type = Char;

    // Constant sentinel value to represent an impossible/invalid string position
    static constexpr size_type npos = static_cast<size_type>(-1);

   private:
    // Pointer to the beginning of the string being viewed
    const_pointer _begin = nullptr;
    // The size of the array that is being viewed via `_begin`
    size_type _size = 0;

   public:
    using traits_type = Traits;
    using reference = Char&;
    using const_reference = const Char&;
    using const_iterator = const_pointer;
    using iterator = const_iterator;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using reverse_iterator = const_reverse_iterator;

    /**
     * @brief Default constructor. Constructs to an empty/null string view
     */
    constexpr basic_string_view() noexcept = default;
    constexpr basic_string_view(const basic_string_view&) noexcept = default;
    bsoncxx_cxx14_constexpr basic_string_view& operator=(const basic_string_view&) noexcept =
        default;

    /**
     * @brief Construct a new string view from a pointer-to-character and an
     * array length.
     */
    constexpr basic_string_view(const_pointer s, size_type count) : _begin(s), _size(count) {}

    /**
     * @brief Construct a new string view from a C-style null-terminated character array.
     *
     * The string size is inferred as-if by strlen()
     */
    constexpr basic_string_view(const_pointer s) : _begin(s), _size(traits_type::length(s)) {}

    /**
     * @brief Implicit conversion from string-like ranges.
     *
     * Requires that `StringLike` is a non-array contiguous range with the same
     * value type as this string view, and is a std::string-like value.
     */
    template <typename Alloc>
    constexpr basic_string_view(
        const std::basic_string<value_type, traits_type, Alloc>& str) noexcept
        : _begin(str.data()), _size(str.size()) {}

#if __cpp_lib_string_view
    constexpr basic_string_view(std::basic_string_view<value_type, traits_type> sv) noexcept
        : _begin(sv.data()), _size(sv.size()) {}
#endif

    // Construction from a null pointer is deleted
    basic_string_view(std::nullptr_t) = delete;

    constexpr const_iterator begin() const noexcept {
        return const_iterator(_begin);
    }
    constexpr const_iterator end() const noexcept {
        return begin() + size();
    }
    constexpr const_iterator cbegin() const noexcept {
        return begin();
    }
    constexpr const_iterator cend() const noexcept {
        return end();
    }

    constexpr const_reverse_iterator rbegin() const noexcept {
        return const_reverse_iterator{end()};
    }

    constexpr const_reverse_iterator rend() const noexcept {
        return const_reverse_iterator{begin()};
    }

    constexpr const_reverse_iterator crbegin() const noexcept {
        return const_reverse_iterator{cend()};
    }

    constexpr const_reverse_iterator crend() const noexcept {
        return const_reverse_iterator{crbegin()};
    }

    /**
     * @brief Access the Nth element of the referred-to string
     *
     * @param offset A zero-based offset within the string to access. Must be less
     * than size()
     */
    constexpr const_reference operator[](size_type offset) const {
        return _begin[offset];
    }

    /**
     * @brief Access the Nth element of the referred-to string.
     *
     * @param pos A zero-based offset within the string to access. If not less
     * than size(), throws std::out_of_range
     */
    bsoncxx_cxx14_constexpr const_reference at(size_type pos) const {
        if (pos >= size()) {
            throw std::out_of_range{"bsoncxx::stdx::basic_string_view::at()"};
        }
        return _begin[pos];
    }
    /// Access the first character in the string
    constexpr const_reference front() const {
        return (*this)[0];
    }
    /// Access the last character in the string
    constexpr const_reference back() const {
        return (*this)[size() - 1];
    }

    /// Obtain a pointer to the beginning of the referred-to character array
    constexpr const_pointer data() const noexcept {
        return _begin;
    }
    /// Obtain the length of the referred-to string, in number of characters
    constexpr size_type size() const noexcept {
        return _size;
    }
    /// Obtain the length of the referred-to string, in number of characters
    constexpr size_type length() const noexcept {
        return size();
    }
    /// Return `true` if size() == 0, otherwise `false`
    constexpr bool empty() const noexcept {
        return size() == 0;
    }
    /// Return the maximum value that could be returned by size()
    constexpr size_type max_size() const noexcept {
        return static_cast<size_type>(std::numeric_limits<difference_type>::max());
    }

    /**
     * @brief In-place modify the string_view to view N fewer characters from the beginning
     *
     * @param n The number of characters to remove from the beginning. Must be less than size()
     */
    bsoncxx_cxx14_constexpr void remove_prefix(size_type n) {
        _begin += n;
        _size -= n;
    }

    /**
     * @brief In-place modify the string_view to view N fewer characters from the end
     *
     * @param n The number of characters to remove from the end. Must be less than size()
     */
    bsoncxx_cxx14_constexpr void remove_suffix(size_type n) {
        _size -= n;
    }

    /**
     * @brief Swap the reference with another string_view
     */
    bsoncxx_cxx14_constexpr void swap(basic_string_view& other) {
        std::swap(_begin, other._begin);
        std::swap(_size, other._size);
    }

    /**
     * @brief Copy the contents of the viewed string into the given output destination.
     *
     * @param dest The destination at which to write characters
     * @param count The maximum number of characters to copy.
     * @param pos The offset within the viewed string to begin copying from.
     * @returns The number of characters that were copied to `dest`. The number
     * of copied characters is always the lesser of `size()-pos` and `count`
     *
     * @throws std::out_of_range if pos > size()
     */
    size_type copy(pointer dest, size_type count, size_type pos = 0) const {
        if (pos > size()) {
            throw std::out_of_range{"bsoncxx::stdx::basic_string_view::substr()"};
        }
        count = (std::min)(count, size() - pos);
        Traits::copy(dest, data() + pos, count);
        return count;
    }

    /**
     * @brief Obtain a substring of this string
     *
     * @param pos The zero-based index at which to start the new string.
     * @param count The number of characters to include following `pos` in the new string.
     * Automatically clamped to the available size
     *
     * @throws std::out_of_range if `pos` is greater than this->size()
     */
    bsoncxx_cxx14_constexpr basic_string_view substr(size_type pos = 0,
                                                     size_type count = npos) const {
        if (pos > size()) {
            throw std::out_of_range{"bsoncxx::stdx::basic_string_view::substr()"};
        }
        return basic_string_view(_begin + pos, (std::min)(count, size() - pos));
    }

    /**
     * @brief Compare two strings lexicographically
     *
     * @param other The "right hand" operand of the comparison
     * @returns `0` If *this == other
     * @returns `n : n < 0` if *this is "less than" other.
     * @returns `n : n > 0` if *this is "greater than" other.
     */
    constexpr int compare(basic_string_view other) const noexcept {
        // Another level of indirection to support restricted C++11 constexpr
        return _compare2(Traits::compare(data(), other.data(), (std::min)(size(), other.size())),
                         other);
    }

    /**
     * @brief Compare *this with the given C-string
     *
     * @returns compare(basic_string_view(cstr))
     */
    constexpr int compare(const_pointer cstr) const {
        return compare(basic_string_view(cstr));
    }

    /**
     * @brief Compare a substring of *this with `other`
     *
     * @returns substr(po1, count1).compare(other)
     */
    constexpr int compare(size_type pos1, size_type count1, basic_string_view other) const {
        return substr(pos1, count1).compare(other);
    }

    /**
     * @brief Compare a substring of *this with the given C-string
     *
     * @returns substr(pos1, count1, basic_string_view(cstr))
     */
    constexpr int compare(size_type pos1, size_type count1, const_pointer cstr) const {
        return compare(pos1, count1, basic_string_view(cstr));
    }

    /**
     * @brief Compare a substring of *this with a substring of `other`
     *
     * @returns substr(pos1, count1).compare(other.substr(pos2, count2))
     */
    constexpr int compare(size_type pos1,
                          size_type count1,
                          basic_string_view other,
                          size_type pos2,
                          size_type count2) const {
        return substr(pos1, count1).compare(other.substr(pos2, count2));
    }

    /**
     * @brief Compare a substring of *this with a string viewed through the given pointer+size
     *
     * @returns substr(pos1, count1).compare(basic_string_view(str, count2))
     */
    constexpr int compare(size_type pos1,
                          size_type count1,
                          const_pointer str,
                          size_type count2) const {
        return substr(pos1, count1).compare(basic_string_view(str, count2));
    }

    /**
     * @brief Find the zero-based offset of the left-most occurrence of the given infix,
     * starting with pos. If infix does not occur, returns npos.
     */
    bsoncxx_cxx14_constexpr size_type find(basic_string_view infix, size_type pos = 0) const
        noexcept {
        if (pos > size()) {
            return npos;
        }
        basic_string_view sub = this->substr(pos);
        if (infix.empty()) {
            // The empty string is always "present" at the beginning of any string
            return pos;
        }
        const_iterator found = std::search(sub.begin(), sub.end(), infix.begin(), infix.end());
        if (found == sub.end()) {
            return npos;
        }
        return static_cast<size_type>(found - begin());
    }

    /**
     * @brief Find the zero-based offset of the right-most occurrence of the given infix,
     * starting with (and including) pos. If infix does not occur, returns npos.
     */
    bsoncxx_cxx14_constexpr size_type rfind(basic_string_view infix, size_type pos = npos) const
        noexcept {
        // Calc the endpos where searching should begin, which includes the infix size
        const size_type substr_size = pos != npos ? pos + infix.size() : pos;
        if (infix.empty()) {
            return (std::min)(pos, size());
        }
        basic_string_view searched = this->substr(0, substr_size);
        auto f = std::search(searched.rbegin(), searched.rend(), infix.rbegin(), infix.rend());
        if (f == searched.rend()) {
            return npos;
        }
        return static_cast<size_type>(rend() - f) - infix.size();
    }

    /**
     * @brief Find the zero-based index of the left-most occurrence of any character of the given
     * set, starting at pos
     */
    constexpr size_type find_first_of(basic_string_view set, size_type pos = 0) const noexcept {
        return _find_if(pos, [&](value_type chr) { return set.find(chr) != npos; });
    }

    /**
     * @brief Find the zero-based index of the right-most occurrence of any character of the
     * given set, starting at (and including) pos
     */
    constexpr size_type find_last_of(basic_string_view set, size_type pos = npos) const noexcept {
        return _rfind_if(pos, [&](value_type chr) { return set.find(chr) != npos; });
    }

    /**
     * @brief Find the zero-based index of the left-most occurrence of any character that
     * is NOT a member of the given set of characters
     */
    constexpr size_type find_first_not_of(basic_string_view set, size_type pos = 0) const noexcept {
        return _find_if(pos, [&](value_type chr) { return set.find(chr) == npos; });
    }

    /**
     * @brief Find the zero-based index of the right-most occurrence of any character that
     * is NOT a member of the given set of characters, starting at (and including) pos
     */
    constexpr size_type find_last_not_of(basic_string_view set, size_type pos = npos) const
        noexcept {
        return _rfind_if(pos, [&](value_type chr) { return set.find(chr) == npos; });
    }

#pragma push_macro("DECL_FINDERS")
#undef DECL_FINDERS
#define DECL_FINDERS(Name, DefaultPos)                                                    \
    constexpr size_type Name(value_type chr, size_type pos = DefaultPos) const noexcept { \
        return Name(basic_string_view(&chr, 1), pos);                                     \
    }                                                                                     \
    constexpr size_type Name(const_pointer cstr, size_type pos, size_type count) const {  \
        return Name(basic_string_view(cstr, count), pos);                                 \
    }                                                                                     \
    constexpr size_type Name(const_pointer cstr, size_type pos = DefaultPos) const {      \
        return Name(basic_string_view(cstr), pos);                                        \
    }                                                                                     \
    BSONCXX_FORCE_SEMICOLON
    DECL_FINDERS(find, 0);
    DECL_FINDERS(rfind, npos);
    DECL_FINDERS(find_first_of, 0);
    DECL_FINDERS(find_last_of, npos);
    DECL_FINDERS(find_first_not_of, 0);
    DECL_FINDERS(find_last_not_of, npos);
#pragma pop_macro("DECL_FINDERS")

    /**
     * @brief Explicit-conversion to a std::basic_string
     */
    template <typename Allocator>
    explicit operator std::basic_string<Char, Traits, Allocator>() const {
        return std::basic_string<Char, Traits, Allocator>(data(), size());
    }

#if __cpp_lib_string_view
    explicit operator std::basic_string_view<value_type, traits_type>() const noexcept {
        return std::basic_string_view<value_type, traits_type>(data(), size());
    }
#endif

   private:
    // Additional level-of-indirection for constexpr compare()
    constexpr int _compare2(int diff, basic_string_view other) const noexcept {
        // "diff" is the diff according to Traits::cmp
        return diff ? diff : static_cast<int>(size() - other.size());
    }

    // Implementation of equality comparison
    constexpr friend bool tag_invoke(bsoncxx::detail::equal_to,
                                     basic_string_view left,
                                     basic_string_view right) noexcept {
        return left.size() == right.size() && left.compare(right) == 0;
    }

    // Implementation of a three-way-comparison
    constexpr friend bsoncxx::detail::strong_ordering tag_invoke(
        bsoncxx::detail::compare_three_way cmp,
        basic_string_view left,
        basic_string_view right) noexcept {
        return cmp(left.compare(right), 0);
    }

    friend std::basic_ostream<Char, Traits>& operator<<(std::basic_ostream<Char, Traits>& out,
                                                        basic_string_view self) {
        out << std::basic_string<Char, Traits>(self);
        return out;
    }

    // Find the first in-bounds index I in [pos, size()) where the given predicate
    // returns true for substr(I). If no index exists, returns npos
    template <typename F>
    bsoncxx_cxx14_constexpr size_type _find_if(size_type pos, F pred) const noexcept {
        const auto sub = substr(pos);
        const iterator found = std::find_if(sub.begin(), sub.end(), pred);
        if (found == end()) {
            return npos;
        }
        return static_cast<size_type>(found - begin());
    }

    // Find the LAST index I in [0, pos] where the given predicate returns true for
    // substr(0, I). If no such index exists, returns npos.
    template <typename F>
    bsoncxx_cxx14_constexpr size_type _rfind_if(size_type pos, F pred) const noexcept {
        // Adjust 'pos' for an inclusive range in substr()
        const auto rpos = pos == npos ? npos : pos + 1;
        // The substring that will be searched:
        const auto prefix = substr(0, rpos);
        const const_reverse_iterator found = std::find_if(prefix.rbegin(), prefix.rend(), pred);
        if (found == rend()) {
            return npos;
        }
        // Adjust by 1 to account for reversed-ness
        return static_cast<size_type>(rend() - found) - 1u;
    }
};

// Required to define this here for C++≤14 compatibility. Can be removed in C++≥17
template <typename C, typename Tr>
const std::size_t basic_string_view<C, Tr>::npos;

using string_view = basic_string_view<char>;

}  // namespace stdx
}  // namespace v_noabi
}  // namespace bsoncxx

namespace std {

template <typename CharT, typename Traits>
struct hash<bsoncxx::v_noabi::stdx::basic_string_view<CharT, Traits>>
    : private std::hash<std::basic_string<CharT, Traits>> {
    std::size_t operator()(
        const bsoncxx::v_noabi::stdx::basic_string_view<CharT, Traits>& str) const {
        return std::hash<std::basic_string<CharT, Traits>>::operator()(
            std::basic_string<CharT, Traits>(str.data(), str.size()));
    }
};

}  // namespace std

#else
#error "Cannot find a valid polyfill for string_view"
#endif

#include <bsoncxx/config/postlude.hpp>

namespace bsoncxx {
namespace stdx {

using ::bsoncxx::v_noabi::stdx::basic_string_view;
using ::bsoncxx::v_noabi::stdx::string_view;

}  // namespace stdx
}  // namespace bsoncxx
