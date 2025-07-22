#pragma once

// The base template for std::char_traits has been removed in LLVM 19,
// only the specializations for char, wchar_t, char8_t, char16_t char32_t
// are provided. folly::Ranges triggers the instantiation of
// std::char_traits<unsigned char>, resulting in a compilation error.

#if defined(__clang__) && (__clang_major__ == 19)

#include <string>

namespace arcticdb::workaround
{
    template<class CharT, class BaseT>
    class traits_cloner
    {
    public:
        using char_type = CharT;

        using base_type = BaseT;
        using base_traits = std::char_traits<base_type>;

        static std::size_t length(char_type const* s)
        {
            return base_traits::length(reinterpret_cast<base_type const*>(s));
        }

        static int compare(char_type const* s1, char_type const* s2, std::size_t count)
        {
            return base_traits::compare(
                reinterpret_cast<base_type const*>(s1),
                reinterpret_cast<base_type const*>(s2), count
            );
        }
        
        static char_type* copy(char_type* dest, char_type const* src, std::size_t count)
        {
            return reinterpret_cast<char_type*>(
                base_traits::copy(reinterpret_cast<base_type*>(dest),
                reinterpret_cast<base_type const*>(src), count)
            );
        }

        static void assign(char_type& c1, char_type const& c2) noexcept
        {
            c1 = c2;
        }
        
        static char_type const* find(char_type const* ptr, std::size_t count, char_type const& ch)
        {
            return reinterpret_cast<char_type const*>(
                base_traits::find(reinterpret_cast<base_type const*>(ptr),
                count,
                reinterpret_cast<base_type const&>(ch))
           );
        }
    };
}

template <>
class std::char_traits<unsigned char>
    : public arcticdb::workaround::traits_cloner<unsigned char, char>
{
};

#endif

#include <folly/Range.h>

