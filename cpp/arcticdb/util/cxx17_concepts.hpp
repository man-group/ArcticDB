#include <type_traits>
#include <functional>

namespace arcticdb {

    template<typename F, typename Arg>
    using is_unary_predicate = std::is_invocable_r<bool, F, Arg>;

    template<typename F, typename Arg>
    constexpr bool is_unary_predicate_v = is_unary_predicate<F, Arg>::value;

    template<typename F, typename G, typename Arg>
    using is_binary_predicate = std::is_invocable_r<bool, F, G, Arg>;

    template<typename F, typename G, typename Arg>
    constexpr bool is_binary_predicate_v = is_binary_predicate<F, G, Arg>::value;

}