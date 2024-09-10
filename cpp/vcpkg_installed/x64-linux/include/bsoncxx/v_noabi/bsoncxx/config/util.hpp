// clang-format off
/**
 * @internal
 * @brief Convert the given macro argument to a string literal, after macro expansion
 */
#define BSONCXX_STRINGIFY(...) BSONCXX_STRINGIFY_IMPL(__VA_ARGS__)
#define BSONCXX_STRINGIFY_IMPL(...) #__VA_ARGS__

/**
 * @brief Token-paste two macro arguments, after macro expansion
 */
#define BSONCXX_CONCAT(A, ...) BSONCXX_CONCAT_IMPL(A, __VA_ARGS__)
#define BSONCXX_CONCAT_IMPL(A, ...) A##__VA_ARGS__

/**
 * @internal
 * @brief Expands to a _Pragma() preprocessor directive, after macro expansion
 *
 * The arguments an arbitrary "token soup", and should not be quoted like a regular
 * _Pragma. This macro will stringify-them itself.
 *
 * Example:
 *
 *      BSONCXX_PRAGMA(GCC diagnostic ignore "-Wconversion")
 *
 * will become:
 *
 *      _Pragma("GCC diagnostic ignore \"-Wconversion\"")
 *
 */
#define BSONCXX_PRAGMA(...) _bsoncxxPragma(__VA_ARGS__)
#ifdef _MSC_VER
// Old MSVC doesn't recognize C++11 _Pragma(), but it always recognized __pragma
#define _bsoncxxPragma(...) __pragma(__VA_ARGS__)
#else
#define _bsoncxxPragma(...) _Pragma(BSONCXX_STRINGIFY(__VA_ARGS__))
#endif

/**
 * @internal
 * @brief Use in a declaration position to force the appearence of a semicolon
 * as the next token. Use this for statement-like or declaration-like macros to
 * enforce that their call sites are followed by a semicolon
 */
#define BSONCXX_FORCE_SEMICOLON static_assert(true, "")

/**
 * @internal
 * @brief Add a trailing noexcept, decltype-return, and return-body to a
 * function definition. (Not compatible with lambda expressions.)
 *
 * Example:
 *
 *      template <typename T>
 *      auto foo(T x, T y) BSONCXX_RETURNS(x + y);
 *
 * Becomes:
 *
 *      template <typename T>
 *      auto foo(T x, T y) noexcept(noexcept(x + y))
 *          -> decltype(x + y)
 *      { return x + y };
 *
 */
#define BSONCXX_RETURNS(...)                                 \
    noexcept(noexcept(__VA_ARGS__))->decltype(__VA_ARGS__) { \
        return __VA_ARGS__;                                  \
    }                                                        \
    BSONCXX_FORCE_SEMICOLON

/**
 * @internal
 * @macro mongocxx_cxx14_constexpr
 * @brief Expands to `constexpr` if compiling as c++14 or greater, otherwise
 * expands to `inline`.
 *
 * Use this on functions that can only be constexpr in C++14 or newer, including
 * non-const member functions.
 */
#if __cplusplus >= 201402L || (defined(_MSVC_LANG) && _MSVC_LANG >= 201402L && _MSC_VER > 1910)
#define bsoncxx_cxx14_constexpr constexpr
#else
#define bsoncxx_cxx14_constexpr inline
#endif

/**
 * @internal
 * @brief Disable a warning for a particular compiler.
 *
 * The argument should be of the form:
 *
 * - Clang(<flag-string-literal>)
 * - GCC(<flag-string-literal>)
 * - GNU(<flag-string-literal>)
 * - MSVC(<id-integer-literal>)
 *
 * The "GNU" form applies to both GCC and Clang
 */
#define BSONCXX_DISABLE_WARNING(Spec) \
    BSONCXX_CONCAT(_bsoncxxDisableWarningImpl_for_, Spec) \
    BSONCXX_FORCE_SEMICOLON

/**
 * @internal
 * @brief Push the current compiler diagnostics settings state
 */
#define BSONCXX_PUSH_WARNINGS() \
    BSONCXX_IF_GNU_LIKE(BSONCXX_PRAGMA(GCC diagnostic push)) \
    BSONCXX_IF_MSVC(BSONCXX_PRAGMA(warning(push))) \
    BSONCXX_FORCE_SEMICOLON

/**
 * @internal
 * @brief Restore prior compiler diagnostics settings from before the most
 * recent BSONCXX_PUSH_WARNINGS()
 */
#define BSONCXX_POP_WARNINGS() \
    BSONCXX_IF_GNU_LIKE(BSONCXX_PRAGMA(GCC diagnostic pop)) \
    BSONCXX_IF_MSVC(BSONCXX_PRAGMA(warning(pop))) \
    BSONCXX_FORCE_SEMICOLON

#define _bsoncxxDisableWarningImpl_for_GCC(...) \
    BSONCXX_IF_GCC(BSONCXX_PRAGMA(GCC diagnostic ignored __VA_ARGS__))

#define _bsoncxxDisableWarningImpl_for_Clang(...) \
    BSONCXX_IF_CLANG(BSONCXX_PRAGMA(GCC diagnostic ignored __VA_ARGS__))

#define _bsoncxxDisableWarningImpl_for_GNU(...) \
    _bsoncxxDisableWarningImpl_for_GCC(__VA_ARGS__) \
    _bsoncxxDisableWarningImpl_for_Clang(__VA_ARGS__)

#define _bsoncxxDisableWarningImpl_for_MSVC(...) \
    BSONCXX_IF_MSVC(BSONCXX_PRAGMA(warning(disable : __VA_ARGS__)))

#define BSONCXX_FWD(...) static_cast<decltype(__VA_ARGS__)&&>(__VA_ARGS__)

