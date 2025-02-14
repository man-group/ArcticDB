#ifndef AWS_TESTING_AWS_TEST_HARNESS_H
#define AWS_TESTING_AWS_TEST_HARNESS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/common.h>
#include <aws/common/error.h>
#include <aws/common/logging.h>
#include <aws/common/mutex.h>
#include <aws/common/system_info.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * The return code for skipped tests. Use the return code if the test should be skipped.
 */
#define AWS_OP_SKIP (-2)

#ifndef AWS_UNSTABLE_TESTING_API
#    error The AWS Test Fixture is designed only for use by AWS owned libraries for the AWS C99 SDK. You are welcome to use it,   \
but you should be aware we make no promises on the stability of this API.  To enable use of the aws test fixtures, set \
the AWS_UNSTABLE_TESTING_API compiler flag
#endif

#ifndef AWS_TESTING_REPORT_FD
#    define AWS_TESTING_REPORT_FD stderr
#endif

#ifdef _MSC_VER
#    pragma warning(disable : 4221) /* aggregate initializer using local variable addresses */
#    pragma warning(disable : 4204) /* non-constant aggregate initializer */
#endif

#if defined(__clang__)
#    pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#endif

/** Prints a message to AWS_TESTING_REPORT_FD using printf format that appends the function, file and line number.
 * If format is null, returns 0 without printing anything; otherwise returns 1.
 * If function or file are null, the function, file and line number are not appended.
 */
static int s_cunit_failure_message0(
    const char *prefix,
    const char *function,
    const char *file,
    int line,
    const char *format,
    ...) {
    if (!format) {
        return 0;
    }

    fprintf(AWS_TESTING_REPORT_FD, "%s", prefix);

    va_list ap;
    va_start(ap, format);
    vfprintf(AWS_TESTING_REPORT_FD, format, ap);
    va_end(ap);

    if (function && file) {
        fprintf(AWS_TESTING_REPORT_FD, " [%s(): %s:%d]\n", function, file, line);
    } else {
        fprintf(AWS_TESTING_REPORT_FD, "\n");
    }

    return 1;
}

#define FAIL_PREFIX "***FAILURE*** "
#define CUNIT_FAILURE_MESSAGE(func, file, line, format, ...)                                                           \
    s_cunit_failure_message0(FAIL_PREFIX, func, file, line, format, #__VA_ARGS__)

#define SUCCESS (0)
#define FAILURE (-1)
/* The exit code returned to ctest to indicate the test is skipped. Refer to cmake doc:
 * https://cmake.org/cmake/help/latest/prop_test/SKIP_RETURN_CODE.html
 * The value has no special meaning, it's just an arbitrary exit code reducing the chance of clashing with exit codes
 * that may be returned from various tools (e.g. sanitizer). */
#define SKIP (103)

#define RETURN_SUCCESS(format, ...)                                                                                    \
    do {                                                                                                               \
        printf(format, ##__VA_ARGS__);                                                                                 \
        printf("\n");                                                                                                  \
        return SUCCESS;                                                                                                \
    } while (0)

#define PRINT_FAIL_INTERNAL(...) CUNIT_FAILURE_MESSAGE(__func__, __FILE__, __LINE__, ##__VA_ARGS__, (const char *)NULL)

#define PRINT_FAIL_INTERNAL0(...)                                                                                      \
    s_cunit_failure_message0(FAIL_PREFIX, __func__, __FILE__, __LINE__, ##__VA_ARGS__, (const char *)NULL)

#define PRINT_FAIL_WITHOUT_LOCATION(...)                                                                               \
    s_cunit_failure_message0(FAIL_PREFIX, NULL, NULL, __LINE__, ##__VA_ARGS__, (const char *)NULL)

#define POSTFAIL_INTERNAL()                                                                                            \
    do {                                                                                                               \
        return FAILURE;                                                                                                \
    } while (0)

#define FAIL(...)                                                                                                      \
    do {                                                                                                               \
        PRINT_FAIL_INTERNAL0(__VA_ARGS__);                                                                             \
        POSTFAIL_INTERNAL();                                                                                           \
    } while (0)

#define ASSERT_TRUE(condition, ...)                                                                                    \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("Expected condition to be true: " #condition);                                    \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_FALSE(condition, ...)                                                                                   \
    do {                                                                                                               \
        if ((condition)) {                                                                                             \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("Expected condition to be false: " #condition);                                   \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_SUCCESS(condition, ...)                                                                                 \
    do {                                                                                                               \
        int assert_rv = (condition);                                                                                   \
        if (assert_rv != AWS_OP_SUCCESS) {                                                                             \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "Expected success at %s; got return value %d with last error %d\n",                                \
                    #condition,                                                                                        \
                    assert_rv,                                                                                         \
                    aws_last_error());                                                                                 \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_FAILS(condition, ...)                                                                                   \
    do {                                                                                                               \
        int assert_rv = (condition);                                                                                   \
        if (assert_rv != AWS_OP_ERR) {                                                                                 \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "Expected failure at %s; got return value %d with last error %d\n",                                \
                    #condition,                                                                                        \
                    assert_rv,                                                                                         \
                    aws_last_error());                                                                                 \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_ERROR(error, condition, ...)                                                                            \
    do {                                                                                                               \
        int assert_rv = (condition);                                                                                   \
        int assert_err = aws_last_error();                                                                             \
        int assert_err_expect = (error);                                                                               \
        if (assert_rv != AWS_OP_ERR) {                                                                                 \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%sExpected error but no error occurred; rv=%d, aws_last_error=%d (expected %d): ",                    \
                FAIL_PREFIX,                                                                                           \
                assert_rv,                                                                                             \
                assert_err,                                                                                            \
                assert_err_expect);                                                                                    \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #condition);                                                                \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
        if (assert_err != assert_err_expect) {                                                                         \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%sIncorrect error code; aws_last_error=%d (expected %d): ",                                           \
                FAIL_PREFIX,                                                                                           \
                assert_err,                                                                                            \
                assert_err_expect);                                                                                    \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #condition);                                                                \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_NULL(ptr, ...)                                                                                          \
    do {                                                                                                               \
        /* XXX: Some tests use ASSERT_NULL on ints... */                                                               \
        void *assert_p = (void *)(uintptr_t)(ptr);                                                                     \
        if (assert_p) {                                                                                                \
            fprintf(AWS_TESTING_REPORT_FD, "%sExpected null but got %p: ", FAIL_PREFIX, assert_p);                     \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #ptr);                                                                      \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_NOT_NULL(ptr, ...)                                                                                      \
    do {                                                                                                               \
        /* XXX: Some tests use ASSERT_NULL on ints... */                                                               \
        void *assert_p = (void *)(uintptr_t)(ptr);                                                                     \
        if (!assert_p) {                                                                                               \
            fprintf(AWS_TESTING_REPORT_FD, "%sExpected non-null but got null: ", FAIL_PREFIX);                         \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s", #ptr);                                                                      \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_TYP_EQUALS(type, formatarg, expected, got, ...)                                                         \
    do {                                                                                                               \
        type assert_expected = (expected);                                                                             \
        type assert_actual = (got);                                                                                    \
        if (assert_expected != assert_actual) {                                                                        \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%s" formatarg " != " formatarg ": ",                                                                  \
                FAIL_PREFIX,                                                                                           \
                assert_expected,                                                                                       \
                assert_actual);                                                                                        \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s != %s", #expected, #got);                                                     \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#ifdef _MSC_VER
#    define ASSERT_INT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(intmax_t, "%lld", expected, got, __VA_ARGS__)
#    define ASSERT_UINT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(uintmax_t, "%llu", expected, got, __VA_ARGS__)
#else
/* For comparing any signed integer types */
#    define ASSERT_INT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(intmax_t, "%jd", expected, got, __VA_ARGS__)
/* For comparing any unsigned integer types */
#    define ASSERT_UINT_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(uintmax_t, "%ju", expected, got, __VA_ARGS__)
#endif

#define ASSERT_PTR_EQUALS(expected, got, ...)                                                                          \
    do {                                                                                                               \
        void *assert_expected = (void *)(uintptr_t)(expected);                                                         \
        void *assert_actual = (void *)(uintptr_t)(got);                                                                \
        if (assert_expected != assert_actual) {                                                                        \
            fprintf(AWS_TESTING_REPORT_FD, "%s%p != %p: ", FAIL_PREFIX, assert_expected, assert_actual);               \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("%s != %s", #expected, #got);                                                     \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

/* note that uint8_t is promoted to unsigned int in varargs, so %02x is an acceptable format string */
#define ASSERT_BYTE_HEX_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(uint8_t, "%02X", expected, got, __VA_ARGS__)
#define ASSERT_HEX_EQUALS(expected, got, ...) ASSERT_TYP_EQUALS(unsigned long long, "%llX", expected, got, __VA_ARGS__)

#define ASSERT_STR_EQUALS(expected, got, ...)                                                                          \
    do {                                                                                                               \
        const char *assert_expected = (expected);                                                                      \
        const char *assert_got = (got);                                                                                \
        ASSERT_NOT_NULL(assert_expected);                                                                              \
        ASSERT_NOT_NULL(assert_got);                                                                                   \
        if (strcmp(assert_expected, assert_got) != 0) {                                                                \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD, "%sExpected: \"%s\"; got: \"%s\": ", FAIL_PREFIX, assert_expected, assert_got); \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("ASSERT_STR_EQUALS(%s, %s)", #expected, #got);                                    \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_BIN_ARRAYS_EQUALS(expected, expected_size, got, got_size, ...)                                          \
    do {                                                                                                               \
        const uint8_t *assert_ex_p = (const uint8_t *)(expected);                                                      \
        size_t assert_ex_s = (expected_size);                                                                          \
        const uint8_t *assert_got_p = (const uint8_t *)(got);                                                          \
        size_t assert_got_s = (got_size);                                                                              \
        if (assert_ex_s == 0 && assert_got_s == 0) {                                                                   \
            break;                                                                                                     \
        }                                                                                                              \
        if (assert_ex_s != assert_got_s) {                                                                             \
            fprintf(AWS_TESTING_REPORT_FD, "%sSize mismatch: %zu != %zu: ", FAIL_PREFIX, assert_ex_s, assert_got_s);   \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "ASSERT_BIN_ARRAYS_EQUALS(%s, %s, %s, %s)", #expected, #expected_size, #got, #got_size);           \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
        if (memcmp(assert_ex_p, assert_got_p, assert_got_s) != 0) {                                                    \
            if (assert_got_s <= 1024) {                                                                                \
                for (size_t assert_i = 0; assert_i < assert_ex_s; ++assert_i) {                                        \
                    if (assert_ex_p[assert_i] != assert_got_p[assert_i]) {                                             \
                        fprintf(                                                                                       \
                            AWS_TESTING_REPORT_FD,                                                                     \
                            "%sMismatch at byte[%zu]: 0x%02X != 0x%02X: ",                                             \
                            FAIL_PREFIX,                                                                               \
                            assert_i,                                                                                  \
                            assert_ex_p[assert_i],                                                                     \
                            assert_got_p[assert_i]);                                                                   \
                        break;                                                                                         \
                    }                                                                                                  \
                }                                                                                                      \
            } else {                                                                                                   \
                fprintf(AWS_TESTING_REPORT_FD, "%sData mismatch: ", FAIL_PREFIX);                                      \
            }                                                                                                          \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0(                                                                                  \
                    "ASSERT_BIN_ARRAYS_EQUALS(%s, %s, %s, %s)", #expected, #expected_size, #got, #got_size);           \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_CURSOR_VALUE_CSTRING_EQUALS(cursor, cstring, ...)                                                       \
    do {                                                                                                               \
        const uint8_t *assert_ex_p = (const uint8_t *)((cursor).ptr);                                                  \
        size_t assert_ex_s = (cursor).len;                                                                             \
        const uint8_t *assert_got_p = (const uint8_t *)cstring;                                                        \
        size_t assert_got_s = strlen(cstring);                                                                         \
        if (assert_ex_s == 0 && assert_got_s == 0) {                                                                   \
            break;                                                                                                     \
        }                                                                                                              \
        if (assert_ex_s != assert_got_s) {                                                                             \
            fprintf(AWS_TESTING_REPORT_FD, "%sSize mismatch: %zu != %zu: \n", FAIL_PREFIX, assert_ex_s, assert_got_s); \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%sGot: \"" PRInSTR "\"; Expected: \"%s\" \n",                                                         \
                FAIL_PREFIX,                                                                                           \
                AWS_BYTE_CURSOR_PRI(cursor),                                                                           \
                cstring);                                                                                              \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("ASSERT_CURSOR_VALUE_STRING_EQUALS(%s, %s)", #cursor, #cstring);                  \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
        if (memcmp(assert_ex_p, assert_got_p, assert_got_s) != 0) {                                                    \
            fprintf(                                                                                                   \
                AWS_TESTING_REPORT_FD,                                                                                 \
                "%sData mismatch; Got: \"" PRInSTR "\"; Expected: \"%s\" \n",                                          \
                FAIL_PREFIX,                                                                                           \
                AWS_BYTE_CURSOR_PRI(cursor),                                                                           \
                cstring);                                                                                              \
            if (!PRINT_FAIL_INTERNAL0(__VA_ARGS__)) {                                                                  \
                PRINT_FAIL_INTERNAL0("ASSERT_CURSOR_VALUE_STRING_EQUALS(%s, %s)", #cursor, #cstring);                  \
            }                                                                                                          \
            POSTFAIL_INTERNAL();                                                                                       \
        }                                                                                                              \
    } while (0)

#define ASSERT_CURSOR_VALUE_STRING_EQUALS(cursor, string, ...)                                                         \
    ASSERT_CURSOR_VALUE_CSTRING_EQUALS(cursor, aws_string_c_str(string));

typedef int(aws_test_before_fn)(struct aws_allocator *allocator, void *ctx);
typedef int(aws_test_run_fn)(struct aws_allocator *allocator, void *ctx);
typedef int(aws_test_after_fn)(struct aws_allocator *allocator, int setup_result, void *ctx);

struct aws_test_harness {
    aws_test_before_fn *on_before;
    aws_test_run_fn *run;
    aws_test_after_fn *on_after;
    void *ctx;
    const char *test_name;
    int suppress_memcheck;
};

#if defined(_WIN32)
#    include <windows.h>
static LONG WINAPI s_test_print_stack_trace(struct _EXCEPTION_POINTERS *exception_pointers) {
#    if !defined(AWS_HEADER_CHECKER)
    aws_backtrace_print(stderr, exception_pointers);
#    endif
    return EXCEPTION_EXECUTE_HANDLER;
}
#elif defined(AWS_HAVE_EXECINFO)
#    include <signal.h>
static void s_print_stack_trace(int sig, siginfo_t *sig_info, void *user_data) {
    (void)sig;
    (void)sig_info;
    (void)user_data;
#    if !defined(AWS_HEADER_CHECKER)
    aws_backtrace_print(stderr, sig_info);
#    endif
    exit(-1);
}
#endif

static inline int s_aws_run_test_case(struct aws_test_harness *harness) {
    AWS_ASSERT(harness->run);
/*
 * MSVC compiler has a weird interactive pop-up in debug whenever 'abort()' is called, which can be triggered
 * by hitting any aws_assert or aws_pre_condition, causing the CI to hang. So disable the pop-up in tests.
 */
#ifdef _MSC_VER
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif

#if defined(_WIN32)
    SetUnhandledExceptionFilter(s_test_print_stack_trace);
    /* Set working directory to path to this exe */
    char cwd[512];
    DWORD len = GetModuleFileNameA(NULL, cwd, sizeof(cwd));
    DWORD idx = len - 1;
    while (idx && cwd[idx] != '\\') {
        idx--;
    }
    cwd[idx] = 0;
    SetCurrentDirectory(cwd);
#elif defined(AWS_HAVE_EXECINFO)
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sigemptyset(&sa.sa_mask);

    sa.sa_flags = SA_NODEFER;
    sa.sa_sigaction = s_print_stack_trace;

    sigaction(SIGSEGV, &sa, NULL);
#endif

    /* track allocations and report leaks in tests, unless suppressed */
    struct aws_allocator *allocator = NULL;
    if (harness->suppress_memcheck) {
        allocator = aws_default_allocator();
    } else {
        allocator = aws_mem_tracer_new(aws_default_allocator(), NULL, AWS_MEMTRACE_STACKS, 8);
    }

    /* wire up a logger to stderr by default, may be replaced by some tests */
    struct aws_logger err_logger;
    struct aws_logger_standard_options options;
    options.file = AWS_TESTING_REPORT_FD;
    options.level = AWS_LL_TRACE;
    options.filename = NULL;
    aws_logger_init_standard(&err_logger, aws_default_allocator(), &options);
    aws_logger_set(&err_logger);

    int test_res = AWS_OP_ERR;
    int setup_res = AWS_OP_SUCCESS;
    if (harness->on_before) {
        setup_res = harness->on_before(allocator, harness->ctx);
    }

    if (!setup_res) {
        test_res = harness->run(allocator, harness->ctx);
    }

    if (harness->on_after) {
        test_res |= harness->on_after(allocator, setup_res, harness->ctx);
    }

    if (test_res != AWS_OP_SUCCESS) {
        goto fail;
    }

    if (!harness->suppress_memcheck) {
        /* Reset the logger, as test can set their own logger and clean it up,
         * but aws_mem_tracer_dump() needs a valid logger to be active */
        aws_logger_set(&err_logger);

        const size_t leaked_allocations = aws_mem_tracer_count(allocator);
        const size_t leaked_bytes = aws_mem_tracer_bytes(allocator);
        if (leaked_bytes) {
            aws_mem_tracer_dump(allocator);
            PRINT_FAIL_WITHOUT_LOCATION(
                "Test leaked memory: %zu bytes %zu allocations", leaked_bytes, leaked_allocations);
            goto fail;
        }

        aws_mem_tracer_destroy(allocator);
    }

    aws_logger_set(NULL);
    aws_logger_clean_up(&err_logger);

    RETURN_SUCCESS("%s [ \033[32mOK\033[0m ]", harness->test_name);

fail:
    if (test_res == AWS_OP_SKIP) {
        fprintf(AWS_TESTING_REPORT_FD, "%s [ \033[32mSKIP\033[0m ]\n", harness->test_name);
    } else {
        PRINT_FAIL_WITHOUT_LOCATION("%s [ \033[31mFAILED\033[0m ]", harness->test_name);
    }
    /* Use _Exit() to terminate without cleaning up resources.
     * This prevents LeakSanitizer spam (yes, we know failing tests don't bother cleaning up).
     * It also prevents errors where threads that haven't cleaned are still using the logger declared in this fn. */
    fflush(AWS_TESTING_REPORT_FD);
    fflush(stdout);
    fflush(stderr);
    _Exit(test_res == AWS_OP_SKIP ? SKIP : FAILURE);
}

/* Enables terminal escape sequences for text coloring on Windows. */
/* https://docs.microsoft.com/en-us/windows/console/console-virtual-terminal-sequences */
#ifdef _WIN32

#    include <windows.h>

#    ifndef ENABLE_VIRTUAL_TERMINAL_PROCESSING
#        define ENABLE_VIRTUAL_TERMINAL_PROCESSING 0x0004
#    endif

static inline int enable_vt_mode(void) {
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    if (hOut == INVALID_HANDLE_VALUE) {
        return AWS_OP_ERR;
    }

    DWORD dwMode = 0;
    if (!GetConsoleMode(hOut, &dwMode)) {
        return AWS_OP_ERR;
    }

    dwMode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
    if (!SetConsoleMode(hOut, dwMode)) {
        return AWS_OP_ERR;
    }
    return AWS_OP_SUCCESS;
}

#else

static inline int enable_vt_mode(void) {
    return AWS_OP_ERR;
}

#endif

#define AWS_TEST_CASE_SUPRESSION(name, fn, s)                                                                          \
    static int fn(struct aws_allocator *allocator, void *ctx);                                                         \
    static struct aws_test_harness name##_test = {                                                                     \
        NULL,                                                                                                          \
        fn,                                                                                                            \
        NULL,                                                                                                          \
        NULL,                                                                                                          \
        #name,                                                                                                         \
        s,                                                                                                             \
    };                                                                                                                 \
    int name(int argc, char *argv[]) {                                                                                 \
        (void)argc, (void)argv;                                                                                        \
        return s_aws_run_test_case(&name##_test);                                                                      \
    }

#define AWS_TEST_CASE_FIXTURE_SUPPRESSION(name, b, fn, af, c, s)                                                       \
    static int b(struct aws_allocator *allocator, void *ctx);                                                          \
    static int fn(struct aws_allocator *allocator, void *ctx);                                                         \
    static int af(struct aws_allocator *allocator, int setup_result, void *ctx);                                       \
    static struct aws_test_harness name##_test = {                                                                     \
        b,                                                                                                             \
        fn,                                                                                                            \
        af,                                                                                                            \
        c,                                                                                                             \
        #name,                                                                                                         \
        s,                                                                                                             \
    };                                                                                                                 \
    int name(int argc, char *argv[]) {                                                                                 \
        (void)argc;                                                                                                    \
        (void)argv;                                                                                                    \
        return s_aws_run_test_case(&name##_test);                                                                      \
    }

#define AWS_TEST_CASE(name, fn) AWS_TEST_CASE_SUPRESSION(name, fn, 0)
#define AWS_TEST_CASE_FIXTURE(name, b, fn, af, c) AWS_TEST_CASE_FIXTURE_SUPPRESSION(name, b, fn, af, c, 0)

#endif /* AWS_TESTING_AWS_TEST_HARNESS_H */
