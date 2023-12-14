#ifndef AWS_TESTING_ASYNC_STREAM_TESTER_H
#define AWS_TESTING_ASYNC_STREAM_TESTER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/io/async_stream.h>

#include <aws/common/byte_buf.h>
#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/thread.h>
#include <aws/io/future.h>
#include <aws/testing/stream_tester.h>

#ifndef AWS_UNSTABLE_TESTING_API
#    error This code is designed for use by AWS owned libraries for the AWS C99 SDK. \
You are welcome to use it, but we make no promises on the stability of this API. \
To enable use of this code, set the AWS_UNSTABLE_TESTING_API compiler flag.
#endif

/**
 * Use aws_async_input_stream_tester to test edge cases in systems that take async streams.
 * You can customize its behavior (e.g. fail on 3rd read, always complete async, always complete synchronously, etc)
 */

enum aws_async_read_completion_strategy {
    /* the tester has its own thread, and reads always complete from there */
    AWS_ASYNC_READ_COMPLETES_ON_ANOTHER_THREAD,
    /* reads complete before read() even returns */
    AWS_ASYNC_READ_COMPLETES_IMMEDIATELY,
    /* sometimes reads complete immediately, sometimes they complete on another thread */
    AWS_ASYNC_READ_COMPLETES_ON_RANDOM_THREAD,
};

struct aws_async_input_stream_tester_options {
    /* the async tester uses the synchronous tester under the hood,
     * so here are those options */
    struct aws_input_stream_tester_options base;

    enum aws_async_read_completion_strategy completion_strategy;

    /* if non-zero, a read will take at least this long to complete */
    uint64_t read_duration_ns;
};

struct aws_async_input_stream_tester {
    struct aws_async_input_stream base;
    struct aws_allocator *alloc;
    struct aws_async_input_stream_tester_options options;
    struct aws_input_stream *source_stream;

    struct aws_thread thread;
    struct {
        struct aws_mutex lock;
        struct aws_condition_variable cvar;

        /* when thread should perform a read, these are set */
        struct aws_byte_buf *read_dest;
        struct aws_future_bool *read_future;

        /* if true, thread should shut down */
        bool do_shutdown;
    } synced_data;

    struct aws_atomic_var num_outstanding_reads;
};

static inline void s_async_input_stream_tester_do_actual_read(
    struct aws_async_input_stream_tester *impl,
    struct aws_byte_buf *dest,
    struct aws_future_bool *read_future) {

    int error_code = 0;

    /* delay, if that's how we're configured */
    if (impl->options.read_duration_ns != 0) {
        aws_thread_current_sleep(impl->options.read_duration_ns);
    }

    /* Keep calling read() until we get some data, or hit EOF.
     * We do this because the synchronous aws_input_stream API allows
     * 0 byte reads, but the aws_async_input_stream API does not. */
    size_t prev_len = dest->len;
    struct aws_stream_status status = {.is_end_of_stream = false, .is_valid = true};
    while ((dest->len == prev_len) && !status.is_end_of_stream) {
        /* read from stream */
        if (aws_input_stream_read(impl->source_stream, dest) != AWS_OP_SUCCESS) {
            error_code = aws_last_error();
            goto done;
        }

        /* check if stream is done */
        if (aws_input_stream_get_status(impl->source_stream, &status) != AWS_OP_SUCCESS) {
            error_code = aws_last_error();
            goto done;
        }
    }

done:
    aws_atomic_fetch_sub(&impl->num_outstanding_reads, 1);

    if (error_code != 0) {
        aws_future_bool_set_error(read_future, error_code);
    } else {
        aws_future_bool_set_result(read_future, status.is_end_of_stream);
    }

    aws_future_bool_release(read_future);
}

static inline struct aws_future_bool *s_async_input_stream_tester_read(
    struct aws_async_input_stream *stream,
    struct aws_byte_buf *dest) {

    struct aws_async_input_stream_tester *impl = (struct aws_async_input_stream_tester *)stream->impl;

    size_t prev_outstanding_reads = aws_atomic_fetch_add(&impl->num_outstanding_reads, 1);
    AWS_FATAL_ASSERT(prev_outstanding_reads == 0 && "Overlapping read() calls are forbidden");

    struct aws_future_bool *read_future = aws_future_bool_new(stream->alloc);

    bool do_on_thread = false;
    switch (impl->options.completion_strategy) {
        case AWS_ASYNC_READ_COMPLETES_ON_ANOTHER_THREAD:
            do_on_thread = true;
            break;
        case AWS_ASYNC_READ_COMPLETES_IMMEDIATELY:
            do_on_thread = false;
            break;
        case AWS_ASYNC_READ_COMPLETES_ON_RANDOM_THREAD:
            do_on_thread = (rand() % 2 == 0);
            break;
    }

    if (do_on_thread) {
        /* BEGIN CRITICAL SECTION */
        aws_mutex_lock(&impl->synced_data.lock);
        impl->synced_data.read_dest = dest;
        impl->synced_data.read_future = aws_future_bool_acquire(read_future);
        AWS_FATAL_ASSERT(aws_condition_variable_notify_all(&impl->synced_data.cvar) == AWS_OP_SUCCESS);
        aws_mutex_unlock(&impl->synced_data.lock);
        /* END CRITICAL SECTION */
    } else {
        /* acquire additional refcount on future, since we call release once it's complete */
        aws_future_bool_acquire(read_future);
        s_async_input_stream_tester_do_actual_read(impl, dest, read_future);
    }

    return read_future;
}

static inline void s_async_input_stream_tester_do_actual_destroy(struct aws_async_input_stream_tester *impl) {
    if (impl->options.completion_strategy != AWS_ASYNC_READ_COMPLETES_IMMEDIATELY) {
        aws_condition_variable_clean_up(&impl->synced_data.cvar);
        aws_mutex_clean_up(&impl->synced_data.lock);
    }

    aws_input_stream_release(impl->source_stream);
    aws_mem_release(impl->base.alloc, impl);
}

/* refcount has reached zero */
static inline void s_async_input_stream_tester_destroy(struct aws_async_input_stream *async_stream) {
    struct aws_async_input_stream_tester *impl = (struct aws_async_input_stream_tester *)async_stream->impl;

    if (impl->options.completion_strategy == AWS_ASYNC_READ_COMPLETES_IMMEDIATELY) {
        s_async_input_stream_tester_do_actual_destroy(impl);
    } else {
        /* signal thread to finish cleaning things up */

        /* BEGIN CRITICAL SECTION */
        aws_mutex_lock(&impl->synced_data.lock);
        impl->synced_data.do_shutdown = true;
        AWS_FATAL_ASSERT(aws_condition_variable_notify_all(&impl->synced_data.cvar) == AWS_OP_SUCCESS);
        aws_mutex_unlock(&impl->synced_data.lock);
        /* END CRITICAL SECTION */
    }
}

static inline bool s_async_input_stream_tester_thread_pred(void *arg) {
    struct aws_async_input_stream_tester *impl = (struct aws_async_input_stream_tester *)arg;
    return impl->synced_data.do_shutdown || (impl->synced_data.read_dest != NULL);
}

static inline void s_async_input_stream_tester_thread(void *arg) {
    struct aws_async_input_stream_tester *impl = (struct aws_async_input_stream_tester *)arg;
    bool do_shutdown = false;
    struct aws_byte_buf *read_dest = NULL;
    struct aws_future_bool *read_future = NULL;
    while (!do_shutdown) {
        /* BEGIN CRITICAL SECTION */
        aws_mutex_lock(&impl->synced_data.lock);
        AWS_FATAL_ASSERT(
            aws_condition_variable_wait_pred(
                &impl->synced_data.cvar, &impl->synced_data.lock, s_async_input_stream_tester_thread_pred, impl) ==
            AWS_OP_SUCCESS);

        /* acquire work */
        do_shutdown = impl->synced_data.do_shutdown;
        read_dest = impl->synced_data.read_dest;
        impl->synced_data.read_dest = NULL;
        read_future = impl->synced_data.read_future;
        impl->synced_data.read_future = NULL;

        aws_mutex_unlock(&impl->synced_data.lock);
        /* END CRITICAL SECTION */

        if (read_dest != NULL) {
            s_async_input_stream_tester_do_actual_read(impl, read_dest, read_future);
        }
    }

    /* thread has shut down, finish destruction */
    s_async_input_stream_tester_do_actual_destroy(impl);
}

static inline uint64_t aws_async_input_stream_tester_total_bytes_read(
    const struct aws_async_input_stream *async_stream) {

    const struct aws_async_input_stream_tester *async_impl =
        (const struct aws_async_input_stream_tester *)async_stream->impl;
    const struct aws_input_stream_tester *synchronous_impl =
        (const struct aws_input_stream_tester *)async_impl->source_stream->impl;
    return synchronous_impl->total_bytes_read;
}

static struct aws_async_input_stream_vtable s_async_input_stream_tester_vtable = {
    .destroy = s_async_input_stream_tester_destroy,
    .read = s_async_input_stream_tester_read,
};

static inline struct aws_async_input_stream *aws_async_input_stream_new_tester(
    struct aws_allocator *alloc,
    const struct aws_async_input_stream_tester_options *options) {

    struct aws_async_input_stream_tester *impl =
        (struct aws_async_input_stream_tester *)aws_mem_calloc(alloc, 1, sizeof(struct aws_async_input_stream_tester));
    aws_async_input_stream_init_base(&impl->base, alloc, &s_async_input_stream_tester_vtable, impl);
    impl->options = *options;
    aws_atomic_init_int(&impl->num_outstanding_reads, 0);

    impl->source_stream = aws_input_stream_new_tester(alloc, &options->base);
    AWS_FATAL_ASSERT(impl->source_stream);

    if (options->completion_strategy != AWS_ASYNC_READ_COMPLETES_IMMEDIATELY) {
        aws_mutex_init(&impl->synced_data.lock);
        aws_condition_variable_init(&impl->synced_data.cvar);

        AWS_FATAL_ASSERT(aws_thread_init(&impl->thread, alloc) == AWS_OP_SUCCESS);
        struct aws_thread_options thread_options = *aws_default_thread_options();
        thread_options.name = aws_byte_cursor_from_c_str("AsyncStream");
        thread_options.join_strategy = AWS_TJS_MANAGED;

        AWS_FATAL_ASSERT(
            aws_thread_launch(&impl->thread, s_async_input_stream_tester_thread, impl, &thread_options) ==
            AWS_OP_SUCCESS);
    }

    return &impl->base;
}

#endif /* AWS_TESTING_ASYNC_STREAM_TESTER_H */
