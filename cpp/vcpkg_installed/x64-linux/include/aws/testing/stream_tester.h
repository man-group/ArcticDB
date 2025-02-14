#ifndef AWS_TESTING_STREAM_TESTER_H
#define AWS_TESTING_STREAM_TESTER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/io/stream.h>

#ifndef AWS_UNSTABLE_TESTING_API
#    error This code is designed for use by AWS owned libraries for the AWS C99 SDK. \
You are welcome to use it, but we make no promises on the stability of this API. \
To enable use of this code, set the AWS_UNSTABLE_TESTING_API compiler flag.
#endif

/**
 * Use aws_input_stream tester to test edge cases in systems that take input streams.
 * You can make it behave in specific weird ways (e.g. fail on 3rd read).
 *
 * There are a few ways to set what gets streamed.
 * - source_bytes: if set, stream these bytes.
 * - source_stream: if set, wrap this stream (but insert weird behavior like failing on 3rd read).
 * - autogen_length: autogen streaming content N bytes in length.
 */

enum aws_autogen_style {
    AWS_AUTOGEN_LOREM_IPSUM,
    AWS_AUTOGEN_ALPHABET,
    AWS_AUTOGEN_NUMBERS,
};

struct aws_input_stream_tester_options {
    /* bytes to be streamed.
     * the stream copies these to its own internal buffer.
     * or you can set the autogen_length  */
    struct aws_byte_cursor source_bytes;

    /* wrap another stream */
    struct aws_input_stream *source_stream;

    /* if non-zero, autogen streaming content N bytes in length */
    size_t autogen_length;

    /* style of contents (if using autogen) */
    enum aws_autogen_style autogen_style;

    /* if non-zero, read at most N bytes per read() */
    size_t max_bytes_per_read;

    /* if non-zero, read 0 bytes the Nth time read() is called */
    size_t read_zero_bytes_on_nth_read;

    /* If false, EOF is reported by the read() which produces the last few bytes.
     * If true, EOF isn't reported until there's one more read(), producing zero bytes.
     * This emulates an underlying stream that reports EOF by reading 0 bytes */
    bool eof_requires_extra_read;

    /* if non-zero, fail the Nth time read() is called, raising `fail_with_error_code` */
    size_t fail_on_nth_read;

    /* error-code to raise if failing on purpose */
    int fail_with_error_code;
};

struct aws_input_stream_tester {
    struct aws_input_stream base;
    struct aws_allocator *alloc;
    struct aws_input_stream_tester_options options;
    struct aws_byte_buf source_buf;
    struct aws_input_stream *source_stream;
    size_t read_count;
    bool num_bytes_last_read; /* number of bytes read in the most recent successful read() */
    uint64_t total_bytes_read;
};

static inline int s_input_stream_tester_seek(
    struct aws_input_stream *stream,
    int64_t offset,
    enum aws_stream_seek_basis basis) {

    struct aws_input_stream_tester *impl = (struct aws_input_stream_tester *)stream->impl;
    return aws_input_stream_seek(impl->source_stream, offset, basis);
}

static inline int s_input_stream_tester_read(struct aws_input_stream *stream, struct aws_byte_buf *original_dest) {
    struct aws_input_stream_tester *impl = (struct aws_input_stream_tester *)stream->impl;

    impl->read_count++;

    /* if we're configured to fail, then do it */
    if (impl->read_count == impl->options.fail_on_nth_read) {
        AWS_FATAL_ASSERT(impl->options.fail_with_error_code != 0);
        return aws_raise_error(impl->options.fail_with_error_code);
    }

    /* cap how much is read, if that's how we're configured */
    size_t bytes_to_read = original_dest->capacity - original_dest->len;
    if (impl->options.max_bytes_per_read != 0) {
        bytes_to_read = aws_min_size(bytes_to_read, impl->options.max_bytes_per_read);
    }

    if (impl->read_count == impl->options.read_zero_bytes_on_nth_read) {
        bytes_to_read = 0;
    }

    /* pass artificially capped buffer to actual stream */
    struct aws_byte_buf capped_buf =
        aws_byte_buf_from_empty_array(original_dest->buffer + original_dest->len, bytes_to_read);

    if (aws_input_stream_read(impl->source_stream, &capped_buf)) {
        return AWS_OP_ERR;
    }

    size_t bytes_actually_read = capped_buf.len;
    original_dest->len += bytes_actually_read;
    impl->num_bytes_last_read = bytes_actually_read;
    impl->total_bytes_read += bytes_actually_read;

    return AWS_OP_SUCCESS;
}

static inline int s_input_stream_tester_get_status(struct aws_input_stream *stream, struct aws_stream_status *status) {
    struct aws_input_stream_tester *impl = (struct aws_input_stream_tester *)stream->impl;
    if (aws_input_stream_get_status(impl->source_stream, status)) {
        return AWS_OP_ERR;
    }

    /* if we're emulating a stream that requires an additional 0 byte read to realize it's EOF */
    if (impl->options.eof_requires_extra_read) {
        if (impl->num_bytes_last_read > 0) {
            status->is_end_of_stream = false;
        }
    }

    return AWS_OP_SUCCESS;
}

static inline int s_input_stream_tester_get_length(struct aws_input_stream *stream, int64_t *out_length) {
    struct aws_input_stream_tester *impl = (struct aws_input_stream_tester *)stream->impl;
    return aws_input_stream_get_length(impl->source_stream, out_length);
}

static struct aws_input_stream_vtable s_input_stream_tester_vtable = {
    .seek = s_input_stream_tester_seek,
    .read = s_input_stream_tester_read,
    .get_status = s_input_stream_tester_get_status,
    .get_length = s_input_stream_tester_get_length,
};

/* init byte-buf and fill it autogenned content */
static inline void s_byte_buf_init_autogenned(
    struct aws_byte_buf *buf,
    struct aws_allocator *alloc,
    size_t length,
    enum aws_autogen_style style) {

    aws_byte_buf_init(buf, alloc, length);
    struct aws_byte_cursor pattern = {0};
    switch (style) {
        case AWS_AUTOGEN_LOREM_IPSUM:
            pattern = aws_byte_cursor_from_c_str(
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore "
                "et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut "
                "aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse "
                "cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa "
                "qui officia deserunt mollit anim id est laborum. ");
            break;
        case AWS_AUTOGEN_ALPHABET:
            pattern = aws_byte_cursor_from_c_str("abcdefghijklmnopqrstuvwxyz");
            break;
        case AWS_AUTOGEN_NUMBERS:
            pattern = aws_byte_cursor_from_c_str("1234567890");
            break;
    }

    struct aws_byte_cursor pattern_cursor = {0};
    while (buf->len < buf->capacity) {
        if (pattern_cursor.len == 0) {
            pattern_cursor = pattern;
        }
        aws_byte_buf_write_to_capacity(buf, &pattern_cursor);
    }
}

static inline uint64_t aws_input_stream_tester_total_bytes_read(const struct aws_input_stream *stream) {
    const struct aws_input_stream_tester *impl = (const struct aws_input_stream_tester *)stream->impl;
    return impl->total_bytes_read;
}

static inline void s_input_stream_tester_destroy(void *user_data) {
    struct aws_input_stream_tester *impl = (struct aws_input_stream_tester *)user_data;
    aws_input_stream_release(impl->source_stream);
    aws_byte_buf_clean_up(&impl->source_buf);
    aws_mem_release(impl->alloc, impl);
}

static inline struct aws_input_stream *aws_input_stream_new_tester(
    struct aws_allocator *alloc,
    const struct aws_input_stream_tester_options *options) {

    struct aws_input_stream_tester *impl =
        (struct aws_input_stream_tester *)aws_mem_calloc(alloc, 1, sizeof(struct aws_input_stream_tester));
    impl->base.impl = impl;
    impl->base.vtable = &s_input_stream_tester_vtable;
    aws_ref_count_init(&impl->base.ref_count, impl, s_input_stream_tester_destroy);
    impl->alloc = alloc;
    impl->options = *options;

    if (options->source_stream != NULL) {
        AWS_FATAL_ASSERT((options->autogen_length == 0) && (options->source_bytes.len == 0));
        impl->source_stream = aws_input_stream_acquire(options->source_stream);
    } else {
        if (options->autogen_length > 0) {
            AWS_FATAL_ASSERT(options->source_bytes.len == 0);
            s_byte_buf_init_autogenned(&impl->source_buf, alloc, options->autogen_length, options->autogen_style);
        } else {
            aws_byte_buf_init_copy_from_cursor(&impl->source_buf, alloc, options->source_bytes);
        }
        struct aws_byte_cursor source_buf_cursor = aws_byte_cursor_from_buf(&impl->source_buf);
        impl->source_stream = aws_input_stream_new_from_cursor(alloc, &source_buf_cursor);
        AWS_FATAL_ASSERT(impl->source_stream);
    }

    return &impl->base;
}

#endif /* AWS_TESTING_STREAM_TESTER_H */
