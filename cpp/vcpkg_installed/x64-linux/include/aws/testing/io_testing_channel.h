#ifndef AWS_TESTING_IO_TESTING_CHANNEL_H
#define AWS_TESTING_IO_TESTING_CHANNEL_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/common/clock.h>
#include <aws/common/task_scheduler.h>
#include <aws/io/channel.h>
#include <aws/io/event_loop.h>
#include <aws/io/logging.h>
#include <aws/io/statistics.h>
#include <aws/testing/aws_test_harness.h>

struct testing_loop {
    struct aws_task_scheduler scheduler;
    bool mock_on_callers_thread;
};

static int s_testing_loop_run(struct aws_event_loop *event_loop) {
    (void)event_loop;
    return AWS_OP_SUCCESS;
}

static int s_testing_loop_stop(struct aws_event_loop *event_loop) {
    (void)event_loop;
    return AWS_OP_SUCCESS;
}

static int s_testing_loop_wait_for_stop_completion(struct aws_event_loop *event_loop) {
    (void)event_loop;
    return AWS_OP_SUCCESS;
}

static void s_testing_loop_schedule_task_now(struct aws_event_loop *event_loop, struct aws_task *task) {
    struct testing_loop *testing_loop = event_loop->impl_data;
    aws_task_scheduler_schedule_now(&testing_loop->scheduler, task);
}

static void s_testing_loop_schedule_task_future(
    struct aws_event_loop *event_loop,
    struct aws_task *task,
    uint64_t run_at_nanos) {

    struct testing_loop *testing_loop = event_loop->impl_data;
    aws_task_scheduler_schedule_future(&testing_loop->scheduler, task, run_at_nanos);
}

static void s_testing_loop_cancel_task(struct aws_event_loop *event_loop, struct aws_task *task) {
    struct testing_loop *testing_loop = event_loop->impl_data;
    aws_task_scheduler_cancel_task(&testing_loop->scheduler, task);
}

static bool s_testing_loop_is_on_callers_thread(struct aws_event_loop *event_loop) {
    struct testing_loop *testing_loop = event_loop->impl_data;
    return testing_loop->mock_on_callers_thread;
}

static void s_testing_loop_destroy(struct aws_event_loop *event_loop) {
    struct testing_loop *testing_loop = event_loop->impl_data;
    aws_task_scheduler_clean_up(&testing_loop->scheduler);
    aws_mem_release(event_loop->alloc, testing_loop);
    aws_event_loop_clean_up_base(event_loop);
    aws_mem_release(event_loop->alloc, event_loop);
}

static struct aws_event_loop_vtable s_testing_loop_vtable = {
    .destroy = s_testing_loop_destroy,
    .is_on_callers_thread = s_testing_loop_is_on_callers_thread,
    .run = s_testing_loop_run,
    .schedule_task_now = s_testing_loop_schedule_task_now,
    .schedule_task_future = s_testing_loop_schedule_task_future,
    .cancel_task = s_testing_loop_cancel_task,
    .stop = s_testing_loop_stop,
    .wait_for_stop_completion = s_testing_loop_wait_for_stop_completion,
};

static struct aws_event_loop *s_testing_loop_new(struct aws_allocator *allocator, aws_io_clock_fn clock) {
    struct aws_event_loop *event_loop = aws_mem_acquire(allocator, sizeof(struct aws_event_loop));
    aws_event_loop_init_base(event_loop, allocator, clock);

    struct testing_loop *testing_loop = aws_mem_calloc(allocator, 1, sizeof(struct testing_loop));
    aws_task_scheduler_init(&testing_loop->scheduler, allocator);
    testing_loop->mock_on_callers_thread = true;
    event_loop->impl_data = testing_loop;
    event_loop->vtable = &s_testing_loop_vtable;

    return event_loop;
}

typedef void(testing_channel_handler_on_shutdown_fn)(
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately,
    void *user_data);

struct testing_channel_handler {
    struct aws_linked_list messages;
    size_t latest_window_update;
    size_t initial_window;
    bool complete_write_immediately;
    int complete_write_error_code;
    testing_channel_handler_on_shutdown_fn *on_shutdown;
    void *on_shutdown_user_data;
    struct aws_crt_statistics_socket stats;
};

static int s_testing_channel_handler_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {
    (void)handler;
    (void)slot;
    (void)message;

    struct testing_channel_handler *testing_handler = handler->impl;
    aws_linked_list_push_back(&testing_handler->messages, &message->queueing_handle);
    return AWS_OP_SUCCESS;
}

static int s_testing_channel_handler_process_write_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {
    (void)slot;

    struct testing_channel_handler *testing_handler = handler->impl;
    aws_linked_list_push_back(&testing_handler->messages, &message->queueing_handle);

    /* Invoke completion callback if this is the left-most handler */
    if (message->on_completion && !slot->adj_left && testing_handler->complete_write_immediately) {
        message->on_completion(slot->channel, message, testing_handler->complete_write_error_code, message->user_data);
        message->on_completion = NULL;
    }

    return AWS_OP_SUCCESS;
}

static int s_testing_channel_handler_increment_read_window(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    size_t size) {
    (void)slot;

    struct testing_channel_handler *testing_handler = handler->impl;
    testing_handler->latest_window_update = size;
    return AWS_OP_SUCCESS;
}

static int s_testing_channel_handler_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool free_scarce_resources_immediately) {

    struct testing_channel_handler *testing_handler = handler->impl;

    /* If user has registered a callback, invoke it */
    if (testing_handler->on_shutdown) {
        testing_handler->on_shutdown(
            dir, error_code, free_scarce_resources_immediately, testing_handler->on_shutdown_user_data);
    }

    if (dir == AWS_CHANNEL_DIR_WRITE) {
        if (!slot->adj_left) {
            /* Invoke the on_completion callbacks for any queued messages */
            struct aws_linked_list_node *node = aws_linked_list_begin(&testing_handler->messages);
            while (node != aws_linked_list_end(&testing_handler->messages)) {
                struct aws_io_message *msg = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);

                if (msg->on_completion) {
                    msg->on_completion(slot->channel, msg, AWS_IO_SOCKET_CLOSED, msg->user_data);
                    msg->on_completion = NULL;
                }

                node = aws_linked_list_next(node);
            }
        }
    }

    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, free_scarce_resources_immediately);
}

static size_t s_testing_channel_handler_initial_window_size(struct aws_channel_handler *handler) {
    struct testing_channel_handler *testing_handler = handler->impl;
    return testing_handler->initial_window;
}

static size_t s_testing_channel_handler_message_overhead(struct aws_channel_handler *handler) {
    (void)handler;
    return 0;
}

static void s_testing_channel_handler_destroy(struct aws_channel_handler *handler) {
    struct testing_channel_handler *testing_handler = handler->impl;

    while (!aws_linked_list_empty(&testing_handler->messages)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&testing_handler->messages);
        struct aws_io_message *msg = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);
        aws_mem_release(msg->allocator, msg);
    }

    aws_mem_release(handler->alloc, testing_handler);
    aws_mem_release(handler->alloc, handler);
}

static void s_testing_channel_handler_reset_statistics(struct aws_channel_handler *handler) {
    struct testing_channel_handler *testing_handler = handler->impl;

    aws_crt_statistics_socket_reset(&testing_handler->stats);
}

static void s_testing_channel_handler_gather_statistics(
    struct aws_channel_handler *handler,
    struct aws_array_list *stats) {
    struct testing_channel_handler *testing_handler = handler->impl;

    void *stats_base = &testing_handler->stats;
    aws_array_list_push_back(stats, &stats_base);
}

static struct aws_channel_handler_vtable s_testing_channel_handler_vtable = {
    .process_read_message = s_testing_channel_handler_process_read_message,
    .process_write_message = s_testing_channel_handler_process_write_message,
    .increment_read_window = s_testing_channel_handler_increment_read_window,
    .shutdown = s_testing_channel_handler_shutdown,
    .initial_window_size = s_testing_channel_handler_initial_window_size,
    .message_overhead = s_testing_channel_handler_message_overhead,
    .destroy = s_testing_channel_handler_destroy,
    .gather_statistics = s_testing_channel_handler_gather_statistics,
    .reset_statistics = s_testing_channel_handler_reset_statistics,
};

static struct aws_channel_handler *s_new_testing_channel_handler(
    struct aws_allocator *allocator,
    size_t initial_window) {
    struct aws_channel_handler *handler = aws_mem_calloc(allocator, 1, sizeof(struct aws_channel_handler));
    struct testing_channel_handler *testing_handler =
        aws_mem_calloc(allocator, 1, sizeof(struct testing_channel_handler));
    aws_linked_list_init(&testing_handler->messages);
    testing_handler->initial_window = initial_window;
    testing_handler->latest_window_update = 0;
    testing_handler->complete_write_immediately = true;
    testing_handler->complete_write_error_code = AWS_ERROR_SUCCESS;
    handler->impl = testing_handler;
    handler->vtable = &s_testing_channel_handler_vtable;
    handler->alloc = allocator;

    return handler;
}

struct testing_channel {
    struct aws_event_loop *loop;
    struct testing_loop *loop_impl;
    struct aws_channel *channel;
    struct testing_channel_handler *left_handler_impl;
    struct testing_channel_handler *right_handler_impl;
    struct aws_channel_slot *left_handler_slot;
    struct aws_channel_slot *right_handler_slot;

    void (*channel_shutdown)(int error_code, void *user_data);
    void *channel_shutdown_user_data;

    bool channel_setup_completed;
    bool channel_shutdown_completed;
    int channel_shutdown_error_code;
};

static void s_testing_channel_on_setup_completed(struct aws_channel *channel, int error_code, void *user_data) {
    (void)channel;
    (void)error_code;
    struct testing_channel *testing = user_data;
    testing->channel_setup_completed = true;
}

static void s_testing_channel_on_shutdown_completed(struct aws_channel *channel, int error_code, void *user_data) {
    (void)channel;
    (void)error_code;
    struct testing_channel *testing = user_data;
    testing->channel_shutdown_completed = true;
    testing->channel_shutdown_error_code = error_code;

    if (testing->channel_shutdown) {
        testing->channel_shutdown(error_code, testing->channel_shutdown_user_data);
    }
}

/** API for testing, use this for testing purely your channel handlers and nothing else. Because of that, the s_
 * convention isn't used on the functions (since they're intended for you to call). */

/** when you want to test the read path of your handler, call this with the message you want it to read. */
static inline int testing_channel_push_read_message(struct testing_channel *testing, struct aws_io_message *message) {
    return aws_channel_slot_send_message(testing->left_handler_slot, message, AWS_CHANNEL_DIR_READ);
}

/** when you want to test the write path of your handler, call this with the message you want it to write.
 * A downstream handler must have been installed */
static inline int testing_channel_push_write_message(struct testing_channel *testing, struct aws_io_message *message) {
    ASSERT_NOT_NULL(testing->right_handler_slot);
    return aws_channel_slot_send_message(testing->right_handler_slot, message, AWS_CHANNEL_DIR_WRITE);
}

/** when you want to test the write output of your handler, call this, get the queue and iterate the messages. */
static inline struct aws_linked_list *testing_channel_get_written_message_queue(struct testing_channel *testing) {
    return &testing->left_handler_impl->messages;
}

/** Set whether written messages have their on_complete callbacks invoked immediately.
 * The on_complete callback will be cleared after it is invoked. */
static inline void testing_channel_complete_written_messages_immediately(
    struct testing_channel *testing,
    bool complete_immediately,
    int complete_error_code) {

    testing->left_handler_impl->complete_write_immediately = complete_immediately;
    testing->left_handler_impl->complete_write_error_code = complete_error_code;
}

/** when you want to test the read output of your handler, call this, get the queue and iterate the messages.
 * A downstream handler must have been installed */
static inline struct aws_linked_list *testing_channel_get_read_message_queue(struct testing_channel *testing) {
    AWS_ASSERT(testing->right_handler_impl);
    return &testing->right_handler_impl->messages;
}

/** When you want to see what the latest window update issues from your channel handler was, call this. */
static inline size_t testing_channel_last_window_update(struct testing_channel *testing) {
    return testing->left_handler_impl->latest_window_update;
}

/** When you want the downstream handler to issue a window update */
static inline int testing_channel_increment_read_window(struct testing_channel *testing, size_t size) {
    ASSERT_NOT_NULL(testing->right_handler_slot);
    return aws_channel_slot_increment_read_window(testing->right_handler_slot, size);
}

/** Executes all currently scheduled tasks whose time has come.
 * Use testing_channel_drain_queued_tasks() to repeatedly run tasks until only future-tasks remain.
 */
static inline void testing_channel_run_currently_queued_tasks(struct testing_channel *testing) {
    AWS_ASSERT(aws_channel_thread_is_callers_thread(testing->channel));

    uint64_t now = 0;
    aws_event_loop_current_clock_time(testing->loop, &now);
    aws_task_scheduler_run_all(&testing->loop_impl->scheduler, now);
}

/** Repeatedly executes scheduled tasks until only those in the future remain.
 * This covers the common case where there's a chain reaction of now-tasks scheduling further now-tasks.
 */
static inline void testing_channel_drain_queued_tasks(struct testing_channel *testing) {
    AWS_ASSERT(aws_channel_thread_is_callers_thread(testing->channel));

    uint64_t now = 0;
    uint64_t next_task_time = 0;
    size_t count = 0;

    while (true) {
        aws_event_loop_current_clock_time(testing->loop, &now);
        if (aws_task_scheduler_has_tasks(&testing->loop_impl->scheduler, &next_task_time) && (next_task_time <= now)) {
            aws_task_scheduler_run_all(&testing->loop_impl->scheduler, now);
        } else {
            break;
        }

        /* NOTE: This will loop infinitely if there's a task the perpetually re-schedules another task.
         * Consider capping the number of loops if we want to support that behavior. */
        if ((++count % 1000) == 0) {
            AWS_LOGF_WARN(
                AWS_LS_IO_CHANNEL,
                "id=%p: testing_channel_drain_queued_tasks() has looped %zu times.",
                (void *)testing->channel,
                count);
        }
    }
}
/** When you want to force the  "not on channel thread path" for your handler, set 'on_users_thread' to false.
 * when you want to undo that, set it back to true. If you set it to false, you'll need to call
 * 'testing_channel_execute_queued_tasks()' to invoke the tasks that ended up being scheduled. */
static inline void testing_channel_set_is_on_users_thread(struct testing_channel *testing, bool on_users_thread) {
    testing->loop_impl->mock_on_callers_thread = on_users_thread;
}

struct aws_testing_channel_options {
    aws_io_clock_fn *clock_fn;
};

static inline int testing_channel_init(
    struct testing_channel *testing,
    struct aws_allocator *allocator,
    struct aws_testing_channel_options *options) {
    AWS_ZERO_STRUCT(*testing);

    testing->loop = s_testing_loop_new(allocator, options->clock_fn);
    testing->loop_impl = testing->loop->impl_data;

    struct aws_channel_options args = {
        .on_setup_completed = s_testing_channel_on_setup_completed,
        .on_shutdown_completed = s_testing_channel_on_shutdown_completed,
        .setup_user_data = testing,
        .shutdown_user_data = testing,
        .event_loop = testing->loop,
        .enable_read_back_pressure = true,
    };

    testing->channel = aws_channel_new(allocator, &args);

    /* Wait for channel to finish setup */
    testing_channel_drain_queued_tasks(testing);
    ASSERT_TRUE(testing->channel_setup_completed);

    testing->left_handler_slot = aws_channel_slot_new(testing->channel);
    struct aws_channel_handler *handler = s_new_testing_channel_handler(allocator, 16 * 1024);
    testing->left_handler_impl = handler->impl;
    ASSERT_SUCCESS(aws_channel_slot_set_handler(testing->left_handler_slot, handler));

    return AWS_OP_SUCCESS;
}

static inline int testing_channel_clean_up(struct testing_channel *testing) {
    aws_channel_shutdown(testing->channel, AWS_ERROR_SUCCESS);

    /* Wait for channel to finish shutdown */
    testing_channel_drain_queued_tasks(testing);
    ASSERT_TRUE(testing->channel_shutdown_completed);

    aws_channel_destroy(testing->channel);

    /* event_loop can't be destroyed from its own thread */
    testing_channel_set_is_on_users_thread(testing, false);
    aws_event_loop_destroy(testing->loop);

    return AWS_OP_SUCCESS;
}

/** When you want to test your handler with a downstream handler installed to the right. */
static inline int testing_channel_install_downstream_handler(struct testing_channel *testing, size_t initial_window) {
    ASSERT_NULL(testing->right_handler_slot);

    testing->right_handler_slot = aws_channel_slot_new(testing->channel);
    ASSERT_NOT_NULL(testing->right_handler_slot);
    ASSERT_SUCCESS(aws_channel_slot_insert_end(testing->channel, testing->right_handler_slot));

    struct aws_channel_handler *handler =
        s_new_testing_channel_handler(testing->left_handler_slot->alloc, initial_window);
    ASSERT_NOT_NULL(handler);
    testing->right_handler_impl = handler->impl;
    ASSERT_SUCCESS(aws_channel_slot_set_handler(testing->right_handler_slot, handler));

    return AWS_OP_SUCCESS;
}

/** Return whether channel is completely shut down */
static inline bool testing_channel_is_shutdown_completed(const struct testing_channel *testing) {
    return testing->channel_shutdown_completed;
}

/** Return channel's shutdown error_code */
static inline int testing_channel_get_shutdown_error_code(const struct testing_channel *testing) {
    AWS_ASSERT(testing->channel_shutdown_completed);
    return testing->channel_shutdown_error_code;
}

/**
 * Set a callback which is invoked during the handler's shutdown,
 * once in the read direction and again in the write direction.
 * Use this to inject actions that might occur in the middle of channel shutdown.
 */
static inline void testing_channel_set_downstream_handler_shutdown_callback(
    struct testing_channel *testing,
    testing_channel_handler_on_shutdown_fn *on_shutdown,
    void *user_data) {

    AWS_ASSERT(testing->right_handler_impl);
    testing->right_handler_impl->on_shutdown = on_shutdown;
    testing->right_handler_impl->on_shutdown_user_data = user_data;
}

/* Pop first message from queue and compare its contents to expected data. */
static inline int testing_channel_check_written_message(
    struct testing_channel *channel,
    struct aws_byte_cursor expected) {
    struct aws_linked_list *msgs = testing_channel_get_written_message_queue(channel);
    ASSERT_TRUE(!aws_linked_list_empty(msgs));
    struct aws_linked_list_node *node = aws_linked_list_pop_front(msgs);
    struct aws_io_message *msg = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);

    ASSERT_BIN_ARRAYS_EQUALS(expected.ptr, expected.len, msg->message_data.buffer, msg->message_data.len);

    aws_mem_release(msg->allocator, msg);

    return AWS_OP_SUCCESS;
}

/* Pop first message from queue and compare its contents to expected data. */
static inline int testing_channel_check_written_message_str(struct testing_channel *channel, const char *expected) {
    return testing_channel_check_written_message(channel, aws_byte_cursor_from_c_str(expected));
}

/* copies all messages in a list into a buffer, cleans up messages*/
static inline int testing_channel_drain_messages(struct aws_linked_list *msgs, struct aws_byte_buf *buffer) {

    while (!aws_linked_list_empty(msgs)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(msgs);
        struct aws_io_message *msg = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);

        struct aws_byte_cursor msg_cursor = aws_byte_cursor_from_buf(&msg->message_data);
        aws_byte_buf_append_dynamic(buffer, &msg_cursor);

        aws_mem_release(msg->allocator, msg);
    }

    return AWS_OP_SUCCESS;
}

/* Pop all messages from queue and compare their contents to expected data */
static inline int testing_channel_check_messages_ex(
    struct aws_linked_list *msgs,
    struct aws_allocator *allocator,
    struct aws_byte_cursor expected) {
    struct aws_byte_buf all_msgs;
    ASSERT_SUCCESS(aws_byte_buf_init(&all_msgs, allocator, 1024));

    ASSERT_SUCCESS(testing_channel_drain_messages(msgs, &all_msgs));

    ASSERT_BIN_ARRAYS_EQUALS(expected.ptr, expected.len, all_msgs.buffer, all_msgs.len);
    aws_byte_buf_clean_up(&all_msgs);
    return AWS_OP_SUCCESS;
}

/* Check contents of all messages sent in the write direction. */
static inline int testing_channel_check_written_messages(
    struct testing_channel *channel,
    struct aws_allocator *allocator,
    struct aws_byte_cursor expected) {

    struct aws_linked_list *msgs = testing_channel_get_written_message_queue(channel);
    return testing_channel_check_messages_ex(msgs, allocator, expected);
}

/* Check contents of all messages sent in the write direction. */
static inline int testing_channel_check_written_messages_str(
    struct testing_channel *channel,
    struct aws_allocator *allocator,
    const char *expected) {

    return testing_channel_check_written_messages(channel, allocator, aws_byte_cursor_from_c_str(expected));
}

/* Extract contents of all messages sent in the write direction. */
static inline int testing_channel_drain_written_messages(struct testing_channel *channel, struct aws_byte_buf *output) {
    struct aws_linked_list *msgs = testing_channel_get_written_message_queue(channel);
    ASSERT_SUCCESS(testing_channel_drain_messages(msgs, output));

    return AWS_OP_SUCCESS;
}

/* Check contents of all read-messages sent in the read direction by a midchannel http-handler */
static inline int testing_channel_check_midchannel_read_messages(
    struct testing_channel *channel,
    struct aws_allocator *allocator,
    struct aws_byte_cursor expected) {

    struct aws_linked_list *msgs = testing_channel_get_read_message_queue(channel);
    return testing_channel_check_messages_ex(msgs, allocator, expected);
}

/* Check contents of all read-messages sent in the read direction by a midchannel http-handler */
static inline int testing_channel_check_midchannel_read_messages_str(
    struct testing_channel *channel,
    struct aws_allocator *allocator,
    const char *expected) {

    return testing_channel_check_midchannel_read_messages(channel, allocator, aws_byte_cursor_from_c_str(expected));
}

/* For sending an aws_io_message into the channel, in the write or read direction */
static inline int testing_channel_send_data(
    struct testing_channel *channel,
    struct aws_byte_cursor data,
    enum aws_channel_direction dir,
    bool ignore_send_message_errors) {

    struct aws_io_message *msg =
        aws_channel_acquire_message_from_pool(channel->channel, AWS_IO_MESSAGE_APPLICATION_DATA, data.len);
    ASSERT_NOT_NULL(msg);

    ASSERT_TRUE(aws_byte_buf_write_from_whole_cursor(&msg->message_data, data));

    int err;
    if (dir == AWS_CHANNEL_DIR_READ) {
        err = testing_channel_push_read_message(channel, msg);
    } else {
        err = testing_channel_push_write_message(channel, msg);
    }

    if (err) {
        /* If an error happens, clean the message here. Else, the recipient of the message will take the ownership */
        aws_mem_release(msg->allocator, msg);
    }

    if (!ignore_send_message_errors) {
        ASSERT_SUCCESS(err);
    }

    return AWS_OP_SUCCESS;
}

/** Create an aws_io_message, containing the following data, and pushes it up the channel in the read direction */
static inline int testing_channel_push_read_data(struct testing_channel *channel, struct aws_byte_cursor data) {
    return testing_channel_send_data(channel, data, AWS_CHANNEL_DIR_READ, false);
}

/** Create an aws_io_message, containing the following data, and pushes it up the channel in the read direction */
static inline int testing_channel_push_read_str(struct testing_channel *channel, const char *str) {
    return testing_channel_send_data(channel, aws_byte_cursor_from_c_str(str), AWS_CHANNEL_DIR_READ, false);
}

/** Create an aws_io_message, containing the following data.
 * Tries to push it up the channel in the read direction, but don't assert if the message can't be sent.
 * Useful for testing data that arrives during handler shutdown */
static inline int testing_channel_push_read_str_ignore_errors(struct testing_channel *channel, const char *str) {
    return testing_channel_send_data(channel, aws_byte_cursor_from_c_str(str), AWS_CHANNEL_DIR_READ, true);
}

/** Create an aws_io_message, containing the following data, and pushes it up the channel in the write direction */
static inline int testing_channel_push_write_data(struct testing_channel *channel, struct aws_byte_cursor data) {
    return testing_channel_send_data(channel, data, AWS_CHANNEL_DIR_WRITE, false);
}

/** Create an aws_io_message, containing the following data, and pushes it up the channel in the write direction */
static inline int testing_channel_push_write_str(struct testing_channel *channel, const char *str) {
    return testing_channel_send_data(channel, aws_byte_cursor_from_c_str(str), AWS_CHANNEL_DIR_WRITE, false);
}

#endif /* AWS_TESTING_IO_TESTING_CHANNEL_H */
