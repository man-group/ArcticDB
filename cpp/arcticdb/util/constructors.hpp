/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#define ARCTICDB_MOVE_ONLY_DEFAULT(__T__)                                                                              \
    __T__(__T__&&) noexcept = default;                                                                                 \
    __T__& operator=(__T__&&) noexcept = default;                                                                      \
    __T__(const __T__&) = delete;                                                                                      \
    __T__& operator=(const __T__&) = delete;

#define ARCTICDB_MOVE_ONLY_DEFAULT_EXCEPT(__T__)                                                                       \
    __T__(__T__&&) = default;                                                                                          \
    __T__& operator=(__T__&&) = default;                                                                               \
    __T__(const __T__&) = delete;                                                                                      \
    __T__& operator=(const __T__&) = delete;

#define ARCTICDB_MOVE_COPY_DEFAULT(__T__)                                                                              \
    __T__(__T__&&) noexcept = default;                                                                                 \
    __T__& operator=(__T__&&) noexcept = default;                                                                      \
    __T__(const __T__&) = default;                                                                                     \
    __T__& operator=(const __T__&) = default;

#define ARCTICDB_NO_MOVE_OR_COPY(__T__)                                                                                \
    __T__(__T__&&) noexcept = delete;                                                                                  \
    __T__& operator=(__T__&&) noexcept = delete;                                                                       \
    __T__(const __T__&) = delete;                                                                                      \
    __T__& operator=(const __T__&) = delete;

#define ARCTICDB_NO_COPY(__T__)                                                                                        \
    __T__(const __T__&) = delete;                                                                                      \
    __T__& operator=(const __T__&) = delete;

#define ARCTICDB_MOVE(__T__)                                                                                           \
    __T__(__T__&&) noexcept = default;                                                                                 \
    __T__& operator=(__T__&&) noexcept = default;
