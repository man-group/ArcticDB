/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#define ARCTICDB_MOVE_ONLY_DEFAULT(__T__) \
    __T__(__T__ && ) noexcept = default; \
    __T__& operator=(__T__ && )  noexcept = default; \
    __T__(const __T__ & ) = delete; \
    __T__& operator=(const __T__ & ) = delete;

#define ARCTICDB_MOVE_ONLY_DEFAULT_EXCEPT(__T__) \
    __T__(__T__ && )  = default; \
    __T__& operator=(__T__ && )  = default; \
    __T__(const __T__ & ) = delete; \
    __T__& operator=(const __T__ & ) = delete;

#define ARCTICDB_MOVE_COPY_DEFAULT(__T__) \
    __T__(__T__ && ) noexcept = default; \
    __T__& operator=(__T__ && )  noexcept = default; \
    __T__(const __T__ & ) = default; \
    __T__& operator=(const __T__ & ) = default;

#define ARCTICDB_NO_MOVE_OR_COPY(__T__) \
    __T__(__T__ && ) noexcept = delete; \
    __T__& operator=(__T__ && )  noexcept = delete; \
    __T__(const __T__ & ) = delete; \
    __T__& operator=(const __T__ & ) = delete;

#define ARCTICDB_NO_COPY(__T__) \
    __T__(const __T__ & ) = delete; \
    __T__& operator=(const __T__ & ) = delete;

#define ARCTICDB_MOVE(__T__) \
    __T__(__T__ && ) noexcept = default; \
    __T__& operator=(__T__ && )  noexcept = default;
