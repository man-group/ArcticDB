/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_C_H
#define ARCTICDB_C_H

#include <stdint.h>

/* Symbol visibility for shared library export */
#if defined(_WIN32) || defined(__CYGWIN__)
#ifdef ARCTICDB_C_BUILDING
#define ARCTICDB_C_API __declspec(dllexport)
#else
#define ARCTICDB_C_API __declspec(dllimport)
#endif
#elif __GNUC__ >= 4
#define ARCTICDB_C_API __attribute__((visibility("default")))
#else
#define ARCTICDB_C_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handle ──────────────────────────────────────────────────── */

typedef struct ArcticLibrary ArcticLibrary;

/* ── Error handling ─────────────────────────────────────────────────── */

typedef struct ArcticError {
    int code; /* 0 = success, non-zero = error */
    char message[512];
} ArcticError;

/* ── Arrow C Stream Interface (matches Arrow spec exactly) ──────────── */

struct ArrowSchema; /* defined by the Arrow C Data Interface (sparrow) */
struct ArrowArray;  /* defined by the Arrow C Data Interface (sparrow) */

struct ArcticArrowArrayStream {
    int (*get_schema)(struct ArcticArrowArrayStream*, struct ArrowSchema* out);
    int (*get_next)(struct ArcticArrowArrayStream*, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArcticArrowArrayStream*);
    void (*release)(struct ArcticArrowArrayStream*);
    void* private_data;
};

/* ── Lifecycle ──────────────────────────────────────────────────────── */

/**
 * Open an LMDB-backed ArcticDB library at the given filesystem path.
 * Creates the directory if it does not exist.
 *
 * @param path   Filesystem path for LMDB storage
 * @param out    Receives the library handle on success
 * @param err    Receives error details on failure (may be NULL)
 * @return       0 on success, non-zero on failure
 */
ARCTICDB_C_API int arctic_library_open_lmdb(const char* path, ArcticLibrary** out, ArcticError* err);

/**
 * Close and destroy a library handle. Safe to call with NULL.
 */
ARCTICDB_C_API void arctic_library_close(ArcticLibrary* lib);

/* ── Write (test helper) ────────────────────────────────────────────── */

/**
 * Write synthetic numeric test data to the given symbol.
 * Creates a timeseries-indexed DataFrame with float64 columns named col_0..col_N.
 *
 * @param lib          Library handle
 * @param symbol       Symbol name
 * @param num_rows     Number of rows to write
 * @param num_columns  Number of float64 data columns
 * @param err          Receives error details on failure (may be NULL)
 * @return             0 on success, non-zero on failure
 */
ARCTICDB_C_API int arctic_write_test_data(
        ArcticLibrary* lib, const char* symbol, int64_t num_rows, int64_t num_columns, ArcticError* err
);

/* ── Read ───────────────────────────────────────────────────────────── */

/**
 * Open a streaming reader for the given symbol and version.
 * The caller must allocate the ArcticArrowArrayStream struct; this function fills it.
 *
 * Consumption pattern:
 *   1. Call get_schema() once to get the schema
 *   2. Call get_next() in a loop until out->release == NULL (end of stream)
 *   3. Call release() to free resources
 *
 * @param lib      Library handle
 * @param symbol   Symbol name
 * @param version  Version number, or -1 for latest
 * @param out      Caller-allocated stream struct, filled on success
 * @param err      Receives error details on failure (may be NULL)
 * @return         0 on success, non-zero on failure
 */
ARCTICDB_C_API int arctic_read_stream(
        ArcticLibrary* lib, const char* symbol, int64_t version, struct ArcticArrowArrayStream* out, ArcticError* err
);

/* ── Symbol listing ─────────────────────────────────────────────────── */

/**
 * List all symbols in the library.
 *
 * @param lib          Library handle
 * @param out_symbols  Receives an array of null-terminated strings (allocated by callee)
 * @param out_count    Receives the number of symbols
 * @param err          Receives error details on failure (may be NULL)
 * @return             0 on success, non-zero on failure
 */
ARCTICDB_C_API int arctic_list_symbols(ArcticLibrary* lib, char*** out_symbols, int64_t* out_count, ArcticError* err);

/**
 * Free a symbol list returned by arctic_list_symbols().
 */
ARCTICDB_C_API void arctic_free_symbols(char** symbols, int64_t count);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ARCTICDB_C_H */
