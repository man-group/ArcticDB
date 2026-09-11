/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/lmdb/lmdb.hpp>

#include <array>
#include <cstdint>
#include <string>

namespace arcticdb::storage::lmdb {

/// What LMDB believes about an env (mdb_env_info/stat) next to the two meta pages read straight from data.mdb with
/// pread/ReadFile, bypassing the memory map. Attached to corruption-type errors so a failure in CI records whether
/// the mapped view and the file disagree.
struct LmdbEnvDiagnostics {
    struct Meta {
        uint32_t magic = 0;
        uint32_t version = 0;
        uint64_t mapsize = 0;
        uint32_t psize = 0;
        uint64_t free_root = 0;
        uint64_t main_root = 0;
        uint64_t last_pg = 0;
        uint64_t txnid = 0;
    };
    unsigned int flags = 0;
    size_t mapsize = 0;
    size_t last_pgno = 0;
    size_t last_txnid = 0;
    unsigned int psize = 0;
    unsigned int max_readers = 0;
    unsigned int num_readers = 0;
    std::array<Meta, 2> file_metas{};
    std::string file_read_error;

    std::string to_string() const;
};

/// True for LMDB error codes that indicate the env contents are not what LMDB expects
bool is_lmdb_corruption_error(int error_code);

LmdbEnvDiagnostics lmdb_env_diagnostics(::lmdb::env& env);

/// Translates an LMDB error into the matching ArcticDB exception, appending env diagnostics (and logging them) when
/// the code is a corruption-type one and an env is available.
[[noreturn]] void raise_lmdb_exception(
        const ::lmdb::error& e, const std::string& object_name, ::lmdb::env* env = nullptr
);

} // namespace arcticdb::storage::lmdb
