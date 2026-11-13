/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/lmdb/lmdb.hpp>

#include <fmt/format.h>

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
};

/// True for LMDB error codes that indicate the env contents are not what LMDB expects
bool is_lmdb_corruption_error(int error_code);

/// Whether corruption-type errors carry LmdbEnvDiagnostics, from LMDBStorage.Diagnostics in ConfigsMap
/// (env var ARCTICDB_LMDBStorage_Diagnostics_int). Off by default: collecting them parses data.mdb's meta pages by
/// byte offset, which is tied to LMDB's on-disk layout rather than to any public API, and nothing user-facing
/// depends on the result. ArcticDB's own CI turns it on.
bool lmdb_diagnostics_enabled();

LmdbEnvDiagnostics lmdb_env_diagnostics(::lmdb::env& env);

/// Translates an LMDB error into the matching ArcticDB exception, appending env diagnostics (and logging them) when
/// the code is a corruption-type one, an env is available and lmdb_diagnostics_enabled().
[[noreturn]] void raise_lmdb_exception(
        const ::lmdb::error& e, const std::string& object_name, ::lmdb::env* env = nullptr
);

} // namespace arcticdb::storage::lmdb

template<>
struct fmt::formatter<arcticdb::storage::lmdb::LmdbEnvDiagnostics::Meta> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::storage::lmdb::LmdbEnvDiagnostics::Meta& m, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                "{{magic={:#x} version={} mapsize={} psize={} free_root={} main_root={} last_pg={} txnid={}}}",
                m.magic,
                m.version,
                m.mapsize,
                m.psize,
                static_cast<int64_t>(m.free_root),
                static_cast<int64_t>(m.main_root),
                m.last_pg,
                m.txnid
        );
    }
};

template<>
struct fmt::formatter<arcticdb::storage::lmdb::LmdbEnvDiagnostics> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::storage::lmdb::LmdbEnvDiagnostics& d, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                "lmdb env: flags={:#x} mapsize={} maxpg={} last_pgno={} last_txnid={} psize={} readers={}/{}; file "
                "meta0={} meta1={}{}",
                d.flags,
                d.mapsize,
                d.psize ? d.mapsize / d.psize : 0,
                d.last_pgno,
                d.last_txnid,
                d.psize,
                d.num_readers,
                d.max_readers,
                d.file_metas[0],
                d.file_metas[1],
                d.file_read_error.empty() ? "" : fmt::format(" (file read error: {})", d.file_read_error)
        );
    }
};
