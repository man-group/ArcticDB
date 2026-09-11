/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/lmdb/lmdb_error_handling.hpp>

#include <fmt/ranges.h> // must precede storage_exceptions.hpp: KeyNotFoundException formats a vector of keys

#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/key_segment_pair.hpp> // brings entity::VariantKey into arcticdb::storage for the below
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstring>
#include <stdexcept>
#include <vector>
#ifndef _WIN32
#include <unistd.h>
#endif

namespace arcticdb::storage::lmdb {

bool is_lmdb_corruption_error(int error_code) {
    switch (error_code) {
    case MDB_PAGE_NOTFOUND:
    case MDB_CORRUPTED:
    case MDB_PANIC:
    case MDB_INVALID:
    case MDB_MAP_RESIZED:
    case MDB_BAD_TXN:
        return true;
    default:
        return false;
    }
}

namespace {

// Byte offsets within a meta page (64-bit build): 16-byte MDB_page header, then MDB_meta
constexpr size_t META_MAGIC = 16;
constexpr size_t META_VERSION = 20;
constexpr size_t META_MAPSIZE = 32;
constexpr size_t META_DBS =
        40; // MDB_db[2], 48 bytes each: pad(4) flags(2) depth(2) branch(8) leaf(8) overflow(8) entries(8) root(8)
constexpr size_t META_DB_SIZE = 48;
constexpr size_t META_LAST_PG = META_DBS + 2 * META_DB_SIZE;
constexpr size_t META_TXNID = META_LAST_PG + 8;
constexpr size_t META_BYTES = META_TXNID + 8;

template<typename T>
T read_le(const std::vector<uint8_t>& buf, size_t offset) {
    T out{};
    std::memcpy(&out, buf.data() + offset, sizeof(T));
    return out;
}

LmdbEnvDiagnostics::Meta parse_meta(const std::vector<uint8_t>& page) {
    LmdbEnvDiagnostics::Meta meta;
    if (page.size() < META_BYTES)
        return meta;
    meta.magic = read_le<uint32_t>(page, META_MAGIC);
    meta.version = read_le<uint32_t>(page, META_VERSION);
    meta.mapsize = read_le<uint64_t>(page, META_MAPSIZE);
    meta.psize = read_le<uint32_t>(page, META_DBS);
    meta.free_root = read_le<uint64_t>(page, META_DBS + META_DB_SIZE - 8);
    meta.main_root = read_le<uint64_t>(page, META_DBS + 2 * META_DB_SIZE - 8);
    meta.last_pg = read_le<uint64_t>(page, META_LAST_PG);
    meta.txnid = read_le<uint64_t>(page, META_TXNID);
    return meta;
}

// Reads the data file through the file handle, not the map
std::vector<uint8_t> read_file_bytes(mdb_filehandle_t fd, size_t count) {
    std::vector<uint8_t> buf(count);
#ifdef _WIN32
    OVERLAPPED ov{};
    DWORD got = 0;
    if (!ReadFile(fd, buf.data(), static_cast<DWORD>(count), &got, &ov)) {
        throw std::runtime_error(fmt::format("ReadFile failed with error {}", GetLastError()));
    }
    buf.resize(got);
#else
    const auto got = ::pread(fd, buf.data(), count, 0);
    if (got < 0) {
        throw std::runtime_error(fmt::format("pread failed with errno {}", errno));
    }
    buf.resize(static_cast<size_t>(got));
#endif
    return buf;
}

} // namespace

LmdbEnvDiagnostics lmdb_env_diagnostics(::lmdb::env& env) {
    LmdbEnvDiagnostics out;
    MDB_envinfo info{};
    MDB_stat stat{};
    ::lmdb::env_info(env.handle(), &info);
    ::lmdb::env_stat(env.handle(), &stat);
    ::lmdb::env_get_flags(env.handle(), &out.flags);
    out.mapsize = info.me_mapsize;
    out.last_pgno = info.me_last_pgno;
    out.last_txnid = info.me_last_txnid;
    out.max_readers = info.me_maxreaders;
    out.num_readers = info.me_numreaders;
    out.psize = stat.ms_psize;
    try {
        mdb_filehandle_t fd;
        ::lmdb::env_get_fd(env.handle(), &fd);
        const auto bytes = read_file_bytes(fd, 2 * static_cast<size_t>(stat.ms_psize));
        for (size_t i = 0; i < 2; ++i) {
            const auto begin = i * stat.ms_psize;
            if (bytes.size() >= begin + META_BYTES) {
                out.file_metas[i] =
                        parse_meta(std::vector<uint8_t>(bytes.begin() + begin, bytes.begin() + begin + META_BYTES));
            } else {
                out.file_read_error = fmt::format("short read: {} bytes", bytes.size());
            }
        }
    } catch (const std::exception& ex) {
        out.file_read_error = ex.what();
    }
    return out;
}

void raise_lmdb_exception(const ::lmdb::error& e, const std::string& object_name, ::lmdb::env* env) {
    auto error_code = e.code();

    auto error_message_suffix = fmt::format("LMDBError#{}: {} for object {}", error_code, e.what(), object_name);
    if (env != nullptr && is_lmdb_corruption_error(error_code)) {
        std::string diagnostics;
        try {
            diagnostics = fmt::format("{}", lmdb_env_diagnostics(*env));
        } catch (const std::exception& diag_ex) {
            diagnostics = fmt::format("diagnostics unavailable: {}", diag_ex.what());
        }
        log::storage().error("LMDB corruption-type error {}: {}", error_message_suffix, diagnostics);
        error_message_suffix += fmt::format(" | {}", diagnostics);
    }

    if (error_code == MDB_NOTFOUND) {
        throw KeyNotFoundException(fmt::format("Key Not Found Error: {}", error_message_suffix));
    }

    if (error_code == MDB_KEYEXIST) {
        throw DuplicateKeyException(fmt::format("Duplicate Key Error: {}", error_message_suffix));
    }

    if (error_code == MDB_MAP_FULL) {
        throw LMDBMapFullException(fmt::format("Map Full Error: {}", error_message_suffix));
    }

    raise<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(fmt::format("Unexpected LMDB Error: {}", error_message_suffix));
}

} // namespace arcticdb::storage::lmdb
