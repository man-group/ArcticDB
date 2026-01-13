/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_client_impl.hpp>
#include <arcticdb/storage/mock/lmdb_mock_client.hpp>

#include <filesystem>

#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/constants.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/util/format_bytes.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/test/random_throw.hpp>

#include <arcticdb/storage/storage_exceptions.hpp>

namespace arcticdb::storage::lmdb {

struct LmdbKeepalive {
    std::shared_ptr<LmdbInstance> instance_;
    std::shared_ptr<::lmdb::txn> transaction_;

    LmdbKeepalive(std::shared_ptr<LmdbInstance> instance, std::shared_ptr<::lmdb::txn> transaction) :
        instance_(std::move(instance)),
        transaction_(std::move(transaction)) {}
};

static void raise_lmdb_exception(const ::lmdb::error& e, const std::string& object_name) {
    auto error_code = e.code();

    auto error_message_suffix = fmt::format("LMDBError#{}: {} for object {}", error_code, e.what(), object_name);

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

::lmdb::env& LmdbStorage::env() {
    storage::check<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(
            lmdb_instance_,
            "Unexpected LMDB Error: Invalid operation: LMDB environment has been removed. Possibly because the library "
            "has been deleted"
    );
    return lmdb_instance_->env_;
}

::lmdb::dbi& LmdbStorage::get_dbi(const std::string& db_name) {
    storage::check<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(
            lmdb_instance_,
            "Unexpected LMDB Error: Invalid operation: LMDB environment has been removed. Possibly because the library "
            "has been deleted"
    );
    return *(lmdb_instance_->dbi_by_key_type_.at(db_name));
}

void LmdbStorage::do_write_internal(KeySegmentPair& key_seg, ::lmdb::txn& txn) {
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)

    auto db_name = fmt::format(FMT_COMPILE("{}"), key_seg.key_type());
    ::lmdb::dbi& dbi = get_dbi(db_name);

    ARCTICDB_SUBSAMPLE(LmdbStorageWriteValues, 0)
    ARCTICDB_DEBUG(log::storage(), "Lmdb storage writing segment with key {}", key_seg.key_view());
    auto k = to_serialized_key(key_seg.variant_key());
    auto& seg = *key_seg.segment_ptr();
    int64_t overwrite_flag = std::holds_alternative<RefKey>(key_seg.variant_key()) ? 0 : MDB_NOOVERWRITE;
    try {
        lmdb_client_->write(db_name, k, seg, txn, dbi, overwrite_flag);
    } catch (const ::lmdb::key_exist_error& e) {
        throw DuplicateKeyException(fmt::format("Key already exists: {}: {}", key_seg.variant_key(), e.what()));
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex, k);
    }
}

std::string LmdbStorage::name() const { return fmt::format("lmdb_storage-{}", lib_dir_.string()); }

void LmdbStorage::do_write(KeySegmentPair& key_seg) {
    ARCTICDB_SAMPLE(LmdbStorageWrite, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env()); // scoped abort on exception, so no partial writes
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    do_write_internal(key_seg, txn);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();
}

void LmdbStorage::do_update(KeySegmentPair& key_seg, UpdateOpts opts) {
    ARCTICDB_SAMPLE(LmdbStorageUpdate, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    auto key = key_seg.variant_key();
    // Deleting keys (no error is thrown if the keys already exist)
    RemoveOpts remove_opts;
    remove_opts.ignores_missing_key_ = opts.upsert_;
    std::array<VariantKey, 1> arr{std::move(key)};
    auto failed_deletes = do_remove_internal(std::span(arr), txn, remove_opts);
    if (!failed_deletes.empty()) {
        ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
        txn.commit();
        std::string err_message =
                fmt::format("do_update called with upsert=false on non-existent key(s): {}", failed_deletes);
        throw KeyNotFoundException(failed_deletes, err_message);
    }
    do_write_internal(key_seg, txn);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();
}

KeySegmentPair LmdbStorage::do_read(VariantKey&& variant_key, ReadKeyOpts) {
    ARCTICDB_SAMPLE(LmdbStorageReadReturn, 0)
    std::optional<VariantKey> failed_read;
    auto db_name = fmt::format(FMT_COMPILE("{}"), variant_key_type(variant_key));
    ::lmdb::dbi& dbi = get_dbi(db_name);
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
    auto stored_key = to_serialized_key(variant_key);
    try {
        auto txn = std::make_shared<::lmdb::txn>(::lmdb::txn::begin(env(), nullptr, MDB_RDONLY));
        ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
        auto segment = lmdb_client_->read(db_name, stored_key, *txn, dbi);

        if (segment.has_value()) {
            ARCTICDB_SUBSAMPLE(LmdbStorageVisitSegment, 0)
            segment->set_keepalive(std::any{LmdbKeepalive{lmdb_instance_, std::move(txn)}});
            ARCTICDB_DEBUG(
                    log::storage(),
                    "Read key {}: {}, with {} bytes of data",
                    variant_key_type(variant_key),
                    variant_key_view(variant_key),
                    segment->size()
            );
            return {VariantKey{variant_key}, std::move(*segment)};
        } else {
            ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}", variant_key_view(variant_key));
            throw KeyNotFoundException(variant_key);
        }
    } catch (const ::lmdb::not_found_error&) {
        ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}", variant_key_view(variant_key));
        throw KeyNotFoundException(variant_key);
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex, stored_key);
    }
    return KeySegmentPair{};
}

void LmdbStorage::do_read(VariantKey&& key, const ReadVisitor& visitor, storage::ReadKeyOpts) {
    ARCTICDB_SAMPLE(LmdbStorageRead, 0)
    std::optional<VariantKey> failed_read;
    auto db_name = fmt::format(FMT_COMPILE("{}"), variant_key_type(key));
    ::lmdb::dbi& dbi = get_dbi(db_name);
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
    auto stored_key = to_serialized_key(key);
    try {
        auto txn = std::make_shared<::lmdb::txn>(::lmdb::txn::begin(env(), nullptr, MDB_RDONLY));
        ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
        auto segment = lmdb_client_->read(db_name, stored_key, *txn, dbi);

        if (segment.has_value()) {
            ARCTICDB_SUBSAMPLE(LmdbStorageVisitSegment, 0)
            segment->set_keepalive(std::any{LmdbKeepalive{lmdb_instance_, std::move(txn)}});
            ARCTICDB_DEBUG(
                    log::storage(),
                    "Read key {}: {}, with {} bytes of data",
                    variant_key_type(key),
                    variant_key_view(key),
                    segment->size()
            );
            visitor(key, std::move(*segment));
        } else {
            ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}", variant_key_view(key));
            failed_read.emplace(key);
        }
    } catch (const ::lmdb::not_found_error&) {
        ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}", variant_key_view(key));
        failed_read.emplace(key);
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex, stored_key);
    }

    if (failed_read)
        throw KeyNotFoundException(*failed_read);
}

bool LmdbStorage::do_key_exists(const VariantKey& key) {
    ARCTICDB_SAMPLE(LmdbStorageKeyExists, 0)
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY);
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    ARCTICDB_DEBUG_THROW(5)
    auto db_name = fmt::format("{}", variant_key_type(key));
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
    auto stored_key = to_serialized_key(key);
    try {
        ::lmdb::dbi& dbi = get_dbi(db_name);
        return lmdb_client_->exists(db_name, stored_key, txn, dbi);
    } catch ([[maybe_unused]] const ::lmdb::not_found_error& ex) {
        ARCTICDB_DEBUG(log::storage(), "Caught lmdb not found error: {}", ex.what());
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex, stored_key);
    }
    return false;
}

boost::container::small_vector<VariantKey, 1> LmdbStorage::do_remove_internal(
        std::span<VariantKey> variant_keys, ::lmdb::txn& txn, RemoveOpts opts
) {
    boost::container::small_vector<VariantKey, 1> failed_deletes;

    ARCTICDB_DEBUG_THROW(5)
    for (auto&& key : variant_keys) {
        auto db_name = fmt::format("{}", variant_key_type(key));
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        try {
            ::lmdb::dbi& dbi = get_dbi(db_name);
            auto stored_key = to_serialized_key(key);

            try {
                if (lmdb_client_->remove(db_name, stored_key, txn, dbi)) {
                    ARCTICDB_DEBUG(log::storage(), "Deleted segment for key {}", variant_key_view(key));
                } else {
                    if (!opts.ignores_missing_key_) {
                        log::storage().warn("Failed to delete segment for key {}", variant_key_view(key));
                        failed_deletes.emplace_back(key);
                    }
                }
            } catch (const ::lmdb::not_found_error&) {
                if (!opts.ignores_missing_key_) {
                    log::storage().warn("Failed to delete segment for key {}", variant_key_view(key));
                    failed_deletes.emplace_back(key);
                }
            } catch (const ::lmdb::error& ex) {
                raise_lmdb_exception(ex, stored_key);
            }
        } catch (const ::lmdb::error& ex) {
            raise_lmdb_exception(ex, db_name);
        }
    }
    return failed_deletes;
}

void LmdbStorage::do_remove(VariantKey&& variant_key, RemoveOpts opts) {
    ARCTICDB_SAMPLE(LmdbStorageRemove, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    std::array<VariantKey, 1> arr{std::move(variant_key)};
    auto failed_deletes = do_remove_internal(std::span{arr}, txn, opts);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();

    if (!failed_deletes.empty())
        throw KeyNotFoundException(failed_deletes);
}

void LmdbStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
    ARCTICDB_SAMPLE(LmdbStorageRemoveMultiple, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    auto failed_deletes = do_remove_internal(variant_keys, txn, opts);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();

    if (!failed_deletes.empty())
        throw KeyNotFoundException(failed_deletes);
}

bool LmdbStorage::do_fast_delete() {
    std::lock_guard<std::mutex> lock{*write_mutex_};
    // bool is probably not the best return type here but it does help prevent the insane boilerplate for
    // an additional function that checks whether this is supported (like the prefix matching)
    auto dtxn = ::lmdb::txn::begin(env());
    foreach_key_type([&](KeyType key_type) {
        if (key_type == KeyType::TOMBSTONE) {
            // TOMBSTONE and LOCK both format to code 'x' - do not try to drop both
            return;
        }
        auto db_name = fmt::format("{}", key_type);
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ARCTICDB_TRACE(log::storage(), "LMDB storage dropping keytype {}", db_name);
        ::lmdb::dbi& dbi = get_dbi(db_name);
        try {
            ::lmdb::dbi_drop(dtxn, dbi);
        } catch (const ::lmdb::error& ex) {
            raise_lmdb_exception(ex, db_name);
        }
    });

    dtxn.commit();
    return true;
}

bool LmdbStorage::do_iterate_type_until_match(
        KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
) {
    ARCTICDB_SAMPLE(LmdbStorageItType, 0)
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY); // scoped abort on
    std::string type_db = fmt::format("{}", key_type);
    ::lmdb::dbi& dbi = get_dbi(type_db);

    try {
        auto keys = lmdb_client_->list(type_db, prefix, txn, dbi, key_type);
        for (auto& k : keys) {
            ARCTICDB_SUBSAMPLE(LmdbStorageVisitKey, 0)
            if (visitor(std::move(k))) {
                return true;
            }
        }
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex, type_db);
    }
    return false;
}

bool LmdbStorage::do_is_path_valid(std::string_view path ARCTICDB_UNUSED) const {
#ifdef _WIN32
    // Note that \ and / are valid characters as they will create subdirectories which are expected to work.
    // The filenames such as COM1, LPT1, AUX, CON etc. are reserved but not strictly disallowed by Windows as directory
    // names. Therefore, paths with these names are allowed.
    std::string_view invalid_win32_chars = "<>:\"|?*";
    auto found = path.find_first_of(invalid_win32_chars);
    if (found != std::string::npos) {
        return false;
    }

    if (!path.empty() && (path.back() == '.' || std::isspace(path.back()))) {
        return false;
    }
#endif
    return true;
}

void remove_db_files(const fs::path& lib_path) {
    std::array<std::string, 2> files = {"lock.mdb", "data.mdb"};

    for (const auto& file : files) {
        fs::path file_path = lib_path / file;
        try {
            if (fs::exists(file_path)) {
                fs::remove(file_path);
            }
        } catch (const std::filesystem::filesystem_error& e) {
            raise<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(fmt::format(
                    "Unexpected LMDB Error: Failed to remove LMDB file at path: {} error: {}",
                    file_path.string(),
                    e.what()
            ));
        }
    }

    if (fs::exists(lib_path)) {
        if (!fs::is_empty(lib_path)) {
            log::storage().warn(fmt::format("Skipping deletion of directory holding LMDB library during "
                                            "library deletion as it contains files unrelated to LMDB"));
        } else {
            try {
                fs::remove_all(lib_path);
            } catch (const fs::filesystem_error& e) {
                raise<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(fmt::format(
                        "Unexpected LMDB Error: Failed to remove directory: {} error: {}", lib_path.string(), e.what()
                ));
            }
        }
    }
}

void LmdbStorage::cleanup() {
    lmdb_instance_.reset();
    remove_db_files(lib_dir_);
}

namespace {
template<class T>
T or_else(T val, T or_else_val, T def = T()) {
    return val == def ? or_else_val : val;
}
} // namespace

LmdbStorage::LmdbStorage(const LibraryPath& library_path, OpenMode mode, const Config& conf) :
    Storage(library_path, mode) {
    if (conf.use_mock_storage_for_testing()) {
        lmdb_client_ = std::make_unique<MockLmdbClient>();
    } else {
        lmdb_client_ = std::make_unique<RealLmdbClient>();
    }
    const auto lib_path_str = library_path.to_delim_path(fs::path::preferred_separator);
    const fs::path root_path = conf.path().c_str();
    lib_dir_ = root_path / lib_path_str;

    write_mutex_ = std::make_unique<std::mutex>();
    lmdb_instance_ = std::make_shared<LmdbInstance>(LmdbInstance{::lmdb::env::create(conf.flags()), {}});

    warn_if_lmdb_already_open();

    if (!fs::exists(lib_dir_)) {
        util::check_arg(
                mode > OpenMode::READ,
                "Missing dir {} for lib={}. mode={}",
                lib_dir_.generic_string(),
                lib_path_str,
                mode
        );

        fs::create_directories(lib_dir_);
    }

    if (fs::exists(lib_dir_ / "data.mdb")) {
        if (conf.recreate_if_exists() && mode >= OpenMode::WRITE) {
            fs::remove(lib_dir_ / "data.mdb");
            fs::remove(lib_dir_ / "lock.mdb");
        }
    }

    const bool is_read_only = ((conf.flags() & MDB_RDONLY) != 0);
    util::check_arg(
            is_read_only || mode != OpenMode::READ, "Flags {} and operating mode {} are conflicting", conf.flags(), mode
    );
    // Windows needs a sensible size as it allocates disk for the whole file even before any writes. Linux just gets an
    // arbitrarily large size that it probably won't ever reach.
#ifdef _WIN32
    // At least enough for 300 cols and 1M rows
    constexpr uint64_t default_map_size = 2ULL * (1ULL << 30); /* 2 GiB */
#else
    constexpr uint64_t default_map_size = 400ULL * (1ULL << 30); /* 400 GiB */
#endif
    auto mapsize = or_else(static_cast<uint64_t>(conf.map_size()), default_map_size);
    env().set_mapsize(mapsize);
    env().set_max_dbs(or_else(static_cast<unsigned int>(conf.max_dbs()), 1024U));
    env().set_max_readers(or_else(conf.max_readers(), 1024U));
    env().open(lib_dir_.generic_string().c_str(), MDB_NOTLS);

    auto txn = ::lmdb::txn::begin(env());

    try {
        arcticdb::entity::foreach_key_type([&txn, this](KeyType&& key_type) {
            std::string db_name = fmt::format("{}", key_type);
            ::lmdb::dbi dbi = ::lmdb::dbi::open(txn, db_name.data(), MDB_CREATE);
            lmdb_instance_->dbi_by_key_type_.emplace(std::move(db_name), std::make_unique<::lmdb::dbi>(std::move(dbi)));
        });
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex, "dbi creation");
    }

    txn.commit();

    ARCTICDB_DEBUG(
            log::storage(), "Opened lmdb storage at {} with map size {}", lib_dir_.string(), format_bytes(mapsize)
    );
}

void LmdbStorage::warn_if_lmdb_already_open() {
    uint64_t& count_for_pid = ++times_path_opened[lib_dir_.string()];
    // Only warn for the "base" config library to avoid spamming users with more warnings if they decide to ignore it
    // and continue
    if (count_for_pid != 1 && lib_dir_.string().find(CONFIG_LIBRARY_NAME) != std::string::npos) {
        std::filesystem::path user_facing_path = lib_dir_;
        // Strip magic name from warning as it will confuse users
        user_facing_path.remove_filename();
        log::storage().warn(fmt::format(
                "LMDB path at {} has already been opened in this process which is not supported by LMDB. "
                "You should only open a single Arctic instance over a given LMDB path. "
                "To continue safely, you should delete this Arctic instance and any others over the LMDB path in this "
                "process and then try again. Current process ID=[{}]",
                user_facing_path.string(),
                getpid()
        ));
    }
}

LmdbStorage::LmdbStorage(LmdbStorage&& other) noexcept :
    Storage(std::move(static_cast<Storage&>(other))),
    write_mutex_(std::move(other.write_mutex_)),
    lmdb_instance_(std::move(other.lmdb_instance_)),
    lib_dir_(std::move(other.lib_dir_)) {
    other.lib_dir_ = "";
    lmdb_client_ = std::move(other.lmdb_client_);
}

LmdbStorage::~LmdbStorage() {
    if (!lib_dir_.empty()) {
        --times_path_opened[lib_dir_.string()];
    }
}

void LmdbStorage::reset_warning_counter() { times_path_opened = std::unordered_map<std::string, uint64_t>{}; }

} // namespace arcticdb::storage::lmdb
