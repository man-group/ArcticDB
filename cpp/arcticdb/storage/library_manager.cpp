#include <arcticdb/storage/library_manager.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/name_validation.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>

#include <algorithm>
#include <cctype>
#include <sstream>

namespace arcticdb::storage {

namespace {

const std::string BAD_CONFIG_IN_STORAGE_ERROR =
        "Current library config is unsupported in this version of ArcticDB. "
        "Please ask an administrator for your storage to follow the instructions in "
        "https://github.com/man-group/ArcticDB/blob/master/docs/mkdocs/docs/technical/upgrade_storage.md";

const std::string BAD_CONFIG_IN_ATTEMPTED_WRITE = "Attempting to write forbidden storage config. This indicates a "
                                                  "bug in ArcticDB.";

template<typename T>
struct StorageVisitor {
    arcticdb::proto::storage::LibraryConfig& lib_cfg_proto;
    bool override_https;

    void operator()(const T& storage_override) {
        for (auto& storage : *lib_cfg_proto.mutable_storage_by_id()) {
            storage_override.modify_storage_config(storage.second, override_https);
        }
    }
};

void apply_storage_override(
        const StorageOverride& storage_override, arcticdb::proto::storage::LibraryConfig& lib_cfg_proto,
        bool override_https
) {
    util::variant_match(
            storage_override.variant(),
            StorageVisitor<S3Override>{lib_cfg_proto, override_https},
            StorageVisitor<GCPXMLOverride>{lib_cfg_proto, override_https},
            StorageVisitor<AzureOverride>{lib_cfg_proto, override_https},
            StorageVisitor<LmdbOverride>{lib_cfg_proto, override_https},
            [](const std::monostate&) {}
    );
}

bool is_s3_credential_ok(std::string_view cred) { return cred.empty() || cred == s3::USE_AWS_CRED_PROVIDERS_TOKEN; }

bool is_storage_config_ok(
        const arcticdb::proto::storage::VariantStorage& storage, const std::string& error_message, bool throw_on_failure
) {
    bool is_ok{true};
    if (storage.config().Is<arcticdb::proto::s3_storage::Config>()) {
        arcticdb::proto::s3_storage::Config s3_storage;
        storage.config().UnpackTo(&s3_storage);
        is_ok = is_s3_credential_ok(s3_storage.credential_key()) && is_s3_credential_ok(s3_storage.credential_name());
    }
    if (storage.config().Is<arcticdb::proto::azure_storage::Config>()) {
        arcticdb::proto::azure_storage::Config azure_storage;
        storage.config().UnpackTo(&azure_storage);
        is_ok = azure_storage.endpoint().empty();
    }

    if (is_ok) {
        return true;
    } else if (throw_on_failure) {
        internal::raise<ErrorCode::E_STORED_CONFIG_ERROR>(error_message);
    } else {
        return false;
    }
}

} // anonymous namespace

LibraryManager::LibraryManager(const std::shared_ptr<storage::Library>& library) :
    store_(std::make_shared<async::AsyncStore<util::SysClock>>(
            library, codec::default_lz4_codec(), encoding_version(library->config())
    )),
    open_libraries_(ConfigsMap::instance()->get_int("LibraryManager.CacheSize", 100)) {}

void LibraryManager::write_library_config(
        const py::object& lib_cfg, const LibraryPath& path, const StorageOverride& storage_override, const bool validate
) const {
    arcticdb::proto::storage::LibraryConfig lib_cfg_proto;
    python_util::pb_from_python(lib_cfg, lib_cfg_proto);

    apply_storage_override(storage_override, lib_cfg_proto, false);

    write_library_config_internal(lib_cfg_proto, path, validate);
}

void LibraryManager::write_library_config_internal(
        const arcticdb::proto::storage::LibraryConfig& lib_cfg_proto, const LibraryPath& path, bool validate
) const {
    SegmentInMemory segment;
    segment.descriptor().set_index({IndexDescriptor::Type::ROWCOUNT, 0UL});
    google::protobuf::Any output = {};

    output.PackFrom(lib_cfg_proto);

    if (validate) {
        for (const auto& storage : lib_cfg_proto.storage_by_id()) {
            is_storage_config_ok(storage.second, BAD_CONFIG_IN_ATTEMPTED_WRITE, true);
        }
    }

    segment.set_metadata(std::move(output));

    auto library_name = path.to_delim_path();
    verify_library_path_on_write(store_.get(), library_name);

    store_->write_sync(entity::KeyType::LIBRARY_CONFIG, StreamId(library_name), std::move(segment));
}

void LibraryManager::modify_library_option(
        const arcticdb::storage::LibraryPath& path,
        std::variant<ModifiableLibraryOption, ModifiableEnterpriseLibraryOption> option, LibraryOptionValue new_value
) const {
    auto get_bool = [&option](LibraryOptionValue value) {
        if (!std::holds_alternative<bool>(value)) {
            throw UnsupportedLibraryOptionValue(fmt::format(
                    "{} only supports bool values but received {}. Not changing library option.", option, value
            ));
        }
        return std::get<bool>(value);
    };
    auto get_positive_int = [&option](LibraryOptionValue value) {
        if (!std::holds_alternative<int64_t>(value) || std::get<int64_t>(value) <= 0) {
            throw UnsupportedLibraryOptionValue(fmt::format(
                    "{} only supports positive int values but received {}. Not changing library option.", option, value
            ));
        }
        return std::get<int64_t>(value);
    };
    auto get_non_negative_int = [&option](LibraryOptionValue value, int64_t max_val) {
        if (!std::holds_alternative<int64_t>(value) || std::get<int64_t>(value) < 0 ||
            std::get<int64_t>(value) > max_val) {
            throw UnsupportedLibraryOptionValue(fmt::format(
                    "{} only supports int values in [0, {}] but received {}. Not changing library option.",
                    option,
                    max_val,
                    value
            ));
        }
        return std::get<int64_t>(value);
    };
    auto get_string = [&option](const LibraryOptionValue& value) -> const std::string& {
        if (!std::holds_alternative<std::string>(value)) {
            throw UnsupportedLibraryOptionValue(fmt::format(
                    "{} only supports string values but received {}. Not changing library option.", option, value
            ));
        }
        return std::get<std::string>(value);
    };
    auto trim = [](std::string s) {
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
            s.erase(s.begin());
        }
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
            s.pop_back();
        }
        return s;
    };

    auto apply_option = [&](arcticdb::proto::storage::LibraryConfig& config) {
        auto mutable_write_options = config.mutable_lib_desc()->mutable_version()->mutable_write_options();
        util::variant_match(
                option,
                [&](const ModifiableLibraryOption& opt) {
                    switch (opt) {
                    case ModifiableLibraryOption::DEDUP:
                        mutable_write_options->set_de_duplication(get_bool(new_value));
                        break;
                    case ModifiableLibraryOption::ROWS_PER_SEGMENT:
                        mutable_write_options->set_segment_row_size(get_positive_int(new_value));
                        break;
                    case ModifiableLibraryOption::COLUMNS_PER_SEGMENT:
                        mutable_write_options->set_column_group_size(get_positive_int(new_value));
                        break;
                    case ModifiableLibraryOption::RECURSIVE_NORMALIZERS:
                        mutable_write_options->set_recursive_normalizers(get_bool(new_value));
                        break;
                    case ModifiableLibraryOption::PRUNE_PREVIOUS_VERSIONS:
                        mutable_write_options->set_prune_previous_version(get_bool(new_value));
                        break;
                    default:
                        throw UnsupportedLibraryOptionValue(fmt::format("Invalid library option: {}", opt));
                    }
                },
                [&](const ModifiableEnterpriseLibraryOption& opt) {
                    switch (opt) {
                    case ModifiableEnterpriseLibraryOption::REPLICATION:
                        mutable_write_options->mutable_sync_passive()->set_enabled(get_bool(new_value));
                        break;
                    case ModifiableEnterpriseLibraryOption::BACKGROUND_DELETION:
                        mutable_write_options->set_delayed_deletes(get_bool(new_value));
                        break;
                    case ModifiableEnterpriseLibraryOption::SEQUENCER_URL: {
                        const std::string raw = trim(get_string(new_value));
                        if (raw.empty()) {
                            mutable_write_options->clear_sequencer_url();
                            break;
                        }
                        std::stringstream ss(raw);
                        std::string item;
                        std::string normalized;
                        while (std::getline(ss, item, ',')) {
                            item = trim(std::move(item));
                            if (item.empty()) {
                                continue;
                            }
                            for (unsigned char ch : item) {
                                if (ch <= 0x20 || ch == 0x7f) {
                                    throw UnsupportedLibraryOptionValue(
                                            "SEQUENCER_URL must not contain whitespace or control characters. Not changing library option."
                                    );
                                }
                            }
                            std::string lower_prefix = item.substr(0, std::min<size_t>(item.size(), 8));
                            std::transform(lower_prefix.begin(), lower_prefix.end(), lower_prefix.begin(),
                                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                            size_t scheme_len = 0;
                            if (lower_prefix.starts_with("https://")) {
                                scheme_len = 8;
                                item.replace(0, 8, "https://");
                            } else if (lower_prefix.starts_with("http://")) {
                                scheme_len = 7;
                                item.replace(0, 7, "http://");
                            } else {
                                throw UnsupportedLibraryOptionValue(
                                        "SEQUENCER_URL entries must start with http:// or https://. Not changing library option."
                                );
                            }
                            const std::string rest = item.substr(scheme_len);
                            if (rest.empty() || rest.find('@') != std::string::npos ||
                                rest.find('?') != std::string::npos || rest.find('#') != std::string::npos ||
                                rest.find('\\') != std::string::npos) {
                                throw UnsupportedLibraryOptionValue(
                                        "SEQUENCER_URL entries must not contain userinfo, query strings, fragments, or backslashes. Not changing library option."
                                );
                            }
                            const size_t slash_pos = rest.find('/');
                            const std::string authority =
                                    (slash_pos == std::string::npos) ? rest : rest.substr(0, slash_pos);
                            if (authority.empty()) {
                                throw UnsupportedLibraryOptionValue(
                                        "SEQUENCER_URL entry missing host. Not changing library option."
                                );
                            }
                            if (!normalized.empty()) {
                                normalized.push_back(',');
                            }
                            normalized += item;
                        }
                        if (normalized.empty()) {
                            throw UnsupportedLibraryOptionValue(
                                    "SEQUENCER_URL must contain at least one valid http:// or https:// endpoint (or be empty to clear). Not changing library option."
                            );
                        }
                        mutable_write_options->set_sequencer_url(normalized);
                        break;
                    }
                    case ModifiableEnterpriseLibraryOption::SEQUENCER_INLINE_THRESHOLD_BYTES: {
                        constexpr int64_t kMaxInlineBytes = 16LL * 1024LL * 1024LL;
                        const int64_t v = get_non_negative_int(new_value, kMaxInlineBytes);
                        mutable_write_options->set_sequencer_inline_threshold_bytes(static_cast<uint32_t>(v));
                        break;
                    }
                    case ModifiableEnterpriseLibraryOption::SEQUENCER_CONFLICT_POLICY: {
                        std::string pol = trim(get_string(new_value));
                        std::transform(pol.begin(), pol.end(), pol.begin(), [](unsigned char c) {
                            return static_cast<char>(std::tolower(c));
                        });
                        if (pol.empty() || pol == "unspecified" || pol == "conflict_policy_unspecified") {
                            mutable_write_options->set_sequencer_conflict_policy(0);
                        } else if (pol == "strict_append" || pol == "strict") {
                            mutable_write_options->set_sequencer_conflict_policy(1);
                        } else if (pol == "reject" || pol == "overlap_fail") {
                            mutable_write_options->set_sequencer_conflict_policy(2);
                        } else if (pol == "overwrite_lww" || pol == "lww" || pol == "overwrite") {
                            mutable_write_options->set_sequencer_conflict_policy(3);
                        } else if (pol == "keep_both") {
                            mutable_write_options->set_sequencer_conflict_policy(4);
                        } else {
                            throw UnsupportedLibraryOptionValue(fmt::format(
                                    "SEQUENCER_CONFLICT_POLICY must be one of '', 'strict_append', 'reject', "
                                    "'overwrite_lww', 'keep_both' (got '{}'). Not changing library option.",
                                    pol
                            ));
                        }
                        break;
                    }
                    default:
                        throw UnsupportedLibraryOptionValue(fmt::format("Invalid library option: {}", opt));
                    }
                }
        );
    };

    const bool use_cas = store_->supports_cas_writes();
    constexpr int kMaxCasRetries = 5;
    for (int attempt = 0; attempt < (use_cas ? kMaxCasRetries : 1); ++attempt) {
        // We don't apply a storage override to keep modification backwards compatible.
        arcticdb::proto::storage::LibraryConfig config;
        std::optional<std::string> etag;
        if (use_cas) {
            auto [key, segment_in_memory, read_etag] =
                    store_->read_with_etag_sync(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG});
            (void)key;
            auto any = segment_in_memory.metadata();
            any->UnpackTo(&config);
            etag = std::move(read_etag);
        } else {
            config = get_config_internal(path, std::nullopt);
        }

        apply_option(config);

        if (use_cas && etag.has_value()) {
            SegmentInMemory segment;
            segment.descriptor().set_index({IndexDescriptor::Type::ROWCOUNT, 0UL});
            google::protobuf::Any output = {};
            output.PackFrom(config);
            segment.set_metadata(std::move(output));
            auto library_name = path.to_delim_path();
            verify_library_path_on_write(store_.get(), library_name);
            try {
                (void)store_->write_if_match_sync(
                        entity::KeyType::LIBRARY_CONFIG, StreamId(library_name), std::move(segment), *etag
                );
                return;
            } catch (const AtomicOperationFailedException&) {
                if (attempt + 1 >= kMaxCasRetries) {
                    throw;
                }
                continue;
            }
        } else {
            // We use validate=false because we don't want to validate old pre v3.0.0 configs
            write_library_config_internal(config, path, false);
            return;
        }
    }
}

py::object LibraryManager::get_library_config(const LibraryPath& path, const StorageOverride& storage_override) const {
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, {storage_override});

    return arcticdb::python_util::pb_to_python(config);
}

py::object LibraryManager::get_unaltered_library_config(const LibraryPath& path) const {
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, std::nullopt);

    return arcticdb::python_util::pb_to_python(config);
}

bool LibraryManager::is_library_config_ok(const LibraryPath& path, bool throw_on_failure) const {
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, {StorageOverride{}});
    return std::all_of(
            config.storage_by_id().begin(),
            config.storage_by_id().end(),
            [&throw_on_failure](const auto& storage) {
                return is_storage_config_ok(storage.second, BAD_CONFIG_IN_STORAGE_ERROR, throw_on_failure);
            }
    );
}

void LibraryManager::remove_library_config(const LibraryPath& path) const {
    store_->remove_key(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG}).wait();
}

LibraryManager::LibraryWithConfig LibraryManager::get_library(
        const LibraryPath& path, const StorageOverride& storage_override, const bool ignore_cache,
        const NativeVariantStorage& native_storage_config
) {
    if (!ignore_cache) {
        // Check global cache first, important for LMDB to only open once from a given process
        std::lock_guard<std::mutex> lock{open_libraries_mutex_};
        if (auto cached = open_libraries_.get(path); cached) {
            return cached.value();
        }
    }

    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, {storage_override});
    auto storages = create_storages(path, OpenMode::DELETE, config.storage_by_id(), native_storage_config);
    auto lib = std::make_shared<Library>(path, std::move(storages), config.lib_desc().version());
    open_libraries_.put(path, {config, lib});

    return {config, lib};
}

void LibraryManager::cleanup_library_if_open(const LibraryPath& path) {
    std::lock_guard<std::mutex> lock{open_libraries_mutex_};
    if (auto library = open_libraries_.get(path); library) {
        library.value().library->cleanup();
        open_libraries_.remove(path);
    }
}

std::vector<LibraryPath> LibraryManager::get_library_paths() const {
    std::vector<LibraryPath> ids;
    store_->iterate_type(entity::KeyType::LIBRARY_CONFIG, [&ids](const VariantKey&& key) {
        const auto& k = std::get<entity::RefKey>(key);
        const auto& lp = std::get<std::string>(k.id());
        ids.emplace_back(lp, '.');
    });

    return ids;
}

bool LibraryManager::has_library(const LibraryPath& path) const {
    {
        std::lock_guard<std::mutex> lock{open_libraries_mutex_};
        if (auto cached = open_libraries_.get(path); cached) {
            return true;
        }
    }

    return store_->key_exists_sync(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG});
}

arcticdb::proto::storage::LibraryConfig LibraryManager::get_config_internal(
        const LibraryPath& path, const std::optional<StorageOverride>& storage_override
) const {
    auto [key, segment_in_memory] =
            store_->read_sync(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG});

    auto any = segment_in_memory.metadata();
    arcticdb::proto::storage::LibraryConfig lib_cfg_proto;
    any->UnpackTo(&lib_cfg_proto);
    if (storage_override.has_value()) {
        apply_storage_override(storage_override.value(), lib_cfg_proto, true);
    }
    return lib_cfg_proto;
}

} // namespace arcticdb::storage
