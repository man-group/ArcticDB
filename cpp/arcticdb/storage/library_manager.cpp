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
    // We don't apply a storage override to keep modification backwards compatible.
    // Before v3.0.0 we didn't use storage overrides and applying one when modifying options would break readers older
    // than v3.0.0.
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, std::nullopt);
    auto mutable_write_options = config.mutable_lib_desc()->mutable_version()->mutable_write_options();

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

    util::variant_match(
            option,
            [&](const ModifiableLibraryOption& option) {
                switch (option) {
                case ModifiableLibraryOption::DEDUP:
                    mutable_write_options->set_de_duplication(get_bool(new_value));
                    break;
                case ModifiableLibraryOption::ROWS_PER_SEGMENT:
                    mutable_write_options->set_segment_row_size(get_positive_int(new_value));
                    break;
                case ModifiableLibraryOption::COLUMNS_PER_SEGMENT:
                    mutable_write_options->set_column_group_size(get_positive_int(new_value));
                    break;
                default:
                    throw UnsupportedLibraryOptionValue(fmt::format("Invalid library option: {}", option));
                }
            },
            [&](const ModifiableEnterpriseLibraryOption& option) {
                switch (option) {
                case ModifiableEnterpriseLibraryOption::REPLICATION:
                    mutable_write_options->mutable_sync_passive()->set_enabled(get_bool(new_value));
                    break;
                case ModifiableEnterpriseLibraryOption::BACKGROUND_DELETION:
                    mutable_write_options->set_delayed_deletes(get_bool(new_value));
                    break;
                default:
                    throw UnsupportedLibraryOptionValue(fmt::format("Invalid library option: {}", option));
                }
            }
    );

    // We use validate=false because we don't want to validate old pre v3.0.0 configs
    write_library_config_internal(config, path, false);
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
