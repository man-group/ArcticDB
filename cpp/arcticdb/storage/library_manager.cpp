#include <arcticdb/storage/library_manager.hpp>

#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>

#include <folly/Range.h>
#include <filesystem>

namespace arcticdb::storage {

namespace {

const std::string BAD_CONFIG_IN_STORAGE_ERROR = "Current library config is unsupported in this version of ArcticDB. "
                                                "Please ask an administrator for your storage to follow the instructions in "
                                                "https://github.com/man-group/ArcticDB/blob/master/docs/mkdocs/docs/technical/upgrade_storage.md";

const std::string BAD_CONFIG_IN_ATTEMPTED_WRITE = "Attempting to write forbidden storage config. This indicates a "
                                                  "bug in ArcticDB.zz";

template<typename T>
struct StorageVisitor {
    arcticdb::proto::storage::LibraryConfig& lib_cfg_proto;

    void operator()(const T& storage_override) {
        for(auto& storage: *lib_cfg_proto.mutable_storage_by_id()){
            storage_override.modify_storage_config(storage.second);
        }
    }
};

void apply_storage_override(const StorageOverride& storage_override,
                                   arcticdb::proto::storage::LibraryConfig& lib_cfg_proto) {
    util::variant_match(
            storage_override.variant(),
            StorageVisitor<S3Override>{lib_cfg_proto},
            StorageVisitor<AzureOverride>{lib_cfg_proto},
            StorageVisitor<LmdbOverride>{lib_cfg_proto},
            [] (const std::monostate&) {});
}

bool is_s3_credential_ok(std::string_view cred) {
    return cred.empty() || cred == s3::USE_AWS_CRED_PROVIDERS_TOKEN;
}

bool is_storage_config_ok(const arcticdb::proto::storage::VariantStorage& storage, const std::string& error_message, bool throw_on_failure) {
    bool is_ok{true};
    if(storage.config().Is<arcticdb::proto::s3_storage::Config>()) {
        arcticdb::proto::s3_storage::Config s3_storage;
        storage.config().UnpackTo(&s3_storage);
        is_ok = is_s3_credential_ok(s3_storage.credential_key()) && is_s3_credential_ok(s3_storage.credential_name());
    }
    if(storage.config().Is<arcticdb::proto::azure_storage::Config>()) {
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
            store_(std::make_shared<async::AsyncStore<util::SysClock>>(library, codec::default_lz4_codec(),
                    encoding_version(library->config()))){
}

void LibraryManager::write_library_config(const py::object& lib_cfg, const LibraryPath& path, const StorageOverride& storage_override,
                          const bool validate) const {
    SegmentInMemory segment;

    arcticdb::proto::storage::LibraryConfig lib_cfg_proto;
    google::protobuf::Any output = {};
    python_util::pb_from_python(lib_cfg, lib_cfg_proto);

    apply_storage_override(storage_override, lib_cfg_proto);

    output.PackFrom(lib_cfg_proto);

    if (validate) {
        for (const auto &storage: lib_cfg_proto.storage_by_id()) {
            is_storage_config_ok(storage.second, BAD_CONFIG_IN_ATTEMPTED_WRITE, true);
        }
    }

    segment.set_metadata(std::move(output));

    store_->write_sync(
            entity::KeyType::LIBRARY_CONFIG,
            StreamId(path.to_delim_path()),
            std::move(segment)
    );
}

py::object LibraryManager::get_library_config(const LibraryPath& path, const StorageOverride& storage_override) const {
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, storage_override);

    return arcticdb::python_util::pb_to_python(config);
}

bool LibraryManager::is_library_config_ok(const LibraryPath& path, bool throw_on_failure) const {
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, StorageOverride{});
    return std::all_of(config.storage_by_id().begin(), config.storage_by_id().end(), [&throw_on_failure](const auto& storage) {
       return is_storage_config_ok(storage.second, BAD_CONFIG_IN_STORAGE_ERROR, throw_on_failure);
    });
}

void LibraryManager::remove_library_config(const LibraryPath& path) const {
    store_->remove_key(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG}).wait();
}

std::shared_ptr<Library> LibraryManager::get_library(const LibraryPath& path, const StorageOverride& storage_override) const {
    arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, storage_override);

    std::vector<arcticdb::proto::storage::VariantStorage> st;
    for(const auto& storage: config.storage_by_id()){
        st.emplace_back(storage.second);
    }
    auto storages = create_storages(path, OpenMode::DELETE, st);

    return std::make_shared<Library>(path, std::move(storages), config.lib_desc().version());
}

std::vector<LibraryPath> LibraryManager::get_library_paths() const {
    std::vector<LibraryPath> ids;
    store_->iterate_type(entity::KeyType::LIBRARY_CONFIG, [&ids](const VariantKey &&key) {
                             const auto& k = std::get<entity::RefKey>(key);
                             const auto& lp = std::get<std::string>(k.id());
                             ids.emplace_back(lp, '.');
                         }
    );

    return ids;
}

bool LibraryManager::has_library(const LibraryPath& path) const {
    return store_->key_exists_sync(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG});
}

arcticdb::proto::storage::LibraryConfig LibraryManager::get_config_internal(const LibraryPath& path, const StorageOverride& storage_override) const {
    auto [key, segment_in_memory] = store_->read_sync(
            RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG}
    );

    auto any = segment_in_memory.metadata();
    arcticdb::proto::storage::LibraryConfig lib_cfg_proto;
    any->UnpackTo(&lib_cfg_proto);
    apply_storage_override(storage_override, lib_cfg_proto);
    return lib_cfg_proto;
}

}  // arcticdb::storage
