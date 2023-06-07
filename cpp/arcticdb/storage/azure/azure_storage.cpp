/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/azure/azure_storage.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb::storage::azure{

using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

AzureStorage::AzureStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Parent(library_path, mode),
    blob_container_url_(conf.connect_to_azurite() ? 
                            fmt::format("{}/{}/{}", conf.endpoint(), conf.credential_name(), conf.container_name()) :
                            fmt::format("{}://{}.{}/{}", conf.https() ? "https" : "http", conf.credential_name(), conf.endpoint(), conf.container_name())),
    container_client_(BlobContainerClient(blob_container_url_, get_azure_credentials(conf))),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    connect_to_azurite_(conf.connect_to_azurite()),
    request_timeout_(conf.request_timeout() == 0 ? 60000 : conf.request_timeout()){
        log::version().info("Connecting to Azure Blob Storage: {}", blob_container_url_);

        if (!conf.prefix().empty()) {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Azure prefix found, using: {}", conf.prefix());
            auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
            root_folder_ = object_store_utils::get_root_folder(prefix_path);
        } else {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Azure prefix not found, will use {}", root_folder_);
        }
        unsigned int max_connections = conf.max_connections() == 0 ? ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", 16) : conf.max_connections();
        upload_option_.TransferOptions.Concurrency = max_connections;
        download_option_.TransferOptions.Concurrency = max_connections;
}

} // namespace arcticdb::storage::azure