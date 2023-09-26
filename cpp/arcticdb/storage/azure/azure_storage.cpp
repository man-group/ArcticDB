/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#define ARCTICDB_AZURE_STORAGE_H_
#include <arcticdb/storage/azure/azure_storage-inl.hpp>


#include <arcticdb/log/log.hpp>
#include <azure/core/http/curl_transport.hpp>

namespace arcticdb::storage::azure{

using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;


AzureStorage::AzureStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Storage(library_path, mode),
    container_client_(BlobContainerClient::CreateFromConnectionString(conf.endpoint(), conf.container_name(), get_client_options(conf))),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    request_timeout_(conf.request_timeout() == 0 ? 60000 : conf.request_timeout()){
        if (conf.ca_cert_path().empty())
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using default CA cert path");
        else
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "CA cert path: {}", conf.ca_cert_path());
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Connecting to Azure Blob Storage: {} Container: {}", conf.endpoint(), conf.container_name());

        if (!conf.prefix().empty()) {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Azure prefix found, using: {}", conf.prefix());
            auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
            root_folder_ = object_store_utils::get_root_folder(prefix_path);
        } else 
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Azure prefix not found, will use {}", root_folder_);
        unsigned int max_connections = conf.max_connections() == 0 ? ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", 16) : conf.max_connections();
        upload_option_.TransferOptions.Concurrency = max_connections;
        download_option_.TransferOptions.Concurrency = max_connections;
}

Azure::Storage::Blobs::BlobClientOptions AzureStorage::get_client_options(const Config &conf) {
    BlobClientOptions client_options;
    if (!conf.ca_cert_path().empty()) {//WARNING: Setting ca_cert_path will force Azure sdk uses libcurl as backend support, instead of winhttp
        Azure::Core::Http::CurlTransportOptions curl_transport_options;
        curl_transport_options.CAInfo = conf.ca_cert_path();
        client_options.Transport.Transport = std::make_shared<Azure::Core::Http::CurlTransport>(curl_transport_options);
    }
    return client_options;
}

} // namespace arcticdb::storage::azure
