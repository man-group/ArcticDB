#pragma once

#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

#include "http_client.hpp"
#include "http_request.hpp"
#include "arcticdb/log/log.hpp"
#include <csignal>

namespace arcticdb {

class ArcticHttpClientFactory : public Aws::Http::HttpClientFactory {
        std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration) const override
        {
#if ENABLE_WINDOWS_CLIENT
#if ENABLE_WINDOWS_IXML_HTTP_REQUEST_2_CLIENT
                return Aws::MakeShared<IXmlHttpRequest2HttpClient>(HTTP_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);
#else
                switch (clientConfiguration.httpLibOverride)
                {
                    case TransferLibType::WIN_INET_CLIENT:
                        return Aws::MakeShared<WinINetSyncHttpClient>(HTTP_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);

                    default:
                        return Aws::MakeShared<WinHttpSyncHttpClient>(HTTP_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);
                }
#endif // ENABLE_WINDOWS_IXML_HTTP_REQUEST_2_CLIENT
#endif
            return Aws::MakeShared<ArcticCurlHttpClient>("ARCTICDB_HTTP_CLIENT_FACTORY_ALLOCATION_TAG", clientConfiguration);
        }

        std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(const Aws::String &uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory &streamFactory) const override
        {
            return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
        }

        std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory) const override
        {
            auto request = Aws::MakeShared<ArcticHttpRequest>("ARCTICDB_HTTP_CLIENT_FACTORY_ALLOCATION_TAG", uri, method);
            request->SetResponseStreamFactory(streamFactory);

            return request;
        }
    };
} // namespace arcticdb