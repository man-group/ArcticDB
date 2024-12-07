/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <arcticdb/storage/s3/http_client.hpp>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/crypto/Hash.h>
#include <aws/core/utils/Outcome.h>
#include <cassert>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/s3/http_request.hpp>
#include <arcticdb/storage/s3/http_response.hpp>
#include <arcticdb/util/timer.hpp>

#include <algorithm>

using namespace Aws::Client;
using namespace Aws::Http;
using namespace Aws::Http::Standard;
using namespace Aws::Utils;
using namespace Aws::Utils::Logging;
using namespace Aws::Monitoring;

namespace arcticdb {

static const char* CURL_HTTP_CLIENT_TAG = "ArcticdbCurlHttpClient";

struct CurlWriteCallbackContext {
    CurlWriteCallbackContext(
        const ArcticCurlHttpClient *client,
        HttpRequest *request,
        HttpResponse *response) :
        m_client(client),
        m_request(request),
        m_response(response),
        m_numBytesResponseReceived(0) {}

    const ArcticCurlHttpClient *m_client;
    HttpRequest *m_request;
    HttpResponse *m_response;
    int64_t m_numBytesResponseReceived;
};

struct CurlReadCallbackContext {
    CurlReadCallbackContext(const ArcticCurlHttpClient *client,
                            CURL *curlHandle,
                            HttpRequest *request) :
        m_client(client),
        m_curlHandle(curlHandle),
        m_request(request),
        m_chunkEnd(false) {}

    const ArcticCurlHttpClient *m_client;
    CURL *m_curlHandle;
    HttpRequest *m_request;
    bool m_chunkEnd;
};

static int64_t GetContentLengthFromHeader(CURL *connectionHandle,
                                          bool& hasContentLength) {
#if LIBCURL_VERSION_NUM >= 0x073700  // 7.55.0
    curl_off_t contentLength = {};
    CURLcode res = curl_easy_getinfo(connectionHandle, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &contentLength);
#else
    double contentLength = {};
    CURLcode res = curl_easy_getinfo(connectionHandle, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &contentLength);
#endif
    hasContentLength = (res == CURLE_OK) && (contentLength != -1);
    return hasContentLength ? static_cast<int64_t>(contentLength) : -1;
}

static size_t WriteData(char *ptr, size_t size, size_t nmemb, void *userdata) {
    if (ptr) {
        auto *context = reinterpret_cast<CurlWriteCallbackContext *>(userdata);

        const ArcticCurlHttpClient *client = context->m_client;
        if (!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled()) {
            return 0;
        }

        HttpResponse *response = context->m_response;
        size_t sizeToWrite = size * nmemb;

        for (const auto& hashIterator : context->m_request->GetResponseValidationHashes()) {
            hashIterator.second->Update(reinterpret_cast<unsigned char *>(ptr), sizeToWrite);
        }

        if (response->GetResponseBody().fail()) {
            const auto& ref = response->GetResponseBody();
            log::storage().error("Response output stream in bad state (eof: {}, bad: {})", ref.eof(), ref.bad());
            return 0;
        }

        size_t cur = response->GetResponseBody().tellp();
        if (response->GetResponseBody().fail()) {
            const auto& ref = response->GetResponseBody();
            log::storage().error("Unable to query response output position (eof: {}, bad: {})", ref.eof(), ref.bad());
            return 0;
        }

        response->GetResponseBody().write(ptr, static_cast<std::streamsize>(sizeToWrite));
        if (response->GetResponseBody().fail()) {
            const auto& ref = response->GetResponseBody();
            log::storage().error("Failed to write {}/{} B response at {} (eof: {}, bad: {})", size, sizeToWrite, cur, ref.eof(), ref.bad());
            return 0;
        }
        
        if (context->m_request->IsEventStreamRequest() && !response->HasHeader(Aws::Http::X_AMZN_ERROR_TYPE)) {
            response->GetResponseBody().flush();
            if (response->GetResponseBody().fail()) {
                const auto& ref = response->GetResponseBody();
                log::storage().error("Failed to flush event response (eof: {}, bad: {})", ref.eof(), ref.bad());
                return 0;
            }
        }
        auto& receivedHandler = context->m_request->GetDataReceivedEventHandler();
        if (receivedHandler) {
            receivedHandler(context->m_request, context->m_response, static_cast<long long>(sizeToWrite));
        }

        ARCTICDB_RUNTIME_DEBUG(log::storage(), "{} bytes written to response.", sizeToWrite);
        context->m_numBytesResponseReceived += static_cast<int64_t>(sizeToWrite);
        return sizeToWrite;
    }
    return 0;
}

std::pair<std::string_view, std::string_view> split_string_view(std::string_view str, char delimiter) {
    size_t pos = str.find(delimiter);
    if (pos == std::string_view::npos) {
        return {str, std::string_view()};
    }
    std::string_view first = str.substr(0, pos);
    std::string_view second = str.substr(pos + 1);
    return {first, second};
}

static bool is_space(int ch) {
    if (ch < -1 || ch > 255) {
        return false;
    }
    return std::isspace(ch) != 0;
}

std::string_view trim(std::string_view str) {
    size_t start = 0;
    while (start < str.size() && is_space(str[start])) {
        ++start;
    }

    if (start == str.size())
        return std::string_view();

    size_t end = str.size() - 1;
    while (end > start && is_space(str[end])) {
        --end;
    }

    return str.substr(start, end - start + 1);
}

static size_t WriteHeader(char *ptr, size_t size, size_t nmemb, void *userdata) {
    if (ptr) {
        auto *context = reinterpret_cast<CurlWriteCallbackContext *>(userdata);
        HttpResponse *response = context->m_response;
        std::string_view strv(ptr, std::strlen(ptr));
        auto key_value_pair = split_string_view(strv, ':');
        if (!key_value_pair.second.empty()) {
            response->AddHeader(std::string{trim(key_value_pair.first)}, std::string{trim(key_value_pair.second)});
        }

        return size * nmemb;
    }
    return 0;
}

static size_t ReadBody(char *ptr, size_t size, size_t nmemb, void *userdata, bool isStreaming) {
    auto *context = reinterpret_cast<CurlReadCallbackContext *>(userdata);
    if (context == nullptr) {
        return 0;
    }

    const ArcticCurlHttpClient *client = context->m_client;
    if (!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled()) {
        return CURL_READFUNC_ABORT;
    }

    HttpRequest *request = context->m_request;
    const std::shared_ptr<Aws::IOStream>& ioStream = request->GetContentBody();

    size_t amountToRead = size * nmemb;
    bool isAwsChunked = request->HasHeader(Aws::Http::CONTENT_ENCODING_HEADER) &&
        request->GetHeaderValue(Aws::Http::CONTENT_ENCODING_HEADER) == Aws::Http::AWS_CHUNKED_VALUE;
    // aws-chunk = hex(chunk-size) + CRLF + chunk-data + CRLF
    // Needs to reserve bytes of sizeof(hex(chunk-size)) + sizeof(CRLF) + sizeof(CRLF)
    if (isAwsChunked) {
        Aws::String amountToReadHexString = Aws::Utils::StringUtils::ToHexString(amountToRead);
        amountToRead -= (amountToReadHexString.size() + 4);
    }

    if (ioStream != nullptr && amountToRead > 0) {
        if (isStreaming) {
            if (ioStream->readsome(ptr, static_cast<std::streamsize>(amountToRead)) == 0 && !ioStream->eof()) {
                return CURL_READFUNC_PAUSE;
            }
        } else {
            ioStream->read(ptr, static_cast<std::streamsize>(amountToRead));
        }
        auto amountRead = static_cast<size_t>(ioStream->gcount());

        if (isAwsChunked) {
            if (amountRead > 0) {
                if (request->GetRequestHash().second != nullptr) {
                    request->GetRequestHash().second->Update(reinterpret_cast<unsigned char *>(ptr), amountRead);
                }

                Aws::String hex = Aws::Utils::StringUtils::ToHexString(amountRead);
                memmove(ptr + hex.size() + 2, ptr, amountRead);
                memmove(ptr + hex.size() + 2 + amountRead, "\r\n", 2);
                memmove(ptr, hex.c_str(), hex.size());
                memmove(ptr + hex.size(), "\r\n", 2);
                amountRead += hex.size() + 4;
            } else if (!context->m_chunkEnd) {
                Aws::StringStream chunkedTrailer;
                chunkedTrailer << "0\r\n";
                if (request->GetRequestHash().second != nullptr) {
                    chunkedTrailer << "x-amz-checksum-" << request->GetRequestHash().first << ":"
                                   << HashingUtils::Base64Encode(request->GetRequestHash().second->GetHash().GetResult())
                                   << "\r\n";
                }
                chunkedTrailer << "\r\n";
                amountRead = chunkedTrailer.str().size();
                memcpy(ptr, chunkedTrailer.str().c_str(), amountRead);
                context->m_chunkEnd = true;
            }
        }

        auto& sentHandler = request->GetDataSentEventHandler();
        if (sentHandler) {
            sentHandler(request, static_cast<long long>(amountRead));
        }

        return amountRead;
    }

    return 0;
}

static size_t ReadBodyStreaming(char *ptr, size_t size, size_t nmemb, void *userdata) {
    return ReadBody(ptr, size, nmemb, userdata, true);
}

static size_t ReadBodyFunc(char *ptr, size_t size, size_t nmemb, void *userdata) {
    return ReadBody(ptr, size, nmemb, userdata, false);
}

static size_t SeekBody(void *userdata, curl_off_t offset, int origin) {
    auto *context = reinterpret_cast<CurlReadCallbackContext *>(userdata);
    if (context == nullptr) {
        return CURL_SEEKFUNC_FAIL;
    }

    const ArcticCurlHttpClient *client = context->m_client;
    if (!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled()) {
        return CURL_SEEKFUNC_FAIL;
    }

    HttpRequest *request = context->m_request;
    const std::shared_ptr<Aws::IOStream>& ioStream = request->GetContentBody();

    std::ios_base::seekdir dir;
    switch (origin) {
    case SEEK_SET:dir = std::ios_base::beg;
        break;
    case SEEK_CUR:dir = std::ios_base::cur;
        break;
    case SEEK_END:dir = std::ios_base::end;
        break;
    default:return CURL_SEEKFUNC_FAIL;
    }

    ioStream->clear();
    ioStream->seekg(offset, dir);
    if (ioStream->fail()) {
        return CURL_SEEKFUNC_CANTSEEK;
    }

    return CURL_SEEKFUNC_OK;
}
#if LIBCURL_VERSION_NUM >= 0x072000 // 7.32.0
static int CurlProgressCallback(void *userdata, curl_off_t, curl_off_t, curl_off_t, curl_off_t)
#else
static int CurlProgressCallback(void *userdata, double, double, double, double)
#endif
{
    auto *context = reinterpret_cast<CurlReadCallbackContext *>(userdata);

    const std::shared_ptr<Aws::IOStream>& ioStream = context->m_request->GetContentBody();
    if (ioStream->eof()) {
        curl_easy_pause(context->m_curlHandle, CURLPAUSE_CONT);
        return 0;
    }
    char output[1];
    if (ioStream->readsome(output, 1) > 0) {
        ioStream->unget();
        if (!ioStream->good()) {
            log::storage().warn("Input stream failed to perform unget().");
        }
        curl_easy_pause(context->m_curlHandle, CURLPAUSE_CONT);
    }

    return 0;
}

void SetOptCodeForHttpMethod(CURL *requestHandle, const std::shared_ptr<HttpRequest>& request) {
    switch (request->GetMethod()) {
    case HttpMethod::HTTP_GET:curl_easy_setopt(requestHandle, CURLOPT_HTTPGET, 1L);
        break;
    case HttpMethod::HTTP_POST:
        if (request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)
            && request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") {
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "POST");
        } else {
            curl_easy_setopt(requestHandle, CURLOPT_POST, 1L);
        }
        break;
    case HttpMethod::HTTP_PUT:
        if ((!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)
            || request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") &&
            !request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER)) {
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PUT");
        } else {
#if LIBCURL_VERSION_NUM >= 0x070c01 // 7.12.1
            curl_easy_setopt(requestHandle, CURLOPT_UPLOAD, 1L);
#else
            curl_easy_setopt(requestHandle, CURLOPT_PUT, 1L);
#endif
        }
        break;
    case HttpMethod::HTTP_HEAD:curl_easy_setopt(requestHandle, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(requestHandle, CURLOPT_NOBODY, 1L);
        break;
    case HttpMethod::HTTP_PATCH:
        if ((!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)
            || request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") &&
            !request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER)) {
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
        } else {
            curl_easy_setopt(requestHandle, CURLOPT_POST, 1L);
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
        }

        break;
    case HttpMethod::HTTP_DELETE:curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
        break;
    default:assert(0);
        curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "GET");
        break;
    }
}

std::atomic<bool> ArcticCurlHttpClient::isInit(false);

void ArcticCurlHttpClient::InitGlobalState() {
    if (!isInit) {
        auto curlVersionData = curl_version_info(CURLVERSION_NOW);
        log::storage().warn("Initializing Curl library with version: {}, ssl version: {}", curlVersionData->version, curlVersionData->ssl_version);
        isInit = true;
        curl_global_init(CURL_GLOBAL_ALL);
    }
}

void ArcticCurlHttpClient::CleanupGlobalState() {
    curl_global_cleanup();
}

Aws::String CurlInfoTypeToString(curl_infotype type) {
    switch (type) {
    case CURLINFO_TEXT:return "Text";

    case CURLINFO_HEADER_IN:return "HeaderIn";

    case CURLINFO_HEADER_OUT:return "HeaderOut";

    case CURLINFO_DATA_IN:return "DataIn";

    case CURLINFO_DATA_OUT:return "DataOut";

    case CURLINFO_SSL_DATA_IN:return "SSLDataIn";

    case CURLINFO_SSL_DATA_OUT:return "SSLDataOut";

    default:return "Unknown";
    }
}

int CurlDebugCallback(CURL*, curl_infotype type, char *data, size_t size, void*) {
    if (type == CURLINFO_SSL_DATA_IN || type == CURLINFO_SSL_DATA_OUT) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "CURL ({}) {} bytes", CurlInfoTypeToString(type), size);
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "CURL ({}) data: {} size: {}", CurlInfoTypeToString(type), data, size);
    }

    return 0;
}

ArcticCurlHttpClient::ArcticCurlHttpClient(const ClientConfiguration& clientConfig) :
    Base(),
    m_curlHandleContainer(clientConfig.maxConnections,
                          clientConfig.httpRequestTimeoutMs,
                          clientConfig.connectTimeoutMs,
                          clientConfig.enableTcpKeepAlive,
                          clientConfig.tcpKeepAliveIntervalMs,
                          clientConfig.requestTimeoutMs,
                          clientConfig.lowSpeedLimit,
                          clientConfig.version),
    m_isAllowSystemProxy(clientConfig.allowSystemProxy), m_isUsingProxy(!clientConfig.proxyHost.empty()),
    m_proxyUserName(clientConfig.proxyUserName),
    m_proxyPassword(clientConfig.proxyPassword), m_proxyScheme(SchemeMapper::ToString(clientConfig.proxyScheme)),
    m_proxyHost(clientConfig.proxyHost),
    m_proxySSLCertPath(clientConfig.proxySSLCertPath), m_proxySSLCertType(clientConfig.proxySSLCertType),
    m_proxySSLKeyPath(clientConfig.proxySSLKeyPath), m_proxySSLKeyType(clientConfig.proxySSLKeyType),
    m_proxyKeyPasswd(clientConfig.proxySSLKeyPassword),
    m_proxyPort(clientConfig.proxyPort), m_verifySSL(clientConfig.verifySSL), m_caPath(clientConfig.caPath),
    m_caFile(clientConfig.caFile),
    m_disableExpectHeader(clientConfig.disableExpectHeader),
    m_telemetryProvider(clientConfig.telemetryProvider) {
    if (clientConfig.followRedirects == FollowRedirectsPolicy::NEVER ||
        (clientConfig.followRedirects == FollowRedirectsPolicy::DEFAULT
            && clientConfig.region == Aws::Region::AWS_GLOBAL)) {
        m_allowRedirects = false;
    } else {
        m_allowRedirects = true;
    }
    if (clientConfig.nonProxyHosts.GetLength() > 0) {
        Aws::StringStream ss;
        ss << clientConfig.nonProxyHosts.GetItem(0);
        for (auto i = 1u; i < clientConfig.nonProxyHosts.GetLength(); i++) {
            ss << "," << clientConfig.nonProxyHosts.GetItem(i);
        }
        m_nonProxyHosts = ss.str();
    }
}

std::shared_ptr<HttpResponse> ArcticCurlHttpClient::MakeRequest(const std::shared_ptr<HttpRequest>& request,
                                                          Aws::Utils::RateLimits::RateLimiterInterface*,
                                                          Aws::Utils::RateLimits::RateLimiterInterface*) const {
    URI uri = request->GetUri();
    Aws::String url = uri.GetURIString();
    std::shared_ptr<HttpResponse> response = Aws::MakeShared<ArcticHttpResponse>(CURL_HTTP_CLIENT_TAG, request);
    interval_timer timer;
    timer.start_timer("curl");
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Making request to {}", url);
    struct curl_slist *headers = nullptr;
    Aws::StringStream headerStream;
    auto requestHeaders = std::dynamic_pointer_cast<ArcticHttpRequest>(request)->GetHeaderViews();

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Including headers:");
    for (auto& requestHeader : requestHeaders) {
        headerStream.str("");
        headerStream << requestHeader.first << ": " << requestHeader.second;
        Aws::String headerString = headerStream.str();
        ARCTICDB_RUNTIME_DEBUG(log::storage(), headerString);
        headers = curl_slist_append(headers, headerString.c_str());
    }

    if (!request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER)) {
        headers = curl_slist_append(headers, "transfer-encoding:");
    }

    if (!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)) {
        headers = curl_slist_append(headers, "content-length:");
    }

    if (!request->HasHeader(Aws::Http::CONTENT_TYPE_HEADER)) {
        headers = curl_slist_append(headers, "content-type:");
    }

    // Discard Expect header to avoid using multiple payloads to send a http request (header + body)
    if (m_disableExpectHeader) {
        headers = curl_slist_append(headers, "Expect:");
    }

    CURL *connectionHandle = m_curlHandleContainer.AcquireCurlHandle();

    if (connectionHandle) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Obtained connection handle {}", connectionHandle);

        if (headers) {
            curl_easy_setopt(connectionHandle, CURLOPT_HTTPHEADER, headers);
        }

        CurlWriteCallbackContext writeContext(this, request.get(), response.get());
        CurlReadCallbackContext readContext(this, connectionHandle, request.get());

        SetOptCodeForHttpMethod(connectionHandle, request);

        curl_easy_setopt(connectionHandle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(connectionHandle, CURLOPT_WRITEFUNCTION, WriteData);
        curl_easy_setopt(connectionHandle, CURLOPT_WRITEDATA, &writeContext);
        curl_easy_setopt(connectionHandle, CURLOPT_HEADERFUNCTION, WriteHeader);
        curl_easy_setopt(connectionHandle, CURLOPT_HEADERDATA, &writeContext);

        //we only want to override the default path if someone has explicitly told us to.
        if (!m_caPath.empty()) {
            curl_easy_setopt(connectionHandle, CURLOPT_CAPATH, m_caPath.c_str());
        }
        if (!m_caFile.empty()) {
            curl_easy_setopt(connectionHandle, CURLOPT_CAINFO, m_caFile.c_str());
        }

        // enable the cookie engine without reading any initial cookies.
        curl_easy_setopt(connectionHandle, CURLOPT_COOKIEFILE, "");

        if (m_verifySSL) {
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYHOST, 2L);
            curl_easy_setopt(connectionHandle, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
        } else {
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYHOST, 0L);
        }

        if (m_allowRedirects) {
            curl_easy_setopt(connectionHandle, CURLOPT_FOLLOWLOCATION, 1L);
        } else {
            curl_easy_setopt(connectionHandle, CURLOPT_FOLLOWLOCATION, 0L);
        }

        if(ConfigsMap::instance()->get_int("Curl.Verbose", 0) == 1) {
            curl_easy_setopt(connectionHandle, CURLOPT_VERBOSE, 1);
            curl_easy_setopt(connectionHandle, CURLOPT_DEBUGFUNCTION, CurlDebugCallback);
        }

        if (m_isUsingProxy) {
            Aws::StringStream ss;
            ss << m_proxyScheme << "://" << m_proxyHost;
            curl_easy_setopt(connectionHandle, CURLOPT_PROXY, ss.str().c_str());
            curl_easy_setopt(connectionHandle, CURLOPT_PROXYPORT, (long) m_proxyPort);
            if (!m_proxyUserName.empty() || !m_proxyPassword.empty()) {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXYUSERNAME, m_proxyUserName.c_str());
                curl_easy_setopt(connectionHandle, CURLOPT_PROXYPASSWORD, m_proxyPassword.c_str());
            }
            curl_easy_setopt(connectionHandle, CURLOPT_NOPROXY, m_nonProxyHosts.c_str());

            if (!m_proxySSLCertPath.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLCERT, m_proxySSLCertPath.c_str());
                if (!m_proxySSLCertType.empty())
                {
                    curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLCERTTYPE, m_proxySSLCertType.c_str());
                }
            }
            if (!m_proxySSLKeyPath.empty())
            {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLKEY, m_proxySSLKeyPath.c_str());
                if (!m_proxySSLKeyType.empty())
                {
                    curl_easy_setopt(connectionHandle, CURLOPT_PROXY_SSLKEYTYPE, m_proxySSLKeyType.c_str());
                }
                if (!m_proxyKeyPasswd.empty())
                {
                    curl_easy_setopt(connectionHandle, CURLOPT_PROXY_KEYPASSWD, m_proxyKeyPasswd.c_str());
                }
            }
        } else {
            if (!m_isAllowSystemProxy) {
                curl_easy_setopt(connectionHandle, CURLOPT_PROXY, "");
            }
        }

        if (request->GetContentBody()) {
            curl_easy_setopt(connectionHandle, CURLOPT_READFUNCTION, ReadBodyFunc);
            curl_easy_setopt(connectionHandle, CURLOPT_READDATA, &readContext);
            curl_easy_setopt(connectionHandle, CURLOPT_SEEKFUNCTION, SeekBody);
            curl_easy_setopt(connectionHandle, CURLOPT_SEEKDATA, &readContext);
            if (request->IsEventStreamRequest() && !response->HasHeader(Aws::Http::X_AMZN_ERROR_TYPE)) {
                curl_easy_setopt(connectionHandle, CURLOPT_READFUNCTION, ReadBodyStreaming);
                curl_easy_setopt(connectionHandle, CURLOPT_NOPROGRESS, 0L);
#if LIBCURL_VERSION_NUM >= 0x072000 // 7.32.0
                curl_easy_setopt(connectionHandle, CURLOPT_XFERINFOFUNCTION, CurlProgressCallback);
                curl_easy_setopt(connectionHandle, CURLOPT_XFERINFODATA, &readContext);
#else
                curl_easy_setopt(connectionHandle, CURLOPT_PROGRESSFUNCTION, CurlProgressCallback);
                curl_easy_setopt(connectionHandle, CURLOPT_PROGRESSDATA, &readContext);
#endif
            }
        }

        OverrideOptionsOnConnectionHandle(connectionHandle);
        Aws::Utils::DateTime startTransmissionTime = Aws::Utils::DateTime::Now();
        CURLcode curlResponseCode = curl_easy_perform(connectionHandle);
        bool shouldContinueRequest = ContinueRequest(*request);
        if (curlResponseCode != CURLE_OK && shouldContinueRequest) {
            response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
            Aws::StringStream ss;
            ss << "curlCode: " << curlResponseCode << ", " << curl_easy_strerror(curlResponseCode);
            response->SetClientErrorMessage(ss.str());
            log::storage().error("Curl returned error code {}: {}", static_cast<int>(curlResponseCode), curl_easy_strerror(curlResponseCode));
        } else if (!shouldContinueRequest) {
            response->SetClientErrorType(CoreErrors::USER_CANCELLED);
            response->SetClientErrorMessage("Request cancelled by user's continuation handler");
        } else {
            long responseCode;
            curl_easy_getinfo(connectionHandle, CURLINFO_RESPONSE_CODE, &responseCode);
            response->SetResponseCode(static_cast<HttpResponseCode>(responseCode));
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returned http response code {}", responseCode);

            char *contentType = nullptr;
            curl_easy_getinfo(connectionHandle, CURLINFO_CONTENT_TYPE, &contentType);
            if (contentType) {
                response->SetContentType(contentType);
                ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returned content type {}", contentType);
            }

            bool hasContentLength = false;
            int64_t contentLength =
                GetContentLengthFromHeader(connectionHandle, hasContentLength);

            if (request->GetMethod() != HttpMethod::HTTP_HEAD &&
                writeContext.m_client->IsRequestProcessingEnabled() &&
                hasContentLength) {
                int64_t numBytesResponseReceived = writeContext.m_numBytesResponseReceived;
                ARCTICDB_RUNTIME_DEBUG(log::storage(), "Response content-length header: {}", contentLength);
                ARCTICDB_RUNTIME_DEBUG(log::storage(), "Response body length: {}", numBytesResponseReceived);
                if (contentLength != numBytesResponseReceived) {
                    response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
                    response->SetClientErrorMessage("Response body length doesn't match the content-length header.");
                    log::storage().error("Response body length doesn't match the content-length header.");
                }
            }

            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Releasing curl handle {}", connectionHandle);
        }

        double timep;
        CURLcode ret = curl_easy_getinfo(connectionHandle, CURLINFO_NAMELOOKUP_TIME, &timep); // DNS Resolve Latency, seconds.
        if (ret == CURLE_OK)
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "DNS Latency: {}", static_cast<int64_t>(timep * 1000)); // to milliseconds

        ret = curl_easy_getinfo(connectionHandle, CURLINFO_STARTTRANSFER_TIME, &timep); // Connect Latency
        if (ret == CURLE_OK) 
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Connection Latency: {}",  static_cast<int64_t>(timep * 1000));

        ret = curl_easy_getinfo(connectionHandle, CURLINFO_APPCONNECT_TIME, &timep); // Ssl Latency
        if (ret == CURLE_OK) 
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "SSL Latency: {}", static_cast<int64_t>(timep * 1000));

        curl_off_t speed;
#if LIBCURL_VERSION_NUM >= 0x073700 // 7.55.0
        ret = curl_easy_getinfo(connectionHandle, CURLINFO_SPEED_DOWNLOAD_T, &speed); // throughput
#else
        ret = curl_easy_getinfo(connectionHandle, CURLINFO_SPEED_DOWNLOAD, &speed); // throughput
#endif
        if (ret == CURLE_OK)
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Throughput: {}", static_cast<int64_t>(speed));

        const char *ip = nullptr;
        auto curlGetInfoResult = curl_easy_getinfo(connectionHandle, CURLINFO_PRIMARY_IP, &ip); // Get the IP address of the remote endpoint
        if (curlGetInfoResult == CURLE_OK && ip) {
            request->SetResolvedRemoteHost(ip);
        }
        if (curlResponseCode != CURLE_OK) {
            m_curlHandleContainer.DestroyCurlHandle(connectionHandle);
        } else {
            m_curlHandleContainer.ReleaseCurlHandle(connectionHandle);
        }
        //go ahead and flush the response body stream
        response->GetResponseBody().flush();
        if (response->GetResponseBody().fail()) {
            const auto& ref = response->GetResponseBody();
            Aws::StringStream ss;
            ss << "Failed to flush response stream (eof: " << ref.eof() << ", bad: " << ref.bad() << ")";
            response->SetClientErrorType(CoreErrors::INTERNAL_FAILURE);
            response->SetClientErrorMessage(ss.str());
            log::storage().error(ss.str());
        }
        ARCTICDB_RUNTIME_DEBUG(log::storage(),"Roundtrip time: {}", DateTime::Now().Millis() - startTransmissionTime.Millis());
    }

    if (headers) {
        curl_slist_free_all(headers);
    }
    timer.stop_timer("curl");
    //log::storage().info("{}", timer.display_timer("curl"));
    return response;
}

} // namespace arcticdb