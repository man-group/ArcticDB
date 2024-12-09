//#pragma once
//
//#include <aws/core/http/HttpClientFactory.h>
//#include <aws/core/http/URI.h>
//#include <aws/core/http/standard/StandardHttpRequest.h>
//
//#include <arcticdb/storage/s3/http_client.hpp>
//#include <arcticdb/log/log.hpp>
//#include <csignal>
//
//namespace arcticdb {
//
//static std::shared_ptr<Aws::Http::HttpClientFactory>& GetHttpClientFactory()
//{
//    static std::shared_ptr<Aws::Http::HttpClientFactory> s_HttpClientFactory(nullptr);
//    return s_HttpClientFactory;
//}
//
//class ArcticHttpClientFactory : public Aws::Http::HttpClientFactory {
//    std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration) const override
//    {
//#if ENABLE_WINDOWS_CLIENT
//        #if ENABLE_WINDOWS_IXML_HTTP_REQUEST_2_CLIENT
//                return Aws::MakeShared<IXmlHttpRequest2HttpClient>(HTTP_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);
//#else
//                switch (clientConfiguration.httpLibOverride)
//                {
//                    case TransferLibType::WIN_INET_CLIENT:
//                        return Aws::MakeShared<WinINetSyncHttpClient>(HTTP_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);
//
//                    default:
//                        return Aws::MakeShared<WinHttpSyncHttpClient>(HTTP_CLIENT_FACTORY_ALLOCATION_TAG, clientConfiguration);
//                }
//#endif // ENABLE_WINDOWS_IXML_HTTP_REQUEST_2_CLIENT
//#endif
//        return Aws::MakeShared<ArcticCurlHttpClient>("ARCTICDB_HTTP_CLIENT_FACTORY_ALLOCATION_TAG", clientConfiguration);
//    }
//
//    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(const Aws::String &uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory &streamFactory) const override
//    {
//        return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
//    }
//
//    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory) const override
//    {
//        auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("ARCTICDB_HTTP_CLIENT_FACTORY_ALLOCATION_TAG", uri, method);
//        request->SetResponseStreamFactory(streamFactory);
//
//        return request;
//    }
//
//    void InitStaticState() override
//    {
//        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Initializing Http Static State");
//#if !defined (_WIN32)
//        if(s_InitCleanupCurlFlag)
//        {
//            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Initializing Curl Http Client");
//            ArcticCurlHttpClient::InitGlobalState();
//        }
//
//        if(s_InstallSigPipeHandler)
//        {
//            ::signal(SIGPIPE, LogAndSwallowHandler);
//        }
//#elif ENABLE_WINDOWS_IXML_HTTP_REQUEST_2_CLIENT
//        IXmlHttpRequest2HttpClient::InitCOM();
//#endif
//    }
//
//    virtual void CleanupStaticState() override
//    {
//        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Cleanup Http Static State");
//#if ENABLE_CURL_CLIENT
//        if(s_InitCleanupCurlFlag)
//                {
//                    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Cleanup Curl Http Client");
//                    CurlHttpClient::CleanupGlobalState();
//                }
//#endif
//    }
//};
//
//void SetInitCleanupCurlFlag(bool initCleanupFlag)
//{
//    using namespace Aws::Http;
//    s_InitCleanupCurlFlag = initCleanupFlag;
//}
//
//void SetInstallSigPipeHandlerFlag(bool install)
//{
//    s_InstallSigPipeHandler = install;
//}
//
//void InitHttp()
//{
//    if(!GetHttpClientFactory())
//    {
//        GetHttpClientFactory() = Aws::MakeShared<ArcticHttpClientFactory>("HTTP_CLIENT_FACTORY_ALLOCATION_TAG");
//    }
//    GetHttpClientFactory()->InitStaticState();
//}
//
//void CleanupHttp()
//{
//    if(GetHttpClientFactory())
//    {
//        GetHttpClientFactory()->CleanupStaticState();
//        GetHttpClientFactory() = nullptr;
//    }
//}
//
//void SetHttpClientFactory(const std::shared_ptr<Aws::Http::HttpClientFactory>& factory)
//{
//    CleanupHttp();
//    GetHttpClientFactory() = factory;
//}
//
//std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration)
//{
//    assert(GetHttpClientFactory());
//    auto client = GetHttpClientFactory()->CreateHttpClient(clientConfiguration);
//
//    if (!client)
//    {
//        log::storage().error("Initializing Http Client failed!");
//        // assert just in case this is a misconfiguration at development time to make the dev's job easier.
//        assert(false && "Http client initialization failed. Some client configuration parameters are probably invalid");
//    }
//
//    return client;
//}
//
//virtual std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(const Aws::String& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory) const
//{
//    assert(GetHttpClientFactory());
//    return GetHttpClientFactory()->CreateHttpRequest(uri, method, streamFactory);
//}
//
//std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory)
//{
//    assert(GetHttpClientFactory());
//    return GetHttpClientFactory()->CreateHttpRequest(uri, method, streamFactory);
//}
//};
//
//} // namespace arcticdb