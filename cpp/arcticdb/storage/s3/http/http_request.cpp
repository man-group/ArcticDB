#include "http_request.hpp"
#include "arcticdb/log/log.hpp"

#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/StringUtils.h>

#include <iostream>
#include <cassert>

static bool IsDefaultPort(const Aws::Http::URI& uri)
{
    switch(uri.GetPort())
    {
    case 80:
        return uri.GetScheme() == Aws::Http::Scheme::HTTP;
    case 443:
        return uri.GetScheme() == Aws::Http::Scheme::HTTPS;
    default:
        return false;
    }
}

namespace arcticdb {
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

ArcticHttpRequest::ArcticHttpRequest(const Aws::Http::URI& uri, Aws::Http::HttpMethod method) :
    HttpRequest(uri, method),
    bodyStream(nullptr),
    m_responseStreamFactory()
{
    if(IsDefaultPort(uri))
    {
        ArcticHttpRequest::SetHeaderValue(Aws::Http::HOST_HEADER, uri.GetAuthority());
    }
    else
    {
        Aws::StringStream host;
        host << uri.GetAuthority() << ":" << uri.GetPort();
        ArcticHttpRequest::SetHeaderValue(Aws::Http::HOST_HEADER, host.str());
    }
}

Aws::Http::HeaderValueCollection ArcticHttpRequest::GetHeaders() const
{
    Aws::Http::HeaderValueCollection headers;

    for (const auto & iter : headerMap)
    {
        headers.emplace(Aws::Http::HeaderValuePair(iter.first, iter.second));
    }

    return headers;
}

std::vector<std::pair<std::string_view, std::string_view>> ArcticHttpRequest::GetHeaderViews() const
{
    std::vector<std::pair<std::string_view, std::string_view>> headers;
    headers.reserve(headerMap.size());
    for (const auto & iter : headerMap)
    {
        headers.emplace_back(std::make_pair(std::string_view(iter.first), std::string_view(iter.second)));
    }

    return headers;
}

const Aws::String& ArcticHttpRequest::GetHeaderValue(const char* headerName) const
{
    auto iter = headerMap.find(Aws::Utils::StringUtils::ToLower(headerName));
    assert (iter != headerMap.end());
    if (iter == headerMap.end()) {
        log::storage().error("Requested a header value for a missing header key: {}", headerName);
        static const Aws::String EMPTY_STRING;
        return EMPTY_STRING;
    }
    return iter->second;
}

void ArcticHttpRequest::SetHeaderValue(const char* headerName, const Aws::String& headerValue)
{
    using namespace Aws;
    headerMap[Utils::StringUtils::ToLower(headerName)] = Utils::StringUtils::Trim(headerValue.c_str());
}

void ArcticHttpRequest::SetHeaderValue(const Aws::String &headerName, const Aws::String &headerValue) {
    using namespace Aws;
    headerMap[Utils::StringUtils::ToLower(headerName.c_str())] = Utils::StringUtils::Trim(headerValue.c_str());
}

void ArcticHttpRequest::DeleteHeader(const char* headerName)
{
    using namespace Aws;
    headerMap.erase(Utils::StringUtils::ToLower(headerName));
}

bool ArcticHttpRequest::HasHeader(const char* headerName) const
{
    using namespace Aws;
    return headerMap.find(Utils::StringUtils::ToLower(headerName)) != headerMap.end();
}

int64_t ArcticHttpRequest::GetSize() const
{
    int64_t size = 0;
    std::for_each(headerMap.cbegin(), headerMap.cend(), [&](const auto& kvPair){ size += kvPair.first.length(); size += kvPair.second.length(); });
    return size;
}

const Aws::IOStreamFactory& ArcticHttpRequest::GetResponseStreamFactory() const
{
    return m_responseStreamFactory;
}

void ArcticHttpRequest::SetResponseStreamFactory(const Aws::IOStreamFactory& factory)
{
    m_responseStreamFactory = factory;
}

} // namespace arcticdb

