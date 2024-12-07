/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <arcticdb/storage/s3/http_response.hpp>

#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/memory/AWSMemory.h>

#include <arcticdb/log/log.hpp>

#include <istream>

using namespace Aws::Http;
using namespace Aws::Utils;

namespace arcticdb {

HeaderValueCollection ArcticHttpResponse::GetHeaders() const {
    HeaderValueCollection headerValueCollection;

    for (const auto& iter : headerMap) {
        headerValueCollection.emplace(HeaderValuePair(iter.first, iter.second));
    }

    return headerValueCollection;
}

bool ArcticHttpResponse::HasHeader(const char *headerName) const {
    return headerMap.find(StringUtils::ToLower(headerName)) != headerMap.end();
}

const Aws::String& ArcticHttpResponse::GetHeader(const Aws::String& headerName) const {
    auto foundValue = headerMap.find(StringUtils::ToLower(headerName.c_str()));
    assert(foundValue != headerMap.end());
    if (foundValue == headerMap.end()) {
        log::storage().error(
                            "Requested a header value for a missing header key: {}", headerName);
        static const Aws::String EMPTY_STRING;
        return EMPTY_STRING;
    }
    return foundValue->second;
}

std::vector<std::pair<std::string_view, std::string_view>> ArcticHttpResponse::GetHeaderViews() const
{
    std::vector<std::pair<std::string_view, std::string_view>> headers;
    headers.reserve(headerMap.size());
    for (const auto & iter : headerMap)
    {
        headers.emplace_back(std::make_pair(std::string_view(iter.first), std::string_view(iter.second)));
    }

    return headers;
}

void ArcticHttpResponse::AddHeader(const Aws::String& headerName, const Aws::String& headerValue) {
    headerMap[StringUtils::ToLower(headerName.c_str())] = headerValue;
}

void ArcticHttpResponse::AddHeader(const Aws::String& headerName, Aws::String&& headerValue) {
    headerMap.emplace(StringUtils::ToLower(headerName.c_str()), std::move(headerValue));
}

} // namespace arcticdb