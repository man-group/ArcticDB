/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/sts/model/ResponseMetadata.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace STS
{
namespace Model
{
  class GetAccessKeyInfoResult
  {
  public:
    AWS_STS_API GetAccessKeyInfoResult();
    AWS_STS_API GetAccessKeyInfoResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_STS_API GetAccessKeyInfoResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    ///@{
    /**
     * <p>The number used to identify the Amazon Web Services account.</p>
     */
    inline const Aws::String& GetAccount() const{ return m_account; }
    inline void SetAccount(const Aws::String& value) { m_account = value; }
    inline void SetAccount(Aws::String&& value) { m_account = std::move(value); }
    inline void SetAccount(const char* value) { m_account.assign(value); }
    inline GetAccessKeyInfoResult& WithAccount(const Aws::String& value) { SetAccount(value); return *this;}
    inline GetAccessKeyInfoResult& WithAccount(Aws::String&& value) { SetAccount(std::move(value)); return *this;}
    inline GetAccessKeyInfoResult& WithAccount(const char* value) { SetAccount(value); return *this;}
    ///@}

    ///@{
    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }
    inline GetAccessKeyInfoResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}
    inline GetAccessKeyInfoResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}
    ///@}
  private:

    Aws::String m_account;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
