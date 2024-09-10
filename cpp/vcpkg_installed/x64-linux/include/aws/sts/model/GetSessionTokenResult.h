/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/sts/model/Credentials.h>
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
  /**
   * <p>Contains the response to a successful <a>GetSessionToken</a> request,
   * including temporary Amazon Web Services credentials that can be used to make
   * Amazon Web Services requests. </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/GetSessionTokenResponse">AWS
   * API Reference</a></p>
   */
  class GetSessionTokenResult
  {
  public:
    AWS_STS_API GetSessionTokenResult();
    AWS_STS_API GetSessionTokenResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_STS_API GetSessionTokenResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    ///@{
    /**
     * <p>The temporary security credentials, which include an access key ID, a secret
     * access key, and a security (or session) token.</p>  <p>The size of the
     * security token that STS API operations return is not fixed. We strongly
     * recommend that you make no assumptions about the maximum size.</p> 
     */
    inline const Credentials& GetCredentials() const{ return m_credentials; }
    inline void SetCredentials(const Credentials& value) { m_credentials = value; }
    inline void SetCredentials(Credentials&& value) { m_credentials = std::move(value); }
    inline GetSessionTokenResult& WithCredentials(const Credentials& value) { SetCredentials(value); return *this;}
    inline GetSessionTokenResult& WithCredentials(Credentials&& value) { SetCredentials(std::move(value)); return *this;}
    ///@}

    ///@{
    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }
    inline GetSessionTokenResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}
    inline GetSessionTokenResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}
    ///@}
  private:

    Credentials m_credentials;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
