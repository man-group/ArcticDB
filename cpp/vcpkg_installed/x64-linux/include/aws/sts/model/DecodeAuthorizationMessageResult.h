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
  /**
   * <p>A document that contains additional information about the authorization
   * status of a request from an encoded message that is returned in response to an
   * Amazon Web Services request.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/DecodeAuthorizationMessageResponse">AWS
   * API Reference</a></p>
   */
  class DecodeAuthorizationMessageResult
  {
  public:
    AWS_STS_API DecodeAuthorizationMessageResult();
    AWS_STS_API DecodeAuthorizationMessageResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_STS_API DecodeAuthorizationMessageResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    ///@{
    /**
     * <p>The API returns a response with the decoded message.</p>
     */
    inline const Aws::String& GetDecodedMessage() const{ return m_decodedMessage; }
    inline void SetDecodedMessage(const Aws::String& value) { m_decodedMessage = value; }
    inline void SetDecodedMessage(Aws::String&& value) { m_decodedMessage = std::move(value); }
    inline void SetDecodedMessage(const char* value) { m_decodedMessage.assign(value); }
    inline DecodeAuthorizationMessageResult& WithDecodedMessage(const Aws::String& value) { SetDecodedMessage(value); return *this;}
    inline DecodeAuthorizationMessageResult& WithDecodedMessage(Aws::String&& value) { SetDecodedMessage(std::move(value)); return *this;}
    inline DecodeAuthorizationMessageResult& WithDecodedMessage(const char* value) { SetDecodedMessage(value); return *this;}
    ///@}

    ///@{
    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }
    inline DecodeAuthorizationMessageResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}
    inline DecodeAuthorizationMessageResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}
    ///@}
  private:

    Aws::String m_decodedMessage;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
