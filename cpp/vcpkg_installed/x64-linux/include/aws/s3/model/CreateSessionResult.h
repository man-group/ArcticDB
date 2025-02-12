/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/SessionCredentials.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
namespace S3
{
namespace Model
{
  class CreateSessionResult
  {
  public:
    AWS_S3_API CreateSessionResult();
    AWS_S3_API CreateSessionResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API CreateSessionResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    ///@{
    /**
     * <p>The established temporary security credentials for the created session.</p>
     */
    inline const SessionCredentials& GetCredentials() const{ return m_credentials; }
    inline void SetCredentials(const SessionCredentials& value) { m_credentials = value; }
    inline void SetCredentials(SessionCredentials&& value) { m_credentials = std::move(value); }
    inline CreateSessionResult& WithCredentials(const SessionCredentials& value) { SetCredentials(value); return *this;}
    inline CreateSessionResult& WithCredentials(SessionCredentials&& value) { SetCredentials(std::move(value)); return *this;}
    ///@}

    ///@{
    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }
    inline CreateSessionResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}
    inline CreateSessionResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}
    inline CreateSessionResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}
    ///@}
  private:

    SessionCredentials m_credentials;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
