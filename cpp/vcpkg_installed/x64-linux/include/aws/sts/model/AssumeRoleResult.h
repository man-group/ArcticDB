/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/sts/model/Credentials.h>
#include <aws/sts/model/AssumedRoleUser.h>
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
   * <p>Contains the response to a successful <a>AssumeRole</a> request, including
   * temporary Amazon Web Services credentials that can be used to make Amazon Web
   * Services requests. </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumeRoleResponse">AWS
   * API Reference</a></p>
   */
  class AssumeRoleResult
  {
  public:
    AWS_STS_API AssumeRoleResult();
    AWS_STS_API AssumeRoleResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_STS_API AssumeRoleResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


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
    inline AssumeRoleResult& WithCredentials(const Credentials& value) { SetCredentials(value); return *this;}
    inline AssumeRoleResult& WithCredentials(Credentials&& value) { SetCredentials(std::move(value)); return *this;}
    ///@}

    ///@{
    /**
     * <p>The Amazon Resource Name (ARN) and the assumed role ID, which are identifiers
     * that you can use to refer to the resulting temporary security credentials. For
     * example, you can reference these credentials as a principal in a resource-based
     * policy by using the ARN or assumed role ID. The ARN and ID include the
     * <code>RoleSessionName</code> that you specified when you called
     * <code>AssumeRole</code>. </p>
     */
    inline const AssumedRoleUser& GetAssumedRoleUser() const{ return m_assumedRoleUser; }
    inline void SetAssumedRoleUser(const AssumedRoleUser& value) { m_assumedRoleUser = value; }
    inline void SetAssumedRoleUser(AssumedRoleUser&& value) { m_assumedRoleUser = std::move(value); }
    inline AssumeRoleResult& WithAssumedRoleUser(const AssumedRoleUser& value) { SetAssumedRoleUser(value); return *this;}
    inline AssumeRoleResult& WithAssumedRoleUser(AssumedRoleUser&& value) { SetAssumedRoleUser(std::move(value)); return *this;}
    ///@}

    ///@{
    /**
     * <p>A percentage value that indicates the packed size of the session policies and
     * session tags combined passed in the request. The request fails if the packed
     * size is greater than 100 percent, which means the policies and tags exceeded the
     * allowed space.</p>
     */
    inline int GetPackedPolicySize() const{ return m_packedPolicySize; }
    inline void SetPackedPolicySize(int value) { m_packedPolicySize = value; }
    inline AssumeRoleResult& WithPackedPolicySize(int value) { SetPackedPolicySize(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The source identity specified by the principal that is calling the
     * <code>AssumeRole</code> operation.</p> <p>You can require users to specify a
     * source identity when they assume a role. You do this by using the
     * <code>sts:SourceIdentity</code> condition key in a role trust policy. You can
     * use source identity information in CloudTrail logs to determine who took actions
     * with a role. You can use the <code>aws:SourceIdentity</code> condition key to
     * further control access to Amazon Web Services resources based on the value of
     * source identity. For more information about using source identity, see <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_control-access_monitor.html">Monitor
     * and control actions taken with assumed roles</a> in the <i>IAM User
     * Guide</i>.</p> <p>The regex used to validate this parameter is a string of
     * characters consisting of upper- and lower-case alphanumeric characters with no
     * spaces. You can also include underscores or any of the following characters:
     * =,.@-</p>
     */
    inline const Aws::String& GetSourceIdentity() const{ return m_sourceIdentity; }
    inline void SetSourceIdentity(const Aws::String& value) { m_sourceIdentity = value; }
    inline void SetSourceIdentity(Aws::String&& value) { m_sourceIdentity = std::move(value); }
    inline void SetSourceIdentity(const char* value) { m_sourceIdentity.assign(value); }
    inline AssumeRoleResult& WithSourceIdentity(const Aws::String& value) { SetSourceIdentity(value); return *this;}
    inline AssumeRoleResult& WithSourceIdentity(Aws::String&& value) { SetSourceIdentity(std::move(value)); return *this;}
    inline AssumeRoleResult& WithSourceIdentity(const char* value) { SetSourceIdentity(value); return *this;}
    ///@}

    ///@{
    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }
    inline AssumeRoleResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}
    inline AssumeRoleResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}
    ///@}
  private:

    Credentials m_credentials;

    AssumedRoleUser m_assumedRoleUser;

    int m_packedPolicySize;

    Aws::String m_sourceIdentity;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
