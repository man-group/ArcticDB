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
   * <p>Contains the response to a successful <a>AssumeRoleWithSAML</a> request,
   * including temporary Amazon Web Services credentials that can be used to make
   * Amazon Web Services requests. </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumeRoleWithSAMLResponse">AWS
   * API Reference</a></p>
   */
  class AssumeRoleWithSAMLResult
  {
  public:
    AWS_STS_API AssumeRoleWithSAMLResult();
    AWS_STS_API AssumeRoleWithSAMLResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_STS_API AssumeRoleWithSAMLResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


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
    inline AssumeRoleWithSAMLResult& WithCredentials(const Credentials& value) { SetCredentials(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithCredentials(Credentials&& value) { SetCredentials(std::move(value)); return *this;}
    ///@}

    ///@{
    /**
     * <p>The identifiers for the temporary security credentials that the operation
     * returns.</p>
     */
    inline const AssumedRoleUser& GetAssumedRoleUser() const{ return m_assumedRoleUser; }
    inline void SetAssumedRoleUser(const AssumedRoleUser& value) { m_assumedRoleUser = value; }
    inline void SetAssumedRoleUser(AssumedRoleUser&& value) { m_assumedRoleUser = std::move(value); }
    inline AssumeRoleWithSAMLResult& WithAssumedRoleUser(const AssumedRoleUser& value) { SetAssumedRoleUser(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithAssumedRoleUser(AssumedRoleUser&& value) { SetAssumedRoleUser(std::move(value)); return *this;}
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
    inline AssumeRoleWithSAMLResult& WithPackedPolicySize(int value) { SetPackedPolicySize(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The value of the <code>NameID</code> element in the <code>Subject</code>
     * element of the SAML assertion.</p>
     */
    inline const Aws::String& GetSubject() const{ return m_subject; }
    inline void SetSubject(const Aws::String& value) { m_subject = value; }
    inline void SetSubject(Aws::String&& value) { m_subject = std::move(value); }
    inline void SetSubject(const char* value) { m_subject.assign(value); }
    inline AssumeRoleWithSAMLResult& WithSubject(const Aws::String& value) { SetSubject(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithSubject(Aws::String&& value) { SetSubject(std::move(value)); return *this;}
    inline AssumeRoleWithSAMLResult& WithSubject(const char* value) { SetSubject(value); return *this;}
    ///@}

    ///@{
    /**
     * <p> The format of the name ID, as defined by the <code>Format</code> attribute
     * in the <code>NameID</code> element of the SAML assertion. Typical examples of
     * the format are <code>transient</code> or <code>persistent</code>. </p> <p> If
     * the format includes the prefix
     * <code>urn:oasis:names:tc:SAML:2.0:nameid-format</code>, that prefix is removed.
     * For example, <code>urn:oasis:names:tc:SAML:2.0:nameid-format:transient</code> is
     * returned as <code>transient</code>. If the format includes any other prefix, the
     * format is returned with no modifications.</p>
     */
    inline const Aws::String& GetSubjectType() const{ return m_subjectType; }
    inline void SetSubjectType(const Aws::String& value) { m_subjectType = value; }
    inline void SetSubjectType(Aws::String&& value) { m_subjectType = std::move(value); }
    inline void SetSubjectType(const char* value) { m_subjectType.assign(value); }
    inline AssumeRoleWithSAMLResult& WithSubjectType(const Aws::String& value) { SetSubjectType(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithSubjectType(Aws::String&& value) { SetSubjectType(std::move(value)); return *this;}
    inline AssumeRoleWithSAMLResult& WithSubjectType(const char* value) { SetSubjectType(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The value of the <code>Issuer</code> element of the SAML assertion.</p>
     */
    inline const Aws::String& GetIssuer() const{ return m_issuer; }
    inline void SetIssuer(const Aws::String& value) { m_issuer = value; }
    inline void SetIssuer(Aws::String&& value) { m_issuer = std::move(value); }
    inline void SetIssuer(const char* value) { m_issuer.assign(value); }
    inline AssumeRoleWithSAMLResult& WithIssuer(const Aws::String& value) { SetIssuer(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithIssuer(Aws::String&& value) { SetIssuer(std::move(value)); return *this;}
    inline AssumeRoleWithSAMLResult& WithIssuer(const char* value) { SetIssuer(value); return *this;}
    ///@}

    ///@{
    /**
     * <p> The value of the <code>Recipient</code> attribute of the
     * <code>SubjectConfirmationData</code> element of the SAML assertion. </p>
     */
    inline const Aws::String& GetAudience() const{ return m_audience; }
    inline void SetAudience(const Aws::String& value) { m_audience = value; }
    inline void SetAudience(Aws::String&& value) { m_audience = std::move(value); }
    inline void SetAudience(const char* value) { m_audience.assign(value); }
    inline AssumeRoleWithSAMLResult& WithAudience(const Aws::String& value) { SetAudience(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithAudience(Aws::String&& value) { SetAudience(std::move(value)); return *this;}
    inline AssumeRoleWithSAMLResult& WithAudience(const char* value) { SetAudience(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>A hash value based on the concatenation of the following:</p> <ul> <li>
     * <p>The <code>Issuer</code> response value.</p> </li> <li> <p>The Amazon Web
     * Services account ID.</p> </li> <li> <p>The friendly name (the last part of the
     * ARN) of the SAML provider in IAM.</p> </li> </ul> <p>The combination of
     * <code>NameQualifier</code> and <code>Subject</code> can be used to uniquely
     * identify a user.</p> <p>The following pseudocode shows how the hash value is
     * calculated:</p> <p> <code>BASE64 ( SHA1 ( "https://example.com/saml" +
     * "123456789012" + "/MySAMLIdP" ) )</code> </p>
     */
    inline const Aws::String& GetNameQualifier() const{ return m_nameQualifier; }
    inline void SetNameQualifier(const Aws::String& value) { m_nameQualifier = value; }
    inline void SetNameQualifier(Aws::String&& value) { m_nameQualifier = std::move(value); }
    inline void SetNameQualifier(const char* value) { m_nameQualifier.assign(value); }
    inline AssumeRoleWithSAMLResult& WithNameQualifier(const Aws::String& value) { SetNameQualifier(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithNameQualifier(Aws::String&& value) { SetNameQualifier(std::move(value)); return *this;}
    inline AssumeRoleWithSAMLResult& WithNameQualifier(const char* value) { SetNameQualifier(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The value in the <code>SourceIdentity</code> attribute in the SAML assertion.
     * </p> <p>You can require users to set a source identity value when they assume a
     * role. You do this by using the <code>sts:SourceIdentity</code> condition key in
     * a role trust policy. That way, actions that are taken with the role are
     * associated with that user. After the source identity is set, the value cannot be
     * changed. It is present in the request for all actions that are taken by the role
     * and persists across <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts#iam-term-role-chaining">chained
     * role</a> sessions. You can configure your SAML identity provider to use an
     * attribute associated with your users, like user name or email, as the source
     * identity when calling <code>AssumeRoleWithSAML</code>. You do this by adding an
     * attribute to the SAML assertion. For more information about using source
     * identity, see <a
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
    inline AssumeRoleWithSAMLResult& WithSourceIdentity(const Aws::String& value) { SetSourceIdentity(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithSourceIdentity(Aws::String&& value) { SetSourceIdentity(std::move(value)); return *this;}
    inline AssumeRoleWithSAMLResult& WithSourceIdentity(const char* value) { SetSourceIdentity(value); return *this;}
    ///@}

    ///@{
    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }
    inline AssumeRoleWithSAMLResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}
    inline AssumeRoleWithSAMLResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}
    ///@}
  private:

    Credentials m_credentials;

    AssumedRoleUser m_assumedRoleUser;

    int m_packedPolicySize;

    Aws::String m_subject;

    Aws::String m_subjectType;

    Aws::String m_issuer;

    Aws::String m_audience;

    Aws::String m_nameQualifier;

    Aws::String m_sourceIdentity;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
