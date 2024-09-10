/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/sts/STSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/sts/model/PolicyDescriptorType.h>
#include <aws/sts/model/Tag.h>
#include <utility>

namespace Aws
{
namespace STS
{
namespace Model
{

  /**
   */
  class GetFederationTokenRequest : public STSRequest
  {
  public:
    AWS_STS_API GetFederationTokenRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetFederationToken"; }

    AWS_STS_API Aws::String SerializePayload() const override;

  protected:
    AWS_STS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    ///@{
    /**
     * <p>The name of the federated user. The name is used as an identifier for the
     * temporary security credentials (such as <code>Bob</code>). For example, you can
     * reference the federated user name in a resource-based policy, such as in an
     * Amazon S3 bucket policy.</p> <p>The regex used to validate this parameter is a
     * string of characters consisting of upper- and lower-case alphanumeric characters
     * with no spaces. You can also include underscores or any of the following
     * characters: =,.@-</p>
     */
    inline const Aws::String& GetName() const{ return m_name; }
    inline bool NameHasBeenSet() const { return m_nameHasBeenSet; }
    inline void SetName(const Aws::String& value) { m_nameHasBeenSet = true; m_name = value; }
    inline void SetName(Aws::String&& value) { m_nameHasBeenSet = true; m_name = std::move(value); }
    inline void SetName(const char* value) { m_nameHasBeenSet = true; m_name.assign(value); }
    inline GetFederationTokenRequest& WithName(const Aws::String& value) { SetName(value); return *this;}
    inline GetFederationTokenRequest& WithName(Aws::String&& value) { SetName(std::move(value)); return *this;}
    inline GetFederationTokenRequest& WithName(const char* value) { SetName(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>An IAM policy in JSON format that you want to use as an inline session
     * policy.</p> <p>You must pass an inline or managed <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
     * policy</a> to this operation. You can pass a single JSON policy document to use
     * as an inline session policy. You can also specify up to 10 managed policy Amazon
     * Resource Names (ARNs) to use as managed session policies.</p> <p>This parameter
     * is optional. However, if you do not pass any session policies, then the
     * resulting federated user session has no permissions.</p> <p>When you pass
     * session policies, the session permissions are the intersection of the IAM user
     * policies and the session policies that you pass. This gives you a way to further
     * restrict the permissions for a federated user. You cannot use session policies
     * to grant more permissions than those that are defined in the permissions policy
     * of the IAM user. For more information, see <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">Session
     * Policies</a> in the <i>IAM User Guide</i>.</p> <p>The resulting credentials can
     * be used to access a resource that has a resource-based policy. If that policy
     * specifically references the federated user session in the <code>Principal</code>
     * element of the policy, the session has the permissions allowed by the policy.
     * These permissions are granted in addition to the permissions that are granted by
     * the session policies.</p> <p>The plaintext that you use for both inline and
     * managed session policies can't exceed 2,048 characters. The JSON policy
     * characters can be any ASCII character from the space character to the end of the
     * valid character list (\u0020 through \u00FF). It can also include the tab
     * (\u0009), linefeed (\u000A), and carriage return (\u000D) characters.</p> 
     * <p>An Amazon Web Services conversion compresses the passed inline session
     * policy, managed policy ARNs, and session tags into a packed binary format that
     * has a separate limit. Your request can fail for this limit even if your
     * plaintext meets the other requirements. The <code>PackedPolicySize</code>
     * response element indicates by percentage how close the policies and tags for
     * your request are to the upper size limit.</p> 
     */
    inline const Aws::String& GetPolicy() const{ return m_policy; }
    inline bool PolicyHasBeenSet() const { return m_policyHasBeenSet; }
    inline void SetPolicy(const Aws::String& value) { m_policyHasBeenSet = true; m_policy = value; }
    inline void SetPolicy(Aws::String&& value) { m_policyHasBeenSet = true; m_policy = std::move(value); }
    inline void SetPolicy(const char* value) { m_policyHasBeenSet = true; m_policy.assign(value); }
    inline GetFederationTokenRequest& WithPolicy(const Aws::String& value) { SetPolicy(value); return *this;}
    inline GetFederationTokenRequest& WithPolicy(Aws::String&& value) { SetPolicy(std::move(value)); return *this;}
    inline GetFederationTokenRequest& WithPolicy(const char* value) { SetPolicy(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The Amazon Resource Names (ARNs) of the IAM managed policies that you want to
     * use as a managed session policy. The policies must exist in the same account as
     * the IAM user that is requesting federated access.</p> <p>You must pass an inline
     * or managed <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
     * policy</a> to this operation. You can pass a single JSON policy document to use
     * as an inline session policy. You can also specify up to 10 managed policy Amazon
     * Resource Names (ARNs) to use as managed session policies. The plaintext that you
     * use for both inline and managed session policies can't exceed 2,048 characters.
     * You can provide up to 10 managed policy ARNs. For more information about ARNs,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html">Amazon
     * Resource Names (ARNs) and Amazon Web Services Service Namespaces</a> in the
     * Amazon Web Services General Reference.</p> <p>This parameter is optional.
     * However, if you do not pass any session policies, then the resulting federated
     * user session has no permissions.</p> <p>When you pass session policies, the
     * session permissions are the intersection of the IAM user policies and the
     * session policies that you pass. This gives you a way to further restrict the
     * permissions for a federated user. You cannot use session policies to grant more
     * permissions than those that are defined in the permissions policy of the IAM
     * user. For more information, see <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">Session
     * Policies</a> in the <i>IAM User Guide</i>.</p> <p>The resulting credentials can
     * be used to access a resource that has a resource-based policy. If that policy
     * specifically references the federated user session in the <code>Principal</code>
     * element of the policy, the session has the permissions allowed by the policy.
     * These permissions are granted in addition to the permissions that are granted by
     * the session policies.</p>  <p>An Amazon Web Services conversion compresses
     * the passed inline session policy, managed policy ARNs, and session tags into a
     * packed binary format that has a separate limit. Your request can fail for this
     * limit even if your plaintext meets the other requirements. The
     * <code>PackedPolicySize</code> response element indicates by percentage how close
     * the policies and tags for your request are to the upper size limit.</p> 
     */
    inline const Aws::Vector<PolicyDescriptorType>& GetPolicyArns() const{ return m_policyArns; }
    inline bool PolicyArnsHasBeenSet() const { return m_policyArnsHasBeenSet; }
    inline void SetPolicyArns(const Aws::Vector<PolicyDescriptorType>& value) { m_policyArnsHasBeenSet = true; m_policyArns = value; }
    inline void SetPolicyArns(Aws::Vector<PolicyDescriptorType>&& value) { m_policyArnsHasBeenSet = true; m_policyArns = std::move(value); }
    inline GetFederationTokenRequest& WithPolicyArns(const Aws::Vector<PolicyDescriptorType>& value) { SetPolicyArns(value); return *this;}
    inline GetFederationTokenRequest& WithPolicyArns(Aws::Vector<PolicyDescriptorType>&& value) { SetPolicyArns(std::move(value)); return *this;}
    inline GetFederationTokenRequest& AddPolicyArns(const PolicyDescriptorType& value) { m_policyArnsHasBeenSet = true; m_policyArns.push_back(value); return *this; }
    inline GetFederationTokenRequest& AddPolicyArns(PolicyDescriptorType&& value) { m_policyArnsHasBeenSet = true; m_policyArns.push_back(std::move(value)); return *this; }
    ///@}

    ///@{
    /**
     * <p>The duration, in seconds, that the session should last. Acceptable durations
     * for federation sessions range from 900 seconds (15 minutes) to 129,600 seconds
     * (36 hours), with 43,200 seconds (12 hours) as the default. Sessions obtained
     * using root user credentials are restricted to a maximum of 3,600 seconds (one
     * hour). If the specified duration is longer than one hour, the session obtained
     * by using root user credentials defaults to one hour.</p>
     */
    inline int GetDurationSeconds() const{ return m_durationSeconds; }
    inline bool DurationSecondsHasBeenSet() const { return m_durationSecondsHasBeenSet; }
    inline void SetDurationSeconds(int value) { m_durationSecondsHasBeenSet = true; m_durationSeconds = value; }
    inline GetFederationTokenRequest& WithDurationSeconds(int value) { SetDurationSeconds(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>A list of session tags. Each session tag consists of a key name and an
     * associated value. For more information about session tags, see <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html">Passing
     * Session Tags in STS</a> in the <i>IAM User Guide</i>.</p> <p>This parameter is
     * optional. You can pass up to 50 session tags. The plaintext session tag keys
     * can’t exceed 128 characters and the values can’t exceed 256 characters. For
     * these and additional limits, see <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-limits.html#reference_iam-limits-entity-length">IAM
     * and STS Character Limits</a> in the <i>IAM User Guide</i>.</p>  <p>An
     * Amazon Web Services conversion compresses the passed inline session policy,
     * managed policy ARNs, and session tags into a packed binary format that has a
     * separate limit. Your request can fail for this limit even if your plaintext
     * meets the other requirements. The <code>PackedPolicySize</code> response element
     * indicates by percentage how close the policies and tags for your request are to
     * the upper size limit.</p>  <p>You can pass a session tag with the same
     * key as a tag that is already attached to the user you are federating. When you
     * do, session tags override a user tag with the same key. </p> <p>Tag key–value
     * pairs are not case sensitive, but case is preserved. This means that you cannot
     * have separate <code>Department</code> and <code>department</code> tag keys.
     * Assume that the role has the <code>Department</code>=<code>Marketing</code> tag
     * and you pass the <code>department</code>=<code>engineering</code> session tag.
     * <code>Department</code> and <code>department</code> are not saved as separate
     * tags, and the session tag passed in the request takes precedence over the role
     * tag.</p>
     */
    inline const Aws::Vector<Tag>& GetTags() const{ return m_tags; }
    inline bool TagsHasBeenSet() const { return m_tagsHasBeenSet; }
    inline void SetTags(const Aws::Vector<Tag>& value) { m_tagsHasBeenSet = true; m_tags = value; }
    inline void SetTags(Aws::Vector<Tag>&& value) { m_tagsHasBeenSet = true; m_tags = std::move(value); }
    inline GetFederationTokenRequest& WithTags(const Aws::Vector<Tag>& value) { SetTags(value); return *this;}
    inline GetFederationTokenRequest& WithTags(Aws::Vector<Tag>&& value) { SetTags(std::move(value)); return *this;}
    inline GetFederationTokenRequest& AddTags(const Tag& value) { m_tagsHasBeenSet = true; m_tags.push_back(value); return *this; }
    inline GetFederationTokenRequest& AddTags(Tag&& value) { m_tagsHasBeenSet = true; m_tags.push_back(std::move(value)); return *this; }
    ///@}
  private:

    Aws::String m_name;
    bool m_nameHasBeenSet = false;

    Aws::String m_policy;
    bool m_policyHasBeenSet = false;

    Aws::Vector<PolicyDescriptorType> m_policyArns;
    bool m_policyArnsHasBeenSet = false;

    int m_durationSeconds;
    bool m_durationSecondsHasBeenSet = false;

    Aws::Vector<Tag> m_tags;
    bool m_tagsHasBeenSet = false;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
