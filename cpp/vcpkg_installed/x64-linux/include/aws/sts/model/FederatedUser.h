/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace STS
{
namespace Model
{

  /**
   * <p>Identifiers for the federated user that is associated with the
   * credentials.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/FederatedUser">AWS
   * API Reference</a></p>
   */
  class FederatedUser
  {
  public:
    AWS_STS_API FederatedUser();
    AWS_STS_API FederatedUser(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_STS_API FederatedUser& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_STS_API void OutputToStream(Aws::OStream& ostream, const char* location, unsigned index, const char* locationValue) const;
    AWS_STS_API void OutputToStream(Aws::OStream& oStream, const char* location) const;


    ///@{
    /**
     * <p>The string that identifies the federated user associated with the
     * credentials, similar to the unique ID of an IAM user.</p>
     */
    inline const Aws::String& GetFederatedUserId() const{ return m_federatedUserId; }
    inline bool FederatedUserIdHasBeenSet() const { return m_federatedUserIdHasBeenSet; }
    inline void SetFederatedUserId(const Aws::String& value) { m_federatedUserIdHasBeenSet = true; m_federatedUserId = value; }
    inline void SetFederatedUserId(Aws::String&& value) { m_federatedUserIdHasBeenSet = true; m_federatedUserId = std::move(value); }
    inline void SetFederatedUserId(const char* value) { m_federatedUserIdHasBeenSet = true; m_federatedUserId.assign(value); }
    inline FederatedUser& WithFederatedUserId(const Aws::String& value) { SetFederatedUserId(value); return *this;}
    inline FederatedUser& WithFederatedUserId(Aws::String&& value) { SetFederatedUserId(std::move(value)); return *this;}
    inline FederatedUser& WithFederatedUserId(const char* value) { SetFederatedUserId(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The ARN that specifies the federated user that is associated with the
     * credentials. For more information about ARNs and how to use them in policies,
     * see <a
     * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html">IAM
     * Identifiers</a> in the <i>IAM User Guide</i>. </p>
     */
    inline const Aws::String& GetArn() const{ return m_arn; }
    inline bool ArnHasBeenSet() const { return m_arnHasBeenSet; }
    inline void SetArn(const Aws::String& value) { m_arnHasBeenSet = true; m_arn = value; }
    inline void SetArn(Aws::String&& value) { m_arnHasBeenSet = true; m_arn = std::move(value); }
    inline void SetArn(const char* value) { m_arnHasBeenSet = true; m_arn.assign(value); }
    inline FederatedUser& WithArn(const Aws::String& value) { SetArn(value); return *this;}
    inline FederatedUser& WithArn(Aws::String&& value) { SetArn(std::move(value)); return *this;}
    inline FederatedUser& WithArn(const char* value) { SetArn(value); return *this;}
    ///@}
  private:

    Aws::String m_federatedUserId;
    bool m_federatedUserIdHasBeenSet = false;

    Aws::String m_arn;
    bool m_arnHasBeenSet = false;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
