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
   * <p>A reference to the IAM managed policy that is passed as a session policy for
   * a role session or a federated user session.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/PolicyDescriptorType">AWS
   * API Reference</a></p>
   */
  class PolicyDescriptorType
  {
  public:
    AWS_STS_API PolicyDescriptorType();
    AWS_STS_API PolicyDescriptorType(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_STS_API PolicyDescriptorType& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_STS_API void OutputToStream(Aws::OStream& ostream, const char* location, unsigned index, const char* locationValue) const;
    AWS_STS_API void OutputToStream(Aws::OStream& oStream, const char* location) const;


    ///@{
    /**
     * <p>The Amazon Resource Name (ARN) of the IAM managed policy to use as a session
     * policy for the role. For more information about ARNs, see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html">Amazon
     * Resource Names (ARNs) and Amazon Web Services Service Namespaces</a> in the
     * <i>Amazon Web Services General Reference</i>.</p>
     */
    inline const Aws::String& GetArn() const{ return m_arn; }
    inline bool ArnHasBeenSet() const { return m_arnHasBeenSet; }
    inline void SetArn(const Aws::String& value) { m_arnHasBeenSet = true; m_arn = value; }
    inline void SetArn(Aws::String&& value) { m_arnHasBeenSet = true; m_arn = std::move(value); }
    inline void SetArn(const char* value) { m_arnHasBeenSet = true; m_arn.assign(value); }
    inline PolicyDescriptorType& WithArn(const Aws::String& value) { SetArn(value); return *this;}
    inline PolicyDescriptorType& WithArn(Aws::String&& value) { SetArn(std::move(value)); return *this;}
    inline PolicyDescriptorType& WithArn(const char* value) { SetArn(value); return *this;}
    ///@}
  private:

    Aws::String m_arn;
    bool m_arnHasBeenSet = false;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
