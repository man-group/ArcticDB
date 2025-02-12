/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/sts/STSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace STS
{
namespace Model
{

  /**
   */
  class DecodeAuthorizationMessageRequest : public STSRequest
  {
  public:
    AWS_STS_API DecodeAuthorizationMessageRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "DecodeAuthorizationMessage"; }

    AWS_STS_API Aws::String SerializePayload() const override;

  protected:
    AWS_STS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    ///@{
    /**
     * <p>The encoded message that was returned with the response.</p>
     */
    inline const Aws::String& GetEncodedMessage() const{ return m_encodedMessage; }
    inline bool EncodedMessageHasBeenSet() const { return m_encodedMessageHasBeenSet; }
    inline void SetEncodedMessage(const Aws::String& value) { m_encodedMessageHasBeenSet = true; m_encodedMessage = value; }
    inline void SetEncodedMessage(Aws::String&& value) { m_encodedMessageHasBeenSet = true; m_encodedMessage = std::move(value); }
    inline void SetEncodedMessage(const char* value) { m_encodedMessageHasBeenSet = true; m_encodedMessage.assign(value); }
    inline DecodeAuthorizationMessageRequest& WithEncodedMessage(const Aws::String& value) { SetEncodedMessage(value); return *this;}
    inline DecodeAuthorizationMessageRequest& WithEncodedMessage(Aws::String&& value) { SetEncodedMessage(std::move(value)); return *this;}
    inline DecodeAuthorizationMessageRequest& WithEncodedMessage(const char* value) { SetEncodedMessage(value); return *this;}
    ///@}
  private:

    Aws::String m_encodedMessage;
    bool m_encodedMessageHasBeenSet = false;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
