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
  class GetSessionTokenRequest : public STSRequest
  {
  public:
    AWS_STS_API GetSessionTokenRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetSessionToken"; }

    AWS_STS_API Aws::String SerializePayload() const override;

  protected:
    AWS_STS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    ///@{
    /**
     * <p>The duration, in seconds, that the credentials should remain valid.
     * Acceptable durations for IAM user sessions range from 900 seconds (15 minutes)
     * to 129,600 seconds (36 hours), with 43,200 seconds (12 hours) as the default.
     * Sessions for Amazon Web Services account owners are restricted to a maximum of
     * 3,600 seconds (one hour). If the duration is longer than one hour, the session
     * for Amazon Web Services account owners defaults to one hour.</p>
     */
    inline int GetDurationSeconds() const{ return m_durationSeconds; }
    inline bool DurationSecondsHasBeenSet() const { return m_durationSecondsHasBeenSet; }
    inline void SetDurationSeconds(int value) { m_durationSecondsHasBeenSet = true; m_durationSeconds = value; }
    inline GetSessionTokenRequest& WithDurationSeconds(int value) { SetDurationSeconds(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The identification number of the MFA device that is associated with the IAM
     * user who is making the <code>GetSessionToken</code> call. Specify this value if
     * the IAM user has a policy that requires MFA authentication. The value is either
     * the serial number for a hardware device (such as <code>GAHT12345678</code>) or
     * an Amazon Resource Name (ARN) for a virtual device (such as
     * <code>arn:aws:iam::123456789012:mfa/user</code>). You can find the device for an
     * IAM user by going to the Amazon Web Services Management Console and viewing the
     * user's security credentials. </p> <p>The regex used to validate this parameter
     * is a string of characters consisting of upper- and lower-case alphanumeric
     * characters with no spaces. You can also include underscores or any of the
     * following characters: =,.@:/-</p>
     */
    inline const Aws::String& GetSerialNumber() const{ return m_serialNumber; }
    inline bool SerialNumberHasBeenSet() const { return m_serialNumberHasBeenSet; }
    inline void SetSerialNumber(const Aws::String& value) { m_serialNumberHasBeenSet = true; m_serialNumber = value; }
    inline void SetSerialNumber(Aws::String&& value) { m_serialNumberHasBeenSet = true; m_serialNumber = std::move(value); }
    inline void SetSerialNumber(const char* value) { m_serialNumberHasBeenSet = true; m_serialNumber.assign(value); }
    inline GetSessionTokenRequest& WithSerialNumber(const Aws::String& value) { SetSerialNumber(value); return *this;}
    inline GetSessionTokenRequest& WithSerialNumber(Aws::String&& value) { SetSerialNumber(std::move(value)); return *this;}
    inline GetSessionTokenRequest& WithSerialNumber(const char* value) { SetSerialNumber(value); return *this;}
    ///@}

    ///@{
    /**
     * <p>The value provided by the MFA device, if MFA is required. If any policy
     * requires the IAM user to submit an MFA code, specify this value. If MFA
     * authentication is required, the user must provide a code when requesting a set
     * of temporary security credentials. A user who fails to provide the code receives
     * an "access denied" response when requesting resources that require MFA
     * authentication.</p> <p>The format for this parameter, as described by its regex
     * pattern, is a sequence of six numeric digits.</p>
     */
    inline const Aws::String& GetTokenCode() const{ return m_tokenCode; }
    inline bool TokenCodeHasBeenSet() const { return m_tokenCodeHasBeenSet; }
    inline void SetTokenCode(const Aws::String& value) { m_tokenCodeHasBeenSet = true; m_tokenCode = value; }
    inline void SetTokenCode(Aws::String&& value) { m_tokenCodeHasBeenSet = true; m_tokenCode = std::move(value); }
    inline void SetTokenCode(const char* value) { m_tokenCodeHasBeenSet = true; m_tokenCode.assign(value); }
    inline GetSessionTokenRequest& WithTokenCode(const Aws::String& value) { SetTokenCode(value); return *this;}
    inline GetSessionTokenRequest& WithTokenCode(Aws::String&& value) { SetTokenCode(std::move(value)); return *this;}
    inline GetSessionTokenRequest& WithTokenCode(const char* value) { SetTokenCode(value); return *this;}
    ///@}
  private:

    int m_durationSeconds;
    bool m_durationSecondsHasBeenSet = false;

    Aws::String m_serialNumber;
    bool m_serialNumberHasBeenSet = false;

    Aws::String m_tokenCode;
    bool m_tokenCodeHasBeenSet = false;
  };

} // namespace Model
} // namespace STS
} // namespace Aws
