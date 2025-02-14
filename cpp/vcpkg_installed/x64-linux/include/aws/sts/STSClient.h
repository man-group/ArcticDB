/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/AmazonSerializableWebServiceRequest.h>
#include <aws/core/client/AWSClient.h>
#include <aws/core/client/AWSClientAsyncCRTP.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/sts/STSServiceClientModel.h>

namespace Aws
{
namespace STS
{
  /**
   * <fullname>Security Token Service</fullname> <p>Security Token Service (STS)
   * enables you to request temporary, limited-privilege credentials for users. This
   * guide provides descriptions of the STS API. For more information about using
   * this service, see <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html">Temporary
   * Security Credentials</a>.</p>
   */
  class AWS_STS_API STSClient : public Aws::Client::AWSXMLClient, public Aws::Client::ClientWithAsyncTemplateMethods<STSClient>
  {
    public:
      typedef Aws::Client::AWSXMLClient BASECLASS;
      static const char* GetServiceName();
      static const char* GetAllocationTag();

      typedef STSClientConfiguration ClientConfigurationType;
      typedef STSEndpointProvider EndpointProviderType;

       /**
        * Initializes client to use DefaultCredentialProviderChain, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        STSClient(const Aws::STS::STSClientConfiguration& clientConfiguration = Aws::STS::STSClientConfiguration(),
                  std::shared_ptr<STSEndpointProviderBase> endpointProvider = nullptr);

       /**
        * Initializes client to use SimpleAWSCredentialsProvider, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        STSClient(const Aws::Auth::AWSCredentials& credentials,
                  std::shared_ptr<STSEndpointProviderBase> endpointProvider = nullptr,
                  const Aws::STS::STSClientConfiguration& clientConfiguration = Aws::STS::STSClientConfiguration());

       /**
        * Initializes client to use specified credentials provider with specified client config. If http client factory is not supplied,
        * the default http client factory will be used
        */
        STSClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
                  std::shared_ptr<STSEndpointProviderBase> endpointProvider = nullptr,
                  const Aws::STS::STSClientConfiguration& clientConfiguration = Aws::STS::STSClientConfiguration());


        /* Legacy constructors due deprecation */
       /**
        * Initializes client to use DefaultCredentialProviderChain, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        STSClient(const Aws::Client::ClientConfiguration& clientConfiguration);

       /**
        * Initializes client to use SimpleAWSCredentialsProvider, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        STSClient(const Aws::Auth::AWSCredentials& credentials,
                  const Aws::Client::ClientConfiguration& clientConfiguration);

       /**
        * Initializes client to use specified credentials provider with specified client config. If http client factory is not supplied,
        * the default http client factory will be used
        */
        STSClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
                  const Aws::Client::ClientConfiguration& clientConfiguration);

        /* End of legacy constructors due deprecation */
        virtual ~STSClient();


       /**
        * Converts any request object to a presigned URL with the GET method, using region for the signer and a timeout of 15 minutes.
        */
        Aws::String ConvertRequestToPresignedUrl(const Aws::AmazonSerializableWebServiceRequest& requestToConvert, const char* region) const;


        /**
         * <p>Returns a set of temporary security credentials that you can use to access
         * Amazon Web Services resources. These temporary credentials consist of an access
         * key ID, a secret access key, and a security token. Typically, you use
         * <code>AssumeRole</code> within your account or for cross-account access. For a
         * comparison of <code>AssumeRole</code> with other API operations that produce
         * temporary credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html">Requesting
         * Temporary Security Credentials</a> and <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#stsapi_comparison">Comparing
         * the Amazon Web Services STS API operations</a> in the <i>IAM User Guide</i>.</p>
         * <p> <b>Permissions</b> </p> <p>The temporary security credentials created by
         * <code>AssumeRole</code> can be used to make API calls to any Amazon Web Services
         * service with the following exception: You cannot call the Amazon Web Services
         * STS <code>GetFederationToken</code> or <code>GetSessionToken</code> API
         * operations.</p> <p>(Optional) You can pass inline or managed <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
         * policies</a> to this operation. You can pass a single JSON policy document to
         * use as an inline session policy. You can also specify up to 10 managed policy
         * Amazon Resource Names (ARNs) to use as managed session policies. The plaintext
         * that you use for both inline and managed session policies can't exceed 2,048
         * characters. Passing policies to this operation returns new temporary
         * credentials. The resulting session's permissions are the intersection of the
         * role's identity-based policy and the session policies. You can use the role's
         * temporary credentials in subsequent Amazon Web Services API calls to access
         * resources in the account that owns the role. You cannot use session policies to
         * grant more permissions than those allowed by the identity-based policy of the
         * role that is being assumed. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">Session
         * Policies</a> in the <i>IAM User Guide</i>.</p> <p>When you create a role, you
         * create two policies: a role trust policy that specifies <i>who</i> can assume
         * the role, and a permissions policy that specifies <i>what</i> can be done with
         * the role. You specify the trusted principal that is allowed to assume the role
         * in the role trust policy.</p> <p>To assume a role from a different account, your
         * Amazon Web Services account must be trusted by the role. The trust relationship
         * is defined in the role's trust policy when the role is created. That trust
         * policy states which accounts are allowed to delegate that access to users in the
         * account. </p> <p>A user who wants to access a role in a different account must
         * also have permissions that are delegated from the account administrator. The
         * administrator must attach a policy that allows the user to call
         * <code>AssumeRole</code> for the ARN of the role in the other account.</p> <p>To
         * allow a user to assume a role in the same account, you can do either of the
         * following:</p> <ul> <li> <p>Attach a policy to the user that allows the user to
         * call <code>AssumeRole</code> (as long as the role's trust policy trusts the
         * account).</p> </li> <li> <p>Add the user as a principal directly in the role's
         * trust policy.</p> </li> </ul> <p>You can do either because the role’s trust
         * policy acts as an IAM resource-based policy. When a resource-based policy grants
         * access to a principal in the same account, no additional identity-based policy
         * is required. For more information about trust policies and resource-based
         * policies, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html">IAM
         * Policies</a> in the <i>IAM User Guide</i>.</p> <p> <b>Tags</b> </p>
         * <p>(Optional) You can pass tag key-value pairs to your session. These tags are
         * called session tags. For more information about session tags, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html">Passing
         * Session Tags in STS</a> in the <i>IAM User Guide</i>.</p> <p>An administrator
         * must grant you the permissions necessary to pass session tags. The administrator
         * can also create granular permissions to allow you to pass only specific session
         * tags. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_attribute-based-access-control.html">Tutorial:
         * Using Tags for Attribute-Based Access Control</a> in the <i>IAM User
         * Guide</i>.</p> <p>You can set the session tags as transitive. Transitive tags
         * persist during role chaining. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html#id_session-tags_role-chaining">Chaining
         * Roles with Session Tags</a> in the <i>IAM User Guide</i>.</p> <p> <b>Using MFA
         * with AssumeRole</b> </p> <p>(Optional) You can include multi-factor
         * authentication (MFA) information when you call <code>AssumeRole</code>. This is
         * useful for cross-account scenarios to ensure that the user that assumes the role
         * has been authenticated with an Amazon Web Services MFA device. In that scenario,
         * the trust policy of the role being assumed includes a condition that tests for
         * MFA authentication. If the caller does not include valid MFA information, the
         * request to assume the role is denied. The condition in a trust policy that tests
         * for MFA authentication might look like the following example.</p> <p>
         * <code>"Condition": {"Bool": {"aws:MultiFactorAuthPresent": true}}</code> </p>
         * <p>For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/MFAProtectedAPI.html">Configuring
         * MFA-Protected API Access</a> in the <i>IAM User Guide</i> guide.</p> <p>To use
         * MFA with <code>AssumeRole</code>, you pass values for the
         * <code>SerialNumber</code> and <code>TokenCode</code> parameters. The
         * <code>SerialNumber</code> value identifies the user's hardware or virtual MFA
         * device. The <code>TokenCode</code> is the time-based one-time password (TOTP)
         * that the MFA device produces. </p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumeRole">AWS API
         * Reference</a></p>
         */
        virtual Model::AssumeRoleOutcome AssumeRole(const Model::AssumeRoleRequest& request) const;

        /**
         * A Callable wrapper for AssumeRole that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename AssumeRoleRequestT = Model::AssumeRoleRequest>
        Model::AssumeRoleOutcomeCallable AssumeRoleCallable(const AssumeRoleRequestT& request) const
        {
            return SubmitCallable(&STSClient::AssumeRole, request);
        }

        /**
         * An Async wrapper for AssumeRole that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename AssumeRoleRequestT = Model::AssumeRoleRequest>
        void AssumeRoleAsync(const AssumeRoleRequestT& request, const AssumeRoleResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&STSClient::AssumeRole, request, handler, context);
        }

        /**
         * <p>Returns a set of temporary security credentials for users who have been
         * authenticated via a SAML authentication response. This operation provides a
         * mechanism for tying an enterprise identity store or directory to role-based
         * Amazon Web Services access without user-specific credentials or configuration.
         * For a comparison of <code>AssumeRoleWithSAML</code> with the other API
         * operations that produce temporary credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html">Requesting
         * Temporary Security Credentials</a> and <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#stsapi_comparison">Comparing
         * the Amazon Web Services STS API operations</a> in the <i>IAM User Guide</i>.</p>
         * <p>The temporary security credentials returned by this operation consist of an
         * access key ID, a secret access key, and a security token. Applications can use
         * these temporary security credentials to sign calls to Amazon Web Services
         * services.</p> <p> <b>Session Duration</b> </p> <p>By default, the temporary
         * security credentials created by <code>AssumeRoleWithSAML</code> last for one
         * hour. However, you can use the optional <code>DurationSeconds</code> parameter
         * to specify the duration of your session. Your role session lasts for the
         * duration that you specify, or until the time specified in the SAML
         * authentication response's <code>SessionNotOnOrAfter</code> value, whichever is
         * shorter. You can provide a <code>DurationSeconds</code> value from 900 seconds
         * (15 minutes) up to the maximum session duration setting for the role. This
         * setting can have a value from 1 hour to 12 hours. To learn how to view the
         * maximum value for your role, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html#id_roles_use_view-role-max-session">View
         * the Maximum Session Duration Setting for a Role</a> in the <i>IAM User
         * Guide</i>. The maximum session duration limit applies when you use the
         * <code>AssumeRole*</code> API operations or the <code>assume-role*</code> CLI
         * commands. However the limit does not apply when you use those operations to
         * create a console URL. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html">Using
         * IAM Roles</a> in the <i>IAM User Guide</i>.</p>  <p> <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#iam-term-role-chaining">Role
         * chaining</a> limits your CLI or Amazon Web Services API role session to a
         * maximum of one hour. When you use the <code>AssumeRole</code> API operation to
         * assume a role, you can specify the duration of your role session with the
         * <code>DurationSeconds</code> parameter. You can specify a parameter value of up
         * to 43200 seconds (12 hours), depending on the maximum session duration setting
         * for your role. However, if you assume a role using role chaining and provide a
         * <code>DurationSeconds</code> parameter value greater than one hour, the
         * operation fails.</p>  <p> <b>Permissions</b> </p> <p>The temporary
         * security credentials created by <code>AssumeRoleWithSAML</code> can be used to
         * make API calls to any Amazon Web Services service with the following exception:
         * you cannot call the STS <code>GetFederationToken</code> or
         * <code>GetSessionToken</code> API operations.</p> <p>(Optional) You can pass
         * inline or managed <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
         * policies</a> to this operation. You can pass a single JSON policy document to
         * use as an inline session policy. You can also specify up to 10 managed policy
         * Amazon Resource Names (ARNs) to use as managed session policies. The plaintext
         * that you use for both inline and managed session policies can't exceed 2,048
         * characters. Passing policies to this operation returns new temporary
         * credentials. The resulting session's permissions are the intersection of the
         * role's identity-based policy and the session policies. You can use the role's
         * temporary credentials in subsequent Amazon Web Services API calls to access
         * resources in the account that owns the role. You cannot use session policies to
         * grant more permissions than those allowed by the identity-based policy of the
         * role that is being assumed. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">Session
         * Policies</a> in the <i>IAM User Guide</i>.</p> <p>Calling
         * <code>AssumeRoleWithSAML</code> does not require the use of Amazon Web Services
         * security credentials. The identity of the caller is validated by using keys in
         * the metadata document that is uploaded for the SAML provider entity for your
         * identity provider. </p>  <p>Calling <code>AssumeRoleWithSAML</code>
         * can result in an entry in your CloudTrail logs. The entry includes the value in
         * the <code>NameID</code> element of the SAML assertion. We recommend that you use
         * a <code>NameIDType</code> that is not associated with any personally
         * identifiable information (PII). For example, you could instead use the
         * persistent identifier
         * (<code>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</code>).</p>
         *  <p> <b>Tags</b> </p> <p>(Optional) You can configure your IdP to
         * pass attributes into your SAML assertion as session tags. Each session tag
         * consists of a key name and an associated value. For more information about
         * session tags, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html">Passing
         * Session Tags in STS</a> in the <i>IAM User Guide</i>.</p> <p>You can pass up to
         * 50 session tags. The plaintext session tag keys can’t exceed 128 characters and
         * the values can’t exceed 256 characters. For these and additional limits, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-limits.html#reference_iam-limits-entity-length">IAM
         * and STS Character Limits</a> in the <i>IAM User Guide</i>.</p>  <p>An
         * Amazon Web Services conversion compresses the passed inline session policy,
         * managed policy ARNs, and session tags into a packed binary format that has a
         * separate limit. Your request can fail for this limit even if your plaintext
         * meets the other requirements. The <code>PackedPolicySize</code> response element
         * indicates by percentage how close the policies and tags for your request are to
         * the upper size limit.</p>  <p>You can pass a session tag with the same
         * key as a tag that is attached to the role. When you do, session tags override
         * the role's tags with the same key.</p> <p>An administrator must grant you the
         * permissions necessary to pass session tags. The administrator can also create
         * granular permissions to allow you to pass only specific session tags. For more
         * information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_attribute-based-access-control.html">Tutorial:
         * Using Tags for Attribute-Based Access Control</a> in the <i>IAM User
         * Guide</i>.</p> <p>You can set the session tags as transitive. Transitive tags
         * persist during role chaining. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html#id_session-tags_role-chaining">Chaining
         * Roles with Session Tags</a> in the <i>IAM User Guide</i>.</p> <p> <b>SAML
         * Configuration</b> </p> <p>Before your application can call
         * <code>AssumeRoleWithSAML</code>, you must configure your SAML identity provider
         * (IdP) to issue the claims required by Amazon Web Services. Additionally, you
         * must use Identity and Access Management (IAM) to create a SAML provider entity
         * in your Amazon Web Services account that represents your identity provider. You
         * must also create an IAM role that specifies this SAML provider in its trust
         * policy. </p> <p>For more information, see the following resources:</p> <ul> <li>
         * <p> <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_saml.html">About
         * SAML 2.0-based Federation</a> in the <i>IAM User Guide</i>. </p> </li> <li> <p>
         * <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_saml.html">Creating
         * SAML Identity Providers</a> in the <i>IAM User Guide</i>. </p> </li> <li> <p> <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_saml_relying-party.html">Configuring
         * a Relying Party and Claims</a> in the <i>IAM User Guide</i>. </p> </li> <li> <p>
         * <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-idp_saml.html">Creating
         * a Role for SAML 2.0 Federation</a> in the <i>IAM User Guide</i>. </p> </li>
         * </ul><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumeRoleWithSAML">AWS
         * API Reference</a></p>
         */
        virtual Model::AssumeRoleWithSAMLOutcome AssumeRoleWithSAML(const Model::AssumeRoleWithSAMLRequest& request) const;

        /**
         * A Callable wrapper for AssumeRoleWithSAML that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename AssumeRoleWithSAMLRequestT = Model::AssumeRoleWithSAMLRequest>
        Model::AssumeRoleWithSAMLOutcomeCallable AssumeRoleWithSAMLCallable(const AssumeRoleWithSAMLRequestT& request) const
        {
            return SubmitCallable(&STSClient::AssumeRoleWithSAML, request);
        }

        /**
         * An Async wrapper for AssumeRoleWithSAML that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename AssumeRoleWithSAMLRequestT = Model::AssumeRoleWithSAMLRequest>
        void AssumeRoleWithSAMLAsync(const AssumeRoleWithSAMLRequestT& request, const AssumeRoleWithSAMLResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&STSClient::AssumeRoleWithSAML, request, handler, context);
        }

        /**
         * <p>Returns a set of temporary security credentials for users who have been
         * authenticated in a mobile or web application with a web identity provider.
         * Example providers include the OAuth 2.0 providers Login with Amazon and
         * Facebook, or any OpenID Connect-compatible identity provider such as Google or
         * <a
         * href="https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-identity.html">Amazon
         * Cognito federated identities</a>.</p>  <p>For mobile applications, we
         * recommend that you use Amazon Cognito. You can use Amazon Cognito with the <a
         * href="http://aws.amazon.com/sdkforios/">Amazon Web Services SDK for iOS
         * Developer Guide</a> and the <a
         * href="http://aws.amazon.com/sdkforandroid/">Amazon Web Services SDK for Android
         * Developer Guide</a> to uniquely identify a user. You can also supply the user
         * with a consistent identity throughout the lifetime of an application.</p> <p>To
         * learn more about Amazon Cognito, see <a
         * href="https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-identity.html">Amazon
         * Cognito identity pools</a> in <i>Amazon Cognito Developer Guide</i>.</p> 
         * <p>Calling <code>AssumeRoleWithWebIdentity</code> does not require the use of
         * Amazon Web Services security credentials. Therefore, you can distribute an
         * application (for example, on mobile devices) that requests temporary security
         * credentials without including long-term Amazon Web Services credentials in the
         * application. You also don't need to deploy server-based proxy services that use
         * long-term Amazon Web Services credentials. Instead, the identity of the caller
         * is validated by using a token from the web identity provider. For a comparison
         * of <code>AssumeRoleWithWebIdentity</code> with the other API operations that
         * produce temporary credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html">Requesting
         * Temporary Security Credentials</a> and <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#stsapi_comparison">Comparing
         * the Amazon Web Services STS API operations</a> in the <i>IAM User Guide</i>.</p>
         * <p>The temporary security credentials returned by this API consist of an access
         * key ID, a secret access key, and a security token. Applications can use these
         * temporary security credentials to sign calls to Amazon Web Services service API
         * operations.</p> <p> <b>Session Duration</b> </p> <p>By default, the temporary
         * security credentials created by <code>AssumeRoleWithWebIdentity</code> last for
         * one hour. However, you can use the optional <code>DurationSeconds</code>
         * parameter to specify the duration of your session. You can provide a value from
         * 900 seconds (15 minutes) up to the maximum session duration setting for the
         * role. This setting can have a value from 1 hour to 12 hours. To learn how to
         * view the maximum value for your role, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html#id_roles_use_view-role-max-session">View
         * the Maximum Session Duration Setting for a Role</a> in the <i>IAM User
         * Guide</i>. The maximum session duration limit applies when you use the
         * <code>AssumeRole*</code> API operations or the <code>assume-role*</code> CLI
         * commands. However the limit does not apply when you use those operations to
         * create a console URL. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html">Using
         * IAM Roles</a> in the <i>IAM User Guide</i>. </p> <p> <b>Permissions</b> </p>
         * <p>The temporary security credentials created by
         * <code>AssumeRoleWithWebIdentity</code> can be used to make API calls to any
         * Amazon Web Services service with the following exception: you cannot call the
         * STS <code>GetFederationToken</code> or <code>GetSessionToken</code> API
         * operations.</p> <p>(Optional) You can pass inline or managed <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
         * policies</a> to this operation. You can pass a single JSON policy document to
         * use as an inline session policy. You can also specify up to 10 managed policy
         * Amazon Resource Names (ARNs) to use as managed session policies. The plaintext
         * that you use for both inline and managed session policies can't exceed 2,048
         * characters. Passing policies to this operation returns new temporary
         * credentials. The resulting session's permissions are the intersection of the
         * role's identity-based policy and the session policies. You can use the role's
         * temporary credentials in subsequent Amazon Web Services API calls to access
         * resources in the account that owns the role. You cannot use session policies to
         * grant more permissions than those allowed by the identity-based policy of the
         * role that is being assumed. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">Session
         * Policies</a> in the <i>IAM User Guide</i>.</p> <p> <b>Tags</b> </p>
         * <p>(Optional) You can configure your IdP to pass attributes into your web
         * identity token as session tags. Each session tag consists of a key name and an
         * associated value. For more information about session tags, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html">Passing
         * Session Tags in STS</a> in the <i>IAM User Guide</i>.</p> <p>You can pass up to
         * 50 session tags. The plaintext session tag keys can’t exceed 128 characters and
         * the values can’t exceed 256 characters. For these and additional limits, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-limits.html#reference_iam-limits-entity-length">IAM
         * and STS Character Limits</a> in the <i>IAM User Guide</i>.</p>  <p>An
         * Amazon Web Services conversion compresses the passed inline session policy,
         * managed policy ARNs, and session tags into a packed binary format that has a
         * separate limit. Your request can fail for this limit even if your plaintext
         * meets the other requirements. The <code>PackedPolicySize</code> response element
         * indicates by percentage how close the policies and tags for your request are to
         * the upper size limit.</p>  <p>You can pass a session tag with the same
         * key as a tag that is attached to the role. When you do, the session tag
         * overrides the role tag with the same key.</p> <p>An administrator must grant you
         * the permissions necessary to pass session tags. The administrator can also
         * create granular permissions to allow you to pass only specific session tags. For
         * more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_attribute-based-access-control.html">Tutorial:
         * Using Tags for Attribute-Based Access Control</a> in the <i>IAM User
         * Guide</i>.</p> <p>You can set the session tags as transitive. Transitive tags
         * persist during role chaining. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html#id_session-tags_role-chaining">Chaining
         * Roles with Session Tags</a> in the <i>IAM User Guide</i>.</p> <p>
         * <b>Identities</b> </p> <p>Before your application can call
         * <code>AssumeRoleWithWebIdentity</code>, you must have an identity token from a
         * supported identity provider and create a role that the application can assume.
         * The role that your application assumes must trust the identity provider that is
         * associated with the identity token. In other words, the identity provider must
         * be specified in the role's trust policy. </p>  <p>Calling
         * <code>AssumeRoleWithWebIdentity</code> can result in an entry in your CloudTrail
         * logs. The entry includes the <a
         * href="http://openid.net/specs/openid-connect-core-1_0.html#Claims">Subject</a>
         * of the provided web identity token. We recommend that you avoid using any
         * personally identifiable information (PII) in this field. For example, you could
         * instead use a GUID or a pairwise identifier, as <a
         * href="http://openid.net/specs/openid-connect-core-1_0.html#SubjectIDTypes">suggested
         * in the OIDC specification</a>.</p>  <p>For more information about
         * how to use web identity federation and the
         * <code>AssumeRoleWithWebIdentity</code> API, see the following resources: </p>
         * <ul> <li> <p> <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc_manual.html">Using
         * Web Identity Federation API Operations for Mobile Apps</a> and <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_assumerolewithwebidentity">Federation
         * Through a Web-based Identity Provider</a>. </p> </li> <li> <p> <a
         * href="https://aws.amazon.com/blogs/aws/the-aws-web-identity-federation-playground/">
         * Web Identity Federation Playground</a>. Walk through the process of
         * authenticating through Login with Amazon, Facebook, or Google, getting temporary
         * security credentials, and then using those credentials to make a request to
         * Amazon Web Services. </p> </li> <li> <p> <a
         * href="http://aws.amazon.com/sdkforios/">Amazon Web Services SDK for iOS
         * Developer Guide</a> and <a href="http://aws.amazon.com/sdkforandroid/">Amazon
         * Web Services SDK for Android Developer Guide</a>. These toolkits contain sample
         * apps that show how to invoke the identity providers. The toolkits then show how
         * to use the information from these providers to get and use temporary security
         * credentials. </p> </li> <li> <p> <a
         * href="http://aws.amazon.com/articles/web-identity-federation-with-mobile-applications">Web
         * Identity Federation with Mobile Applications</a>. This article discusses web
         * identity federation and shows an example of how to use web identity federation
         * to get access to content in Amazon S3. </p> </li> </ul><p><h3>See Also:</h3>  
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumeRoleWithWebIdentity">AWS
         * API Reference</a></p>
         */
        virtual Model::AssumeRoleWithWebIdentityOutcome AssumeRoleWithWebIdentity(const Model::AssumeRoleWithWebIdentityRequest& request) const;

        /**
         * A Callable wrapper for AssumeRoleWithWebIdentity that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename AssumeRoleWithWebIdentityRequestT = Model::AssumeRoleWithWebIdentityRequest>
        Model::AssumeRoleWithWebIdentityOutcomeCallable AssumeRoleWithWebIdentityCallable(const AssumeRoleWithWebIdentityRequestT& request) const
        {
            return SubmitCallable(&STSClient::AssumeRoleWithWebIdentity, request);
        }

        /**
         * An Async wrapper for AssumeRoleWithWebIdentity that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename AssumeRoleWithWebIdentityRequestT = Model::AssumeRoleWithWebIdentityRequest>
        void AssumeRoleWithWebIdentityAsync(const AssumeRoleWithWebIdentityRequestT& request, const AssumeRoleWithWebIdentityResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&STSClient::AssumeRoleWithWebIdentity, request, handler, context);
        }

        /**
         * <p>Decodes additional information about the authorization status of a request
         * from an encoded message returned in response to an Amazon Web Services
         * request.</p> <p>For example, if a user is not authorized to perform an operation
         * that he or she has requested, the request returns a
         * <code>Client.UnauthorizedOperation</code> response (an HTTP 403 response). Some
         * Amazon Web Services operations additionally return an encoded message that can
         * provide details about this authorization failure. </p>  <p>Only certain
         * Amazon Web Services operations return an encoded authorization message. The
         * documentation for an individual operation indicates whether that operation
         * returns an encoded message in addition to returning an HTTP code.</p> 
         * <p>The message is encoded because the details of the authorization status can
         * contain privileged information that the user who requested the operation should
         * not see. To decode an authorization status message, a user must be granted
         * permissions through an IAM <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html">policy</a>
         * to request the <code>DecodeAuthorizationMessage</code>
         * (<code>sts:DecodeAuthorizationMessage</code>) action. </p> <p>The decoded
         * message includes the following type of information:</p> <ul> <li> <p>Whether the
         * request was denied due to an explicit deny or due to the absence of an explicit
         * allow. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html#policy-eval-denyallow">Determining
         * Whether a Request is Allowed or Denied</a> in the <i>IAM User Guide</i>. </p>
         * </li> <li> <p>The principal who made the request.</p> </li> <li> <p>The
         * requested action.</p> </li> <li> <p>The requested resource.</p> </li> <li>
         * <p>The values of condition keys in the context of the user's request.</p> </li>
         * </ul><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/DecodeAuthorizationMessage">AWS
         * API Reference</a></p>
         */
        virtual Model::DecodeAuthorizationMessageOutcome DecodeAuthorizationMessage(const Model::DecodeAuthorizationMessageRequest& request) const;

        /**
         * A Callable wrapper for DecodeAuthorizationMessage that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename DecodeAuthorizationMessageRequestT = Model::DecodeAuthorizationMessageRequest>
        Model::DecodeAuthorizationMessageOutcomeCallable DecodeAuthorizationMessageCallable(const DecodeAuthorizationMessageRequestT& request) const
        {
            return SubmitCallable(&STSClient::DecodeAuthorizationMessage, request);
        }

        /**
         * An Async wrapper for DecodeAuthorizationMessage that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename DecodeAuthorizationMessageRequestT = Model::DecodeAuthorizationMessageRequest>
        void DecodeAuthorizationMessageAsync(const DecodeAuthorizationMessageRequestT& request, const DecodeAuthorizationMessageResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&STSClient::DecodeAuthorizationMessage, request, handler, context);
        }

        /**
         * <p>Returns the account identifier for the specified access key ID.</p> <p>Access
         * keys consist of two parts: an access key ID (for example,
         * <code>AKIAIOSFODNN7EXAMPLE</code>) and a secret access key (for example,
         * <code>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</code>). For more information
         * about access keys, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html">Managing
         * Access Keys for IAM Users</a> in the <i>IAM User Guide</i>.</p> <p>When you pass
         * an access key ID to this operation, it returns the ID of the Amazon Web Services
         * account to which the keys belong. Access key IDs beginning with
         * <code>AKIA</code> are long-term credentials for an IAM user or the Amazon Web
         * Services account root user. Access key IDs beginning with <code>ASIA</code> are
         * temporary credentials that are created using STS operations. If the account in
         * the response belongs to you, you can sign in as the root user and review your
         * root user access keys. Then, you can pull a <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_getting-report.html">credentials
         * report</a> to learn which IAM user owns the keys. To learn who requested the
         * temporary credentials for an <code>ASIA</code> access key, view the STS events
         * in your <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/cloudtrail-integration.html">CloudTrail
         * logs</a> in the <i>IAM User Guide</i>.</p> <p>This operation does not indicate
         * the state of the access key. The key might be active, inactive, or deleted.
         * Active keys might not have permissions to perform an operation. Providing a
         * deleted access key might return an error that the key doesn't
         * exist.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/GetAccessKeyInfo">AWS
         * API Reference</a></p>
         */
        virtual Model::GetAccessKeyInfoOutcome GetAccessKeyInfo(const Model::GetAccessKeyInfoRequest& request) const;

        /**
         * A Callable wrapper for GetAccessKeyInfo that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename GetAccessKeyInfoRequestT = Model::GetAccessKeyInfoRequest>
        Model::GetAccessKeyInfoOutcomeCallable GetAccessKeyInfoCallable(const GetAccessKeyInfoRequestT& request) const
        {
            return SubmitCallable(&STSClient::GetAccessKeyInfo, request);
        }

        /**
         * An Async wrapper for GetAccessKeyInfo that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename GetAccessKeyInfoRequestT = Model::GetAccessKeyInfoRequest>
        void GetAccessKeyInfoAsync(const GetAccessKeyInfoRequestT& request, const GetAccessKeyInfoResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&STSClient::GetAccessKeyInfo, request, handler, context);
        }

        /**
         * <p>Returns details about the IAM user or role whose credentials are used to call
         * the operation.</p>  <p>No permissions are required to perform this
         * operation. If an administrator attaches a policy to your identity that
         * explicitly denies access to the <code>sts:GetCallerIdentity</code> action, you
         * can still perform this operation. Permissions are not required because the same
         * information is returned when access is denied. To view an example response, see
         * <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_access-denied-delete-mfa">I
         * Am Not Authorized to Perform: iam:DeleteVirtualMFADevice</a> in the <i>IAM User
         * Guide</i>.</p> <p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/GetCallerIdentity">AWS
         * API Reference</a></p>
         */
        virtual Model::GetCallerIdentityOutcome GetCallerIdentity(const Model::GetCallerIdentityRequest& request = {}) const;

        /**
         * A Callable wrapper for GetCallerIdentity that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename GetCallerIdentityRequestT = Model::GetCallerIdentityRequest>
        Model::GetCallerIdentityOutcomeCallable GetCallerIdentityCallable(const GetCallerIdentityRequestT& request = {}) const
        {
            return SubmitCallable(&STSClient::GetCallerIdentity, request);
        }

        /**
         * An Async wrapper for GetCallerIdentity that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename GetCallerIdentityRequestT = Model::GetCallerIdentityRequest>
        void GetCallerIdentityAsync(const GetCallerIdentityResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr, const GetCallerIdentityRequestT& request = {}) const
        {
            return SubmitAsync(&STSClient::GetCallerIdentity, request, handler, context);
        }

        /**
         * <p>Returns a set of temporary security credentials (consisting of an access key
         * ID, a secret access key, and a security token) for a user. A typical use is in a
         * proxy application that gets temporary security credentials on behalf of
         * distributed applications inside a corporate network.</p> <p>You must call the
         * <code>GetFederationToken</code> operation using the long-term security
         * credentials of an IAM user. As a result, this call is appropriate in contexts
         * where those credentials can be safeguarded, usually in a server-based
         * application. For a comparison of <code>GetFederationToken</code> with the other
         * API operations that produce temporary credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html">Requesting
         * Temporary Security Credentials</a> and <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#stsapi_comparison">Comparing
         * the Amazon Web Services STS API operations</a> in the <i>IAM User Guide</i>.</p>
         * <p>Although it is possible to call <code>GetFederationToken</code> using the
         * security credentials of an Amazon Web Services account root user rather than an
         * IAM user that you create for the purpose of a proxy application, we do not
         * recommend it. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html#lock-away-credentials">Safeguard
         * your root user credentials and don't use them for everyday tasks</a> in the
         * <i>IAM User Guide</i>. </p>  <p>You can create a mobile-based or
         * browser-based app that can authenticate users using a web identity provider like
         * Login with Amazon, Facebook, Google, or an OpenID Connect-compatible identity
         * provider. In this case, we recommend that you use <a
         * href="http://aws.amazon.com/cognito/">Amazon Cognito</a> or
         * <code>AssumeRoleWithWebIdentity</code>. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_assumerolewithwebidentity">Federation
         * Through a Web-based Identity Provider</a> in the <i>IAM User Guide</i>.</p>
         *  <p> <b>Session duration</b> </p> <p>The temporary credentials are valid
         * for the specified duration, from 900 seconds (15 minutes) up to a maximum of
         * 129,600 seconds (36 hours). The default session duration is 43,200 seconds (12
         * hours). Temporary credentials obtained by using the root user credentials have a
         * maximum duration of 3,600 seconds (1 hour).</p> <p> <b>Permissions</b> </p>
         * <p>You can use the temporary credentials created by
         * <code>GetFederationToken</code> in any Amazon Web Services service with the
         * following exceptions:</p> <ul> <li> <p>You cannot call any IAM operations using
         * the CLI or the Amazon Web Services API. This limitation does not apply to
         * console sessions.</p> </li> <li> <p>You cannot call any STS operations except
         * <code>GetCallerIdentity</code>.</p> </li> </ul> <p>You can use temporary
         * credentials for single sign-on (SSO) to the console.</p> <p>You must pass an
         * inline or managed <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
         * policy</a> to this operation. You can pass a single JSON policy document to use
         * as an inline session policy. You can also specify up to 10 managed policy Amazon
         * Resource Names (ARNs) to use as managed session policies. The plaintext that you
         * use for both inline and managed session policies can't exceed 2,048
         * characters.</p> <p>Though the session policy parameters are optional, if you do
         * not pass a policy, then the resulting federated user session has no permissions.
         * When you pass session policies, the session permissions are the intersection of
         * the IAM user policies and the session policies that you pass. This gives you a
         * way to further restrict the permissions for a federated user. You cannot use
         * session policies to grant more permissions than those that are defined in the
         * permissions policy of the IAM user. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">Session
         * Policies</a> in the <i>IAM User Guide</i>. For information about using
         * <code>GetFederationToken</code> to create temporary security credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_getfederationtoken">GetFederationToken—Federation
         * Through a Custom Identity Broker</a>. </p> <p>You can use the credentials to
         * access a resource that has a resource-based policy. If that policy specifically
         * references the federated user session in the <code>Principal</code> element of
         * the policy, the session has the permissions allowed by the policy. These
         * permissions are granted in addition to the permissions granted by the session
         * policies.</p> <p> <b>Tags</b> </p> <p>(Optional) You can pass tag key-value
         * pairs to your session. These are called session tags. For more information about
         * session tags, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html">Passing
         * Session Tags in STS</a> in the <i>IAM User Guide</i>.</p>  <p>You can
         * create a mobile-based or browser-based app that can authenticate users using a
         * web identity provider like Login with Amazon, Facebook, Google, or an OpenID
         * Connect-compatible identity provider. In this case, we recommend that you use <a
         * href="http://aws.amazon.com/cognito/">Amazon Cognito</a> or
         * <code>AssumeRoleWithWebIdentity</code>. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_assumerolewithwebidentity">Federation
         * Through a Web-based Identity Provider</a> in the <i>IAM User Guide</i>.</p>
         *  <p>An administrator must grant you the permissions necessary to pass
         * session tags. The administrator can also create granular permissions to allow
         * you to pass only specific session tags. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_attribute-based-access-control.html">Tutorial:
         * Using Tags for Attribute-Based Access Control</a> in the <i>IAM User
         * Guide</i>.</p> <p>Tag key–value pairs are not case sensitive, but case is
         * preserved. This means that you cannot have separate <code>Department</code> and
         * <code>department</code> tag keys. Assume that the user that you are federating
         * has the <code>Department</code>=<code>Marketing</code> tag and you pass the
         * <code>department</code>=<code>engineering</code> session tag.
         * <code>Department</code> and <code>department</code> are not saved as separate
         * tags, and the session tag passed in the request takes precedence over the user
         * tag.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/GetFederationToken">AWS
         * API Reference</a></p>
         */
        virtual Model::GetFederationTokenOutcome GetFederationToken(const Model::GetFederationTokenRequest& request) const;

        /**
         * A Callable wrapper for GetFederationToken that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename GetFederationTokenRequestT = Model::GetFederationTokenRequest>
        Model::GetFederationTokenOutcomeCallable GetFederationTokenCallable(const GetFederationTokenRequestT& request) const
        {
            return SubmitCallable(&STSClient::GetFederationToken, request);
        }

        /**
         * An Async wrapper for GetFederationToken that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename GetFederationTokenRequestT = Model::GetFederationTokenRequest>
        void GetFederationTokenAsync(const GetFederationTokenRequestT& request, const GetFederationTokenResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const
        {
            return SubmitAsync(&STSClient::GetFederationToken, request, handler, context);
        }

        /**
         * <p>Returns a set of temporary credentials for an Amazon Web Services account or
         * IAM user. The credentials consist of an access key ID, a secret access key, and
         * a security token. Typically, you use <code>GetSessionToken</code> if you want to
         * use MFA to protect programmatic calls to specific Amazon Web Services API
         * operations like Amazon EC2 <code>StopInstances</code>.</p> <p>MFA-enabled IAM
         * users must call <code>GetSessionToken</code> and submit an MFA code that is
         * associated with their MFA device. Using the temporary security credentials that
         * the call returns, IAM users can then make programmatic calls to API operations
         * that require MFA authentication. An incorrect MFA code causes the API to return
         * an access denied error. For a comparison of <code>GetSessionToken</code> with
         * the other API operations that produce temporary credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html">Requesting
         * Temporary Security Credentials</a> and <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#stsapi_comparison">Comparing
         * the Amazon Web Services STS API operations</a> in the <i>IAM User Guide</i>.</p>
         *  <p>No permissions are required for users to perform this operation. The
         * purpose of the <code>sts:GetSessionToken</code> operation is to authenticate the
         * user using MFA. You cannot use policies to control authentication operations.
         * For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_control-access_getsessiontoken.html">Permissions
         * for GetSessionToken</a> in the <i>IAM User Guide</i>.</p>  <p> <b>Session
         * Duration</b> </p> <p>The <code>GetSessionToken</code> operation must be called
         * by using the long-term Amazon Web Services security credentials of an IAM user.
         * Credentials that are created by IAM users are valid for the duration that you
         * specify. This duration can range from 900 seconds (15 minutes) up to a maximum
         * of 129,600 seconds (36 hours), with a default of 43,200 seconds (12 hours).
         * Credentials based on account credentials can range from 900 seconds (15 minutes)
         * up to 3,600 seconds (1 hour), with a default of 1 hour. </p> <p>
         * <b>Permissions</b> </p> <p>The temporary security credentials created by
         * <code>GetSessionToken</code> can be used to make API calls to any Amazon Web
         * Services service with the following exceptions:</p> <ul> <li> <p>You cannot call
         * any IAM API operations unless MFA authentication information is included in the
         * request.</p> </li> <li> <p>You cannot call any STS API <i>except</i>
         * <code>AssumeRole</code> or <code>GetCallerIdentity</code>.</p> </li> </ul>
         * <p>The credentials that <code>GetSessionToken</code> returns are based on
         * permissions associated with the IAM user whose credentials were used to call the
         * operation. The temporary credentials have the same permissions as the IAM
         * user.</p>  <p>Although it is possible to call <code>GetSessionToken</code>
         * using the security credentials of an Amazon Web Services account root user
         * rather than an IAM user, we do not recommend it. If <code>GetSessionToken</code>
         * is called using root user credentials, the temporary credentials have root user
         * permissions. For more information, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html#lock-away-credentials">Safeguard
         * your root user credentials and don't use them for everyday tasks</a> in the
         * <i>IAM User Guide</i> </p>  <p>For more information about using
         * <code>GetSessionToken</code> to create temporary credentials, see <a
         * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html#api_getsessiontoken">Temporary
         * Credentials for Users in Untrusted Environments</a> in the <i>IAM User
         * Guide</i>. </p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/GetSessionToken">AWS
         * API Reference</a></p>
         */
        virtual Model::GetSessionTokenOutcome GetSessionToken(const Model::GetSessionTokenRequest& request = {}) const;

        /**
         * A Callable wrapper for GetSessionToken that returns a future to the operation so that it can be executed in parallel to other requests.
         */
        template<typename GetSessionTokenRequestT = Model::GetSessionTokenRequest>
        Model::GetSessionTokenOutcomeCallable GetSessionTokenCallable(const GetSessionTokenRequestT& request = {}) const
        {
            return SubmitCallable(&STSClient::GetSessionToken, request);
        }

        /**
         * An Async wrapper for GetSessionToken that queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        template<typename GetSessionTokenRequestT = Model::GetSessionTokenRequest>
        void GetSessionTokenAsync(const GetSessionTokenResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr, const GetSessionTokenRequestT& request = {}) const
        {
            return SubmitAsync(&STSClient::GetSessionToken, request, handler, context);
        }


        void OverrideEndpoint(const Aws::String& endpoint);
        std::shared_ptr<STSEndpointProviderBase>& accessEndpointProvider();
  private:
        friend class Aws::Client::ClientWithAsyncTemplateMethods<STSClient>;
        void init(const STSClientConfiguration& clientConfiguration);

        STSClientConfiguration m_clientConfiguration;
        std::shared_ptr<STSEndpointProviderBase> m_endpointProvider;
  };

} // namespace STS
} // namespace Aws
