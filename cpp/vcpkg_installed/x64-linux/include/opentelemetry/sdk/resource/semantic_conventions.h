/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
  DO NOT EDIT, this is an Auto-generated file
  from buildscripts/semantic-convention/templates/SemanticAttributes.h.j2
*/

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{

namespace SemanticConventions
{
/**
 * The URL of the OpenTelemetry schema for these keys and values.
 */
static constexpr const char *kSchemaUrl = "https://opentelemetry.io/schemas/1.22.0";

/**
 * Uniquely identifies the framework API revision offered by a version ({@code os.version}) of the
 * android operating system. More information can be found <a
 * href="https://developer.android.com/guide/topics/manifest/uses-sdk-element#ApiLevels">here</a>.
 */
static constexpr const char *kAndroidOsApiLevel = "android.os.api_level";

/**
 * Array of brand name and version separated by a space
 *
 * <p>Notes:
  <ul> <li>This value is intended to be taken from the <a
 href="https://wicg.github.io/ua-client-hints/#interface">UA client hints API</a> ({@code
 navigator.userAgentData.brands}).</li> </ul>
 */
static constexpr const char *kBrowserBrands = "browser.brands";

/**
 * Preferred language of the user using the browser
 *
 * <p>Notes:
  <ul> <li>This value is intended to be taken from the Navigator API {@code
 navigator.language}.</li> </ul>
 */
static constexpr const char *kBrowserLanguage = "browser.language";

/**
 * A boolean that is true if the browser is running on a mobile device
 *
 * <p>Notes:
  <ul> <li>This value is intended to be taken from the <a
 href="https://wicg.github.io/ua-client-hints/#interface">UA client hints API</a> ({@code
 navigator.userAgentData.mobile}). If unavailable, this attribute SHOULD be left unset.</li> </ul>
 */
static constexpr const char *kBrowserMobile = "browser.mobile";

/**
 * The platform on which the browser is running
 *
 * <p>Notes:
  <ul> <li>This value is intended to be taken from the <a
href="https://wicg.github.io/ua-client-hints/#interface">UA client hints API</a> ({@code
navigator.userAgentData.platform}). If unavailable, the legacy {@code navigator.platform} API SHOULD
NOT be used instead and this attribute SHOULD be left unset in order for the values to be
consistent. The list of possible values is defined in the <a
href="https://wicg.github.io/ua-client-hints/#sec-ch-ua-platform">W3C User-Agent Client Hints
specification</a>. Note that some (but not all) of these values can overlap with values in the <a
href="./os.md">{@code os.type} and {@code os.name} attributes</a>. However, for consistency, the
values in the {@code browser.platform} attribute should capture the exact value that the user agent
provides.</li> </ul>
 */
static constexpr const char *kBrowserPlatform = "browser.platform";

/**
 * The cloud account ID the resource is assigned to.
 */
static constexpr const char *kCloudAccountId = "cloud.account.id";

/**
 * Cloud regions often have multiple, isolated locations known as zones to increase availability.
 Availability zone represents the zone where the resource is running.
 *
 * <p>Notes:
  <ul> <li>Availability zones are called &quot;zones&quot; on Alibaba Cloud and Google Cloud.</li>
 </ul>
 */
static constexpr const char *kCloudAvailabilityZone = "cloud.availability_zone";

/**
 * The cloud platform in use.
 *
 * <p>Notes:
  <ul> <li>The prefix of the service SHOULD match the one specified in {@code cloud.provider}.</li>
 </ul>
 */
static constexpr const char *kCloudPlatform = "cloud.platform";

/**
 * Name of the cloud provider.
 */
static constexpr const char *kCloudProvider = "cloud.provider";

/**
 * The geographical region the resource is running.
 *
 * <p>Notes:
  <ul> <li>Refer to your provider's docs to see the available regions, for example <a
 href="https://www.alibabacloud.com/help/doc-detail/40654.htm">Alibaba Cloud regions</a>, <a
 href="https://aws.amazon.com/about-aws/global-infrastructure/regions_az/">AWS regions</a>, <a
 href="https://azure.microsoft.com/en-us/global-infrastructure/geographies/">Azure regions</a>, <a
 href="https://cloud.google.com/about/locations">Google Cloud regions</a>, or <a
 href="https://www.tencentcloud.com/document/product/213/6091">Tencent Cloud regions</a>.</li> </ul>
 */
static constexpr const char *kCloudRegion = "cloud.region";

/**
 * Cloud provider-specific native identifier of the monitored cloud resource (e.g. an <a
href="https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html">ARN</a> on AWS, a
<a href="https://learn.microsoft.com/en-us/rest/api/resources/resources/get-by-id">fully qualified
resource ID</a> on Azure, a <a
href="https://cloud.google.com/apis/design/resource_names#full_resource_name">full resource name</a>
on GCP)
 *
 * <p>Notes:
  <ul> <li>On some cloud providers, it may not be possible to determine the full ID at startup,
so it may be necessary to set {@code cloud.resource_id} as a span attribute instead.</li><li>The
exact value to use for {@code cloud.resource_id} depends on the cloud provider. The following
well-known definitions MUST be used if you set this attribute and they apply:</li><li><strong>AWS
Lambda:</strong> The function <a
href="https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html">ARN</a>. Take care
not to use the &quot;invoked ARN&quot; directly but replace any <a
href="https://docs.aws.amazon.com/lambda/latest/dg/configuration-aliases.html">alias suffix</a> with
the resolved function version, as the same runtime instance may be invokable with multiple different
aliases.</li> <li><strong>GCP:</strong> The <a
href="https://cloud.google.com/iam/docs/full-resource-names">URI of the resource</a></li>
<li><strong>Azure:</strong> The <a
href="https://docs.microsoft.com/en-us/rest/api/resources/resources/get-by-id">Fully Qualified
Resource ID</a> of the invoked function, <em>not</em> the function app, having the form
{@code
/subscriptions/<SUBSCIPTION_GUID>/resourceGroups/<RG>/providers/Microsoft.Web/sites/<FUNCAPP>/functions/<FUNC>}.
This means that a span attribute MUST be used, as an Azure function app can host multiple functions
that would usually share a TracerProvider.</li>
 </ul>
 */
static constexpr const char *kCloudResourceId = "cloud.resource_id";

/**
 * The ARN of an <a
 * href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/clusters.html">ECS cluster</a>.
 */
static constexpr const char *kAwsEcsClusterArn = "aws.ecs.cluster.arn";

/**
 * The Amazon Resource Name (ARN) of an <a
 * href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_instances.html">ECS
 * container instance</a>.
 */
static constexpr const char *kAwsEcsContainerArn = "aws.ecs.container.arn";

/**
 * The <a
 * href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/launch_types.html">launch
 * type</a> for an ECS task.
 */
static constexpr const char *kAwsEcsLaunchtype = "aws.ecs.launchtype";

/**
 * The ARN of an <a
 * href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definitions.html">ECS task
 * definition</a>.
 */
static constexpr const char *kAwsEcsTaskArn = "aws.ecs.task.arn";

/**
 * The task definition family this task definition is a member of.
 */
static constexpr const char *kAwsEcsTaskFamily = "aws.ecs.task.family";

/**
 * The revision for this task definition.
 */
static constexpr const char *kAwsEcsTaskRevision = "aws.ecs.task.revision";

/**
 * The ARN of an EKS cluster.
 */
static constexpr const char *kAwsEksClusterArn = "aws.eks.cluster.arn";

/**
 * The Amazon Resource Name(s) (ARN) of the AWS log group(s).
 *
 * <p>Notes:
  <ul> <li>See the <a
 href="https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/iam-access-control-overview-cwl.html#CWL_ARN_Format">log
 group ARN format documentation</a>.</li> </ul>
 */
static constexpr const char *kAwsLogGroupArns = "aws.log.group.arns";

/**
 * The name(s) of the AWS log group(s) an application is writing to.
 *
 * <p>Notes:
  <ul> <li>Multiple log groups must be supported for cases like multi-container applications, where
 a single application has sidecar containers, and each write to their own log group.</li> </ul>
 */
static constexpr const char *kAwsLogGroupNames = "aws.log.group.names";

/**
 * The ARN(s) of the AWS log stream(s).
 *
 * <p>Notes:
  <ul> <li>See the <a
 href="https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/iam-access-control-overview-cwl.html#CWL_ARN_Format">log
 stream ARN format documentation</a>. One log group can contain several log streams, so these ARNs
 necessarily identify both a log group and a log stream.</li> </ul>
 */
static constexpr const char *kAwsLogStreamArns = "aws.log.stream.arns";

/**
 * The name(s) of the AWS log stream(s) an application is writing to.
 */
static constexpr const char *kAwsLogStreamNames = "aws.log.stream.names";

/**
 * The name of the Cloud Run <a
 * href="https://cloud.google.com/run/docs/managing/job-executions">execution</a> being run for the
 * Job, as set by the <a
 * href="https://cloud.google.com/run/docs/container-contract#jobs-env-vars">{@code
 * CLOUD_RUN_EXECUTION}</a> environment variable.
 */
static constexpr const char *kGcpCloudRunJobExecution = "gcp.cloud_run.job.execution";

/**
 * The index for a task within an execution as provided by the <a
 * href="https://cloud.google.com/run/docs/container-contract#jobs-env-vars">{@code
 * CLOUD_RUN_TASK_INDEX}</a> environment variable.
 */
static constexpr const char *kGcpCloudRunJobTaskIndex = "gcp.cloud_run.job.task_index";

/**
 * The hostname of a GCE instance. This is the full value of the default or <a
 * href="https://cloud.google.com/compute/docs/instances/custom-hostname-vm">custom hostname</a>.
 */
static constexpr const char *kGcpGceInstanceHostname = "gcp.gce.instance.hostname";

/**
 * The instance name of a GCE instance. This is the value provided by {@code host.name}, the visible
 * name of the instance in the Cloud Console UI, and the prefix for the default hostname of the
 * instance as defined by the <a
 * href="https://cloud.google.com/compute/docs/internal-dns#instance-fully-qualified-domain-names">default
 * internal DNS name</a>.
 */
static constexpr const char *kGcpGceInstanceName = "gcp.gce.instance.name";

/**
 * Unique identifier for the application
 */
static constexpr const char *kHerokuAppId = "heroku.app.id";

/**
 * Commit hash for the current release
 */
static constexpr const char *kHerokuReleaseCommit = "heroku.release.commit";

/**
 * Time and date the release was created
 */
static constexpr const char *kHerokuReleaseCreationTimestamp = "heroku.release.creation_timestamp";

/**
 * The command used to run the container (i.e. the command name).
 *
 * <p>Notes:
  <ul> <li>If using embedded credentials or sensitive data, it is recommended to remove them to
 prevent potential leakage.</li> </ul>
 */
static constexpr const char *kContainerCommand = "container.command";

/**
 * All the command arguments (including the command/executable itself) run by the container. [2]
 */
static constexpr const char *kContainerCommandArgs = "container.command_args";

/**
 * The full command run by the container as a single string representing the full command. [2]
 */
static constexpr const char *kContainerCommandLine = "container.command_line";

/**
 * Container ID. Usually a UUID, as for example used to <a
 * href="https://docs.docker.com/engine/reference/run/#container-identification">identify Docker
 * containers</a>. The UUID might be abbreviated.
 */
static constexpr const char *kContainerId = "container.id";

/**
 * Runtime specific image identifier. Usually a hash algorithm followed by a UUID.
 *
 * <p>Notes:
  <ul> <li>Docker defines a sha256 of the image id; {@code container.image.id} corresponds to the
{@code Image} field from the Docker container inspect <a
href="https://docs.docker.com/engine/api/v1.43/#tag/Container/operation/ContainerInspect">API</a>
endpoint. K8s defines a link to the container registry repository with digest {@code "imageID":
"registry.azurecr.io
/namespace/service/dockerfile@sha256:bdeabd40c3a8a492eaf9e8e44d0ebbb84bac7ee25ac0cf8a7159d25f62555625"}.
The ID is assinged by the container runtime and can vary in different environments. Consider using
{@code oci.manifest.digest} if it is important to identify the same image in different
environments/runtimes.</li> </ul>
 */
static constexpr const char *kContainerImageId = "container.image.id";

/**
 * Name of the image the container was built on.
 */
static constexpr const char *kContainerImageName = "container.image.name";

/**
 * Repo digests of the container image as provided by the container runtime.
 *
 * <p>Notes:
  <ul> <li><a
 href="https://docs.docker.com/engine/api/v1.43/#tag/Image/operation/ImageInspect">Docker</a> and <a
 href="https://github.com/kubernetes/cri-api/blob/c75ef5b473bbe2d0a4fc92f82235efd665ea8e9f/pkg/apis/runtime/v1/api.proto#L1237-L1238">CRI</a>
 report those under the {@code RepoDigests} field.</li> </ul>
 */
static constexpr const char *kContainerImageRepoDigests = "container.image.repo_digests";

/**
 * Container image tags. An example can be found in <a
 * href="https://docs.docker.com/engine/api/v1.43/#tag/Image/operation/ImageInspect">Docker Image
 * Inspect</a>. Should be only the {@code <tag>} section of the full name for example from {@code
 * registry.example.com/my-org/my-image:<tag>}.
 */
static constexpr const char *kContainerImageTags = "container.image.tags";

/**
 * Container name used by container runtime.
 */
static constexpr const char *kContainerName = "container.name";

/**
 * The container runtime managing this container.
 */
static constexpr const char *kContainerRuntime = "container.runtime";

/**
 * Name of the <a href="https://en.wikipedia.org/wiki/Deployment_environment">deployment
 * environment</a> (aka deployment tier).
 */
static constexpr const char *kDeploymentEnvironment = "deployment.environment";

/**
 * A unique identifier representing the device
 *
 * <p>Notes:
  <ul> <li>The device identifier MUST only be defined using the values outlined below. This value is
 not an advertising identifier and MUST NOT be used as such. On iOS (Swift or Objective-C), this
 value MUST be equal to the <a
 href="https://developer.apple.com/documentation/uikit/uidevice/1620059-identifierforvendor">vendor
 identifier</a>. On Android (Java or Kotlin), this value MUST be equal to the Firebase Installation
 ID or a globally unique UUID which is persisted across sessions in your application. More
 information can be found <a
 href="https://developer.android.com/training/articles/user-data-ids">here</a> on best practices and
 exact implementation details. Caution should be taken when storing personal data or anything which
 can identify a user. GDPR and data protection laws may apply, ensure you do your own due
 diligence.</li> </ul>
 */
static constexpr const char *kDeviceId = "device.id";

/**
 * The name of the device manufacturer
 *
 * <p>Notes:
  <ul> <li>The Android OS provides this field via <a
 href="https://developer.android.com/reference/android/os/Build#MANUFACTURER">Build</a>. iOS apps
 SHOULD hardcode the value {@code Apple}.</li> </ul>
 */
static constexpr const char *kDeviceManufacturer = "device.manufacturer";

/**
 * The model identifier for the device
 *
 * <p>Notes:
  <ul> <li>It's recommended this value represents a machine readable version of the model identifier
 rather than the market or consumer-friendly name of the device.</li> </ul>
 */
static constexpr const char *kDeviceModelIdentifier = "device.model.identifier";

/**
 * The marketing name for the device model
 *
 * <p>Notes:
  <ul> <li>It's recommended this value represents a human readable version of the device model
 rather than a machine readable alternative.</li> </ul>
 */
static constexpr const char *kDeviceModelName = "device.model.name";

/**
 * The execution environment ID as a string, that will be potentially reused for other invocations
 to the same function/function version.
 *
 * <p>Notes:
  <ul> <li><strong>AWS Lambda:</strong> Use the (full) log stream name.</li>
 </ul>
 */
static constexpr const char *kFaasInstance = "faas.instance";

/**
 * The amount of memory available to the serverless function converted to Bytes.
 *
 * <p>Notes:
  <ul> <li>It's recommended to set this attribute since e.g. too little memory can easily stop a
 Java AWS Lambda function from working correctly. On AWS Lambda, the environment variable {@code
 AWS_LAMBDA_FUNCTION_MEMORY_SIZE} provides this information (which must be multiplied by
 1,048,576).</li> </ul>
 */
static constexpr const char *kFaasMaxMemory = "faas.max_memory";

/**
 * The name of the single function that this runtime instance executes.
 *
 * <p>Notes:
  <ul> <li>This is the name of the function as configured/deployed on the FaaS
platform and is usually different from the name of the callback
function (which may be stored in the
<a href="/docs/general/attributes.md#source-code-attributes">{@code code.namespace}/{@code
code.function}</a> span attributes).</li><li>For some cloud providers, the above definition is
ambiguous. The following definition of function name MUST be used for this attribute (and
consequently the span name) for the listed cloud providers/products:</li><li><strong>Azure:</strong>
The full name {@code <FUNCAPP>/<FUNC>}, i.e., function app name followed by a forward slash followed
by the function name (this form can also be seen in the resource JSON for the function). This means
that a span attribute MUST be used, as an Azure function app can host multiple functions that would
usually share a TracerProvider (see also the {@code cloud.resource_id} attribute).</li>
 </ul>
 */
static constexpr const char *kFaasName = "faas.name";

/**
 * The immutable version of the function being executed.
 *
 * <p>Notes:
  <ul> <li>Depending on the cloud provider and platform, use:</li><li><strong>AWS Lambda:</strong>
The <a href="https://docs.aws.amazon.com/lambda/latest/dg/configuration-versions.html">function
version</a> (an integer represented as a decimal string).</li> <li><strong>Google Cloud Run
(Services):</strong> The <a href="https://cloud.google.com/run/docs/managing/revisions">revision</a>
(i.e., the function name plus the revision suffix).</li>
<li><strong>Google Cloud Functions:</strong> The value of the
<a
href="https://cloud.google.com/functions/docs/env-var#runtime_environment_variables_set_automatically">{@code
K_REVISION} environment variable</a>.</li> <li><strong>Azure Functions:</strong> Not applicable. Do
not set this attribute.</li>
 </ul>
 */
static constexpr const char *kFaasVersion = "faas.version";

/**
 * The CPU architecture the host system is running on.
 */
static constexpr const char *kHostArch = "host.arch";

/**
 * Unique host ID. For Cloud, this must be the instance_id assigned by the cloud provider. For
 * non-containerized systems, this should be the {@code machine-id}. See the table below for the
 * sources to use to determine the {@code machine-id} based on operating system.
 */
static constexpr const char *kHostId = "host.id";

/**
 * VM image ID or host OS image ID. For Cloud, this value is from the provider.
 */
static constexpr const char *kHostImageId = "host.image.id";

/**
 * Name of the VM image or OS install the host was instantiated from.
 */
static constexpr const char *kHostImageName = "host.image.name";

/**
 * The version string of the VM image or host OS as defined in <a
 * href="README.md#version-attributes">Version Attributes</a>.
 */
static constexpr const char *kHostImageVersion = "host.image.version";

/**
 * Available IP addresses of the host, excluding loopback interfaces.
 *
 * <p>Notes:
  <ul> <li>IPv4 Addresses MUST be specified in dotted-quad notation. IPv6 addresses MUST be
 specified in the <a href="https://www.rfc-editor.org/rfc/rfc5952.html">RFC 5952</a> format.</li>
 </ul>
 */
static constexpr const char *kHostIp = "host.ip";

/**
 * Name of the host. On Unix systems, it may contain what the hostname command returns, or the fully
 * qualified hostname, or another name specified by the user.
 */
static constexpr const char *kHostName = "host.name";

/**
 * Type of host. For Cloud, this must be the machine type.
 */
static constexpr const char *kHostType = "host.type";

/**
 * The amount of level 2 memory cache available to the processor (in Bytes).
 */
static constexpr const char *kHostCpuCacheL2Size = "host.cpu.cache.l2.size";

/**
 * Numeric value specifying the family or generation of the CPU.
 */
static constexpr const char *kHostCpuFamily = "host.cpu.family";

/**
 * Model identifier. It provides more granular information about the CPU, distinguishing it from
 * other CPUs within the same family.
 */
static constexpr const char *kHostCpuModelId = "host.cpu.model.id";

/**
 * Model designation of the processor.
 */
static constexpr const char *kHostCpuModelName = "host.cpu.model.name";

/**
 * Stepping or core revisions.
 */
static constexpr const char *kHostCpuStepping = "host.cpu.stepping";

/**
 * Processor manufacturer identifier. A maximum 12-character string.
 *
 * <p>Notes:
  <ul> <li><a href="https://wiki.osdev.org/CPUID">CPUID</a> command returns the vendor ID string in
 EBX, EDX and ECX registers. Writing these to memory in this order results in a 12-character
 string.</li> </ul>
 */
static constexpr const char *kHostCpuVendorId = "host.cpu.vendor.id";

/**
 * The name of the cluster.
 */
static constexpr const char *kK8sClusterName = "k8s.cluster.name";

/**
 * A pseudo-ID for the cluster, set to the UID of the {@code kube-system} namespace.
 *
 * <p>Notes:
  <ul> <li>K8s does not have support for obtaining a cluster ID. If this is ever
added, we will recommend collecting the {@code k8s.cluster.uid} through the
official APIs. In the meantime, we are able to use the {@code uid} of the
{@code kube-system} namespace as a proxy for cluster ID. Read on for the
rationale.</li><li>Every object created in a K8s cluster is assigned a distinct UID. The
{@code kube-system} namespace is used by Kubernetes itself and will exist
for the lifetime of the cluster. Using the {@code uid} of the {@code kube-system}
namespace is a reasonable proxy for the K8s ClusterID as it will only
change if the cluster is rebuilt. Furthermore, Kubernetes UIDs are
UUIDs as standardized by
<a href="https://www.itu.int/ITU-T/studygroups/com17/oid.html">ISO/IEC 9834-8 and ITU-T X.667</a>.
Which states:</li><blockquote>
<li>If generated according to one of the mechanisms defined in Rec.</li></blockquote>
<li>ITU-T X.667 | ISO/IEC 9834-8, a UUID is either guaranteed to be
  different from all other UUIDs generated before 3603 A.D., or is
  extremely likely to be different (depending on the mechanism chosen).</li><li>Therefore, UIDs
between clusters should be extremely unlikely to conflict.</li> </ul>
 */
static constexpr const char *kK8sClusterUid = "k8s.cluster.uid";

/**
 * The name of the Node.
 */
static constexpr const char *kK8sNodeName = "k8s.node.name";

/**
 * The UID of the Node.
 */
static constexpr const char *kK8sNodeUid = "k8s.node.uid";

/**
 * The name of the namespace that the pod is running in.
 */
static constexpr const char *kK8sNamespaceName = "k8s.namespace.name";

/**
 * The name of the Pod.
 */
static constexpr const char *kK8sPodName = "k8s.pod.name";

/**
 * The UID of the Pod.
 */
static constexpr const char *kK8sPodUid = "k8s.pod.uid";

/**
 * The name of the Container from Pod specification, must be unique within a Pod. Container runtime
 * usually uses different globally unique name ({@code container.name}).
 */
static constexpr const char *kK8sContainerName = "k8s.container.name";

/**
 * Number of times the container was restarted. This attribute can be used to identify a particular
 * container (running or stopped) within a container spec.
 */
static constexpr const char *kK8sContainerRestartCount = "k8s.container.restart_count";

/**
 * The name of the ReplicaSet.
 */
static constexpr const char *kK8sReplicasetName = "k8s.replicaset.name";

/**
 * The UID of the ReplicaSet.
 */
static constexpr const char *kK8sReplicasetUid = "k8s.replicaset.uid";

/**
 * The name of the Deployment.
 */
static constexpr const char *kK8sDeploymentName = "k8s.deployment.name";

/**
 * The UID of the Deployment.
 */
static constexpr const char *kK8sDeploymentUid = "k8s.deployment.uid";

/**
 * The name of the StatefulSet.
 */
static constexpr const char *kK8sStatefulsetName = "k8s.statefulset.name";

/**
 * The UID of the StatefulSet.
 */
static constexpr const char *kK8sStatefulsetUid = "k8s.statefulset.uid";

/**
 * The name of the DaemonSet.
 */
static constexpr const char *kK8sDaemonsetName = "k8s.daemonset.name";

/**
 * The UID of the DaemonSet.
 */
static constexpr const char *kK8sDaemonsetUid = "k8s.daemonset.uid";

/**
 * The name of the Job.
 */
static constexpr const char *kK8sJobName = "k8s.job.name";

/**
 * The UID of the Job.
 */
static constexpr const char *kK8sJobUid = "k8s.job.uid";

/**
 * The name of the CronJob.
 */
static constexpr const char *kK8sCronjobName = "k8s.cronjob.name";

/**
 * The UID of the CronJob.
 */
static constexpr const char *kK8sCronjobUid = "k8s.cronjob.uid";

/**
 * The digest of the OCI image manifest. For container images specifically is the digest by which
the container image is known.
 *
 * <p>Notes:
  <ul> <li>Follows <a href="https://github.com/opencontainers/image-spec/blob/main/manifest.md">OCI
Image Manifest Specification</a>, and specifically the <a
href="https://github.com/opencontainers/image-spec/blob/main/descriptor.md#digests">Digest
property</a>. An example can be found in <a
href="https://docs.docker.com/registry/spec/manifest-v2-2/#example-image-manifest">Example Image
Manifest</a>.</li> </ul>
 */
static constexpr const char *kOciManifestDigest = "oci.manifest.digest";

/**
 * Unique identifier for a particular build or compilation of the operating system.
 */
static constexpr const char *kOsBuildId = "os.build_id";

/**
 * Human readable (not intended to be parsed) OS version information, like e.g. reported by {@code
 * ver} or {@code lsb_release -a} commands.
 */
static constexpr const char *kOsDescription = "os.description";

/**
 * Human readable operating system name.
 */
static constexpr const char *kOsName = "os.name";

/**
 * The operating system type.
 */
static constexpr const char *kOsType = "os.type";

/**
 * The version string of the operating system as defined in <a
 * href="/docs/resource/README.md#version-attributes">Version Attributes</a>.
 */
static constexpr const char *kOsVersion = "os.version";

/**
 * The command used to launch the process (i.e. the command name). On Linux based systems, can be
 * set to the zeroth string in {@code proc/[pid]/cmdline}. On Windows, can be set to the first
 * parameter extracted from {@code GetCommandLineW}.
 */
static constexpr const char *kProcessCommand = "process.command";

/**
 * All the command arguments (including the command/executable itself) as received by the process.
 * On Linux-based systems (and some other Unixoid systems supporting procfs), can be set according
 * to the list of null-delimited strings extracted from {@code proc/[pid]/cmdline}. For libc-based
 * executables, this would be the full argv vector passed to {@code main}.
 */
static constexpr const char *kProcessCommandArgs = "process.command_args";

/**
 * The full command used to launch the process as a single string representing the full command. On
 * Windows, can be set to the result of {@code GetCommandLineW}. Do not set this if you have to
 * assemble it just for monitoring; use {@code process.command_args} instead.
 */
static constexpr const char *kProcessCommandLine = "process.command_line";

/**
 * The name of the process executable. On Linux based systems, can be set to the {@code Name} in
 * {@code proc/[pid]/status}. On Windows, can be set to the base name of {@code
 * GetProcessImageFileNameW}.
 */
static constexpr const char *kProcessExecutableName = "process.executable.name";

/**
 * The full path to the process executable. On Linux based systems, can be set to the target of
 * {@code proc/[pid]/exe}. On Windows, can be set to the result of {@code GetProcessImageFileNameW}.
 */
static constexpr const char *kProcessExecutablePath = "process.executable.path";

/**
 * The username of the user that owns the process.
 */
static constexpr const char *kProcessOwner = "process.owner";

/**
 * Parent Process identifier (PID).
 */
static constexpr const char *kProcessParentPid = "process.parent_pid";

/**
 * Process identifier (PID).
 */
static constexpr const char *kProcessPid = "process.pid";

/**
 * An additional description about the runtime of the process, for example a specific vendor
 * customization of the runtime environment.
 */
static constexpr const char *kProcessRuntimeDescription = "process.runtime.description";

/**
 * The name of the runtime of this process. For compiled native binaries, this SHOULD be the name of
 * the compiler.
 */
static constexpr const char *kProcessRuntimeName = "process.runtime.name";

/**
 * The version of the runtime of this process, as returned by the runtime without modification.
 */
static constexpr const char *kProcessRuntimeVersion = "process.runtime.version";

/**
 * Logical name of the service.
 *
 * <p>Notes:
  <ul> <li>MUST be the same for all instances of horizontally scaled services. If the value was not
 specified, SDKs MUST fallback to {@code unknown_service:} concatenated with <a
 href="process.md#process">{@code process.executable.name}</a>, e.g. {@code unknown_service:bash}.
 If {@code process.executable.name} is not available, the value MUST be set to {@code
 unknown_service}.</li> </ul>
 */
static constexpr const char *kServiceName = "service.name";

/**
 * The version string of the service API or implementation. The format is not defined by these
 * conventions.
 */
static constexpr const char *kServiceVersion = "service.version";

/**
 * The string ID of the service instance.
 *
 * <p>Notes:
  <ul> <li>MUST be unique for each instance of the same {@code service.namespace,service.name} pair
 (in other words {@code service.namespace,service.name,service.instance.id} triplet MUST be globally
 unique). The ID helps to distinguish instances of the same service that exist at the same time
 (e.g. instances of a horizontally scaled service). It is preferable for the ID to be persistent and
 stay the same for the lifetime of the service instance, however it is acceptable that the ID is
 ephemeral and changes during important lifetime events for the service (e.g. service restarts). If
 the service has no inherent unique ID that can be used as the value of this attribute it is
 recommended to generate a random Version 1 or Version 4 RFC 4122 UUID (services aiming for
 reproducible UUIDs may also use Version 5, see RFC 4122 for more recommendations).</li> </ul>
 */
static constexpr const char *kServiceInstanceId = "service.instance.id";

/**
 * A namespace for {@code service.name}.
 *
 * <p>Notes:
  <ul> <li>A string value having a meaning that helps to distinguish a group of services, for
 example the team name that owns a group of services. {@code service.name} is expected to be unique
 within the same namespace. If {@code service.namespace} is not specified in the Resource then
 {@code service.name} is expected to be unique for all services that have no explicit namespace
 defined (so the empty/unspecified namespace is simply one more valid namespace). Zero-length
 namespace string is assumed equal to unspecified namespace.</li> </ul>
 */
static constexpr const char *kServiceNamespace = "service.namespace";

/**
 * The language of the telemetry SDK.
 */
static constexpr const char *kTelemetrySdkLanguage = "telemetry.sdk.language";

/**
 * The name of the telemetry SDK as defined above.
 *
 * <p>Notes:
  <ul> <li>The OpenTelemetry SDK MUST set the {@code telemetry.sdk.name} attribute to {@code
opentelemetry}. If another SDK, like a fork or a vendor-provided implementation, is used, this SDK
MUST set the
{@code telemetry.sdk.name} attribute to the fully-qualified class or module name of this SDK's main
entry point or another suitable identifier depending on the language. The identifier {@code
opentelemetry} is reserved and MUST NOT be used in this case. All custom identifiers SHOULD be
stable across different versions of an implementation.</li> </ul>
 */
static constexpr const char *kTelemetrySdkName = "telemetry.sdk.name";

/**
 * The version string of the telemetry SDK.
 */
static constexpr const char *kTelemetrySdkVersion = "telemetry.sdk.version";

/**
 * The name of the auto instrumentation agent or distribution, if used.
 *
 * <p>Notes:
  <ul> <li>Official auto instrumentation agents and distributions SHOULD set the {@code
telemetry.distro.name} attribute to a string starting with {@code opentelemetry-}, e.g. {@code
opentelemetry-java-instrumentation}.</li> </ul>
 */
static constexpr const char *kTelemetryDistroName = "telemetry.distro.name";

/**
 * The version string of the auto instrumentation agent or distribution, if used.
 */
static constexpr const char *kTelemetryDistroVersion = "telemetry.distro.version";

/**
 * Additional description of the web engine (e.g. detailed version and edition information).
 */
static constexpr const char *kWebengineDescription = "webengine.description";

/**
 * The name of the web engine.
 */
static constexpr const char *kWebengineName = "webengine.name";

/**
 * The version of the web engine.
 */
static constexpr const char *kWebengineVersion = "webengine.version";

/**
 * The name of the instrumentation scope - ({@code InstrumentationScope.Name} in OTLP).
 */
static constexpr const char *kOtelScopeName = "otel.scope.name";

/**
 * The version of the instrumentation scope - ({@code InstrumentationScope.Version} in OTLP).
 */
static constexpr const char *kOtelScopeVersion = "otel.scope.version";

/**
 * Deprecated, use the {@code otel.scope.name} attribute.
 *
 * @deprecated Deprecated, use the `otel.scope.name` attribute.
 */
OPENTELEMETRY_DEPRECATED
static constexpr const char *kOtelLibraryName = "otel.library.name";

/**
 * Deprecated, use the {@code otel.scope.version} attribute.
 *
 * @deprecated Deprecated, use the `otel.scope.version` attribute.
 */
OPENTELEMETRY_DEPRECATED
static constexpr const char *kOtelLibraryVersion = "otel.library.version";

// Enum definitions
namespace CloudPlatformValues
{
/** Alibaba Cloud Elastic Compute Service. */
static constexpr const char *kAlibabaCloudEcs = "alibaba_cloud_ecs";
/** Alibaba Cloud Function Compute. */
static constexpr const char *kAlibabaCloudFc = "alibaba_cloud_fc";
/** Red Hat OpenShift on Alibaba Cloud. */
static constexpr const char *kAlibabaCloudOpenshift = "alibaba_cloud_openshift";
/** AWS Elastic Compute Cloud. */
static constexpr const char *kAwsEc2 = "aws_ec2";
/** AWS Elastic Container Service. */
static constexpr const char *kAwsEcs = "aws_ecs";
/** AWS Elastic Kubernetes Service. */
static constexpr const char *kAwsEks = "aws_eks";
/** AWS Lambda. */
static constexpr const char *kAwsLambda = "aws_lambda";
/** AWS Elastic Beanstalk. */
static constexpr const char *kAwsElasticBeanstalk = "aws_elastic_beanstalk";
/** AWS App Runner. */
static constexpr const char *kAwsAppRunner = "aws_app_runner";
/** Red Hat OpenShift on AWS (ROSA). */
static constexpr const char *kAwsOpenshift = "aws_openshift";
/** Azure Virtual Machines. */
static constexpr const char *kAzureVm = "azure_vm";
/** Azure Container Instances. */
static constexpr const char *kAzureContainerInstances = "azure_container_instances";
/** Azure Kubernetes Service. */
static constexpr const char *kAzureAks = "azure_aks";
/** Azure Functions. */
static constexpr const char *kAzureFunctions = "azure_functions";
/** Azure App Service. */
static constexpr const char *kAzureAppService = "azure_app_service";
/** Azure Red Hat OpenShift. */
static constexpr const char *kAzureOpenshift = "azure_openshift";
/** Google Bare Metal Solution (BMS). */
static constexpr const char *kGcpBareMetalSolution = "gcp_bare_metal_solution";
/** Google Cloud Compute Engine (GCE). */
static constexpr const char *kGcpComputeEngine = "gcp_compute_engine";
/** Google Cloud Run. */
static constexpr const char *kGcpCloudRun = "gcp_cloud_run";
/** Google Cloud Kubernetes Engine (GKE). */
static constexpr const char *kGcpKubernetesEngine = "gcp_kubernetes_engine";
/** Google Cloud Functions (GCF). */
static constexpr const char *kGcpCloudFunctions = "gcp_cloud_functions";
/** Google Cloud App Engine (GAE). */
static constexpr const char *kGcpAppEngine = "gcp_app_engine";
/** Red Hat OpenShift on Google Cloud. */
static constexpr const char *kGcpOpenshift = "gcp_openshift";
/** Red Hat OpenShift on IBM Cloud. */
static constexpr const char *kIbmCloudOpenshift = "ibm_cloud_openshift";
/** Tencent Cloud Cloud Virtual Machine (CVM). */
static constexpr const char *kTencentCloudCvm = "tencent_cloud_cvm";
/** Tencent Cloud Elastic Kubernetes Service (EKS). */
static constexpr const char *kTencentCloudEks = "tencent_cloud_eks";
/** Tencent Cloud Serverless Cloud Function (SCF). */
static constexpr const char *kTencentCloudScf = "tencent_cloud_scf";
}  // namespace CloudPlatformValues

namespace CloudProviderValues
{
/** Alibaba Cloud. */
static constexpr const char *kAlibabaCloud = "alibaba_cloud";
/** Amazon Web Services. */
static constexpr const char *kAws = "aws";
/** Microsoft Azure. */
static constexpr const char *kAzure = "azure";
/** Google Cloud Platform. */
static constexpr const char *kGcp = "gcp";
/** Heroku Platform as a Service. */
static constexpr const char *kHeroku = "heroku";
/** IBM Cloud. */
static constexpr const char *kIbmCloud = "ibm_cloud";
/** Tencent Cloud. */
static constexpr const char *kTencentCloud = "tencent_cloud";
}  // namespace CloudProviderValues

namespace AwsEcsLaunchtypeValues
{
/** ec2. */
static constexpr const char *kEc2 = "ec2";
/** fargate. */
static constexpr const char *kFargate = "fargate";
}  // namespace AwsEcsLaunchtypeValues

namespace HostArchValues
{
/** AMD64. */
static constexpr const char *kAmd64 = "amd64";
/** ARM32. */
static constexpr const char *kArm32 = "arm32";
/** ARM64. */
static constexpr const char *kArm64 = "arm64";
/** Itanium. */
static constexpr const char *kIa64 = "ia64";
/** 32-bit PowerPC. */
static constexpr const char *kPpc32 = "ppc32";
/** 64-bit PowerPC. */
static constexpr const char *kPpc64 = "ppc64";
/** IBM z/Architecture. */
static constexpr const char *kS390x = "s390x";
/** 32-bit x86. */
static constexpr const char *kX86 = "x86";
}  // namespace HostArchValues

namespace OsTypeValues
{
/** Microsoft Windows. */
static constexpr const char *kWindows = "windows";
/** Linux. */
static constexpr const char *kLinux = "linux";
/** Apple Darwin. */
static constexpr const char *kDarwin = "darwin";
/** FreeBSD. */
static constexpr const char *kFreebsd = "freebsd";
/** NetBSD. */
static constexpr const char *kNetbsd = "netbsd";
/** OpenBSD. */
static constexpr const char *kOpenbsd = "openbsd";
/** DragonFly BSD. */
static constexpr const char *kDragonflybsd = "dragonflybsd";
/** HP-UX (Hewlett Packard Unix). */
static constexpr const char *kHpux = "hpux";
/** AIX (Advanced Interactive eXecutive). */
static constexpr const char *kAix = "aix";
/** SunOS, Oracle Solaris. */
static constexpr const char *kSolaris = "solaris";
/** IBM z/OS. */
static constexpr const char *kZOs = "z_os";
}  // namespace OsTypeValues

namespace TelemetrySdkLanguageValues
{
/** cpp. */
static constexpr const char *kCpp = "cpp";
/** dotnet. */
static constexpr const char *kDotnet = "dotnet";
/** erlang. */
static constexpr const char *kErlang = "erlang";
/** go. */
static constexpr const char *kGo = "go";
/** java. */
static constexpr const char *kJava = "java";
/** nodejs. */
static constexpr const char *kNodejs = "nodejs";
/** php. */
static constexpr const char *kPhp = "php";
/** python. */
static constexpr const char *kPython = "python";
/** ruby. */
static constexpr const char *kRuby = "ruby";
/** rust. */
static constexpr const char *kRust = "rust";
/** swift. */
static constexpr const char *kSwift = "swift";
/** webjs. */
static constexpr const char *kWebjs = "webjs";
}  // namespace TelemetrySdkLanguageValues

}  // namespace SemanticConventions
}  // namespace resource
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
