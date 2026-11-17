import pytest
from arcticdb.adapters import S3LibraryAdapter, GCPXMLLibraryAdapter
from arcticdb.adapters.s3_library_adapter import USE_AWS_CRED_PROVIDERS_TOKEN
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import UserInputException
from arcticdb.version_store.helper import add_s3_library_to_env
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb_ext.storage import AWSAuthMethod


def _add_s3_library(aws_auth, aws_profile, is_nfs_layout=False):
    add_s3_library_to_env(
        EnvironmentConfigsMap(),
        lib_name="lib",
        env_name="env",
        credential_name=USE_AWS_CRED_PROVIDERS_TOKEN,
        credential_key=USE_AWS_CRED_PROVIDERS_TOKEN,
        bucket_name="bucket",
        endpoint="endpoint",
        aws_auth=aws_auth,
        aws_profile=aws_profile,
        is_nfs_layout=is_nfs_layout,
    )


def test_s3_default_auth_with_profile_allowed():
    # aws_profile is now allowed with the default credentials provider chain, not just STS
    _add_s3_library(AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN, "my_profile")


def test_s3_profile_requires_aws_auth():
    with pytest.raises(UserInputException, match="aws_profile can only be set"):
        _add_s3_library(AWSAuthMethod.DISABLED, "my_profile")


def test_s3_sts_auth_requires_profile():
    with pytest.raises(UserInputException, match="STS credential provider requires aws_profile"):
        _add_s3_library(AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER, "")


def test_s3_profile_not_allowed_for_nfs():
    with pytest.raises(UserInputException, match="can only be set for S3"):
        _add_s3_library(AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN, "my_profile", is_nfs_layout=True)


def test_s3_native_cfg_sdk_default():
    adapter = S3LibraryAdapter(
        "s3://my_endpoint:my_bucket?aws_auth=true&aws_profile=my_profile", encoding_version=EncodingVersion.V1
    )

    native_config = adapter.native_config().as_s3_settings()

    assert native_config.aws_auth == AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN
    assert native_config.aws_profile == "my_profile"


def test_s3_native_cfg_sts():
    adapter = S3LibraryAdapter(
        "s3://my_endpoint:my_bucket?aws_auth=sts&aws_profile=my_profile", encoding_version=EncodingVersion.V1
    )

    native_config = adapter.native_config().as_s3_settings()

    assert native_config.aws_auth == AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER
    assert native_config.aws_profile == "my_profile"


def test_s3_native_cfg_off():
    adapter = S3LibraryAdapter(
        "s3://my_endpoint:my_bucket?access=my_access&secret=my_secret", encoding_version=EncodingVersion.V1
    )

    native_config = adapter.native_config().as_s3_settings()

    assert native_config.aws_auth == AWSAuthMethod.DISABLED
    assert adapter._query_params.access == "my_access"
    assert adapter._query_params.secret == "my_secret"


def test_s3_repr():
    adapter = S3LibraryAdapter(
        "s3://my_endpoint:my_bucket?aws_auth=sts&aws_profile=my_profile", encoding_version=EncodingVersion.V1
    )
    assert repr(adapter) == "S3(endpoint=my_endpoint, bucket=my_bucket)"


def test_s3_config_library():
    adapter = S3LibraryAdapter(
        "s3://my_endpoint:my_bucket?aws_auth=sts&aws_profile=my_profile", encoding_version=EncodingVersion.V1
    )
    cfg_library = adapter.config_library
    assert cfg_library.library_path == "_arctic_cfg"


def test_s3_path_prefix():
    adapter = S3LibraryAdapter(
        "s3://my_endpoint:my_bucket?aws_auth=true&path_prefix=my_prefix", encoding_version=EncodingVersion.V1
    )
    assert adapter.path_prefix == "my_prefix"


def test_gcpxml_native_cfg_sdk_default():
    adapter = GCPXMLLibraryAdapter("gcpxml://my_endpoint:my_bucket?aws_auth=true", encoding_version=EncodingVersion.V1)

    native_config = adapter.native_config().as_gcpxml_settings()

    assert native_config.aws_auth == AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN


def test_gcpxml_native_cfg_sdk_default_profile_not_supported():
    with pytest.raises(ValueError):
        GCPXMLLibraryAdapter(
            "gcpxml://my_endpoint:my_bucket?aws_auth=true&aws_profile=my_profile", encoding_version=EncodingVersion.V1
        )


def test_gcpxml_native_cfg_sts():
    with pytest.raises(ValueError):
        GCPXMLLibraryAdapter(
            "gcpxml://my_endpoint:my_bucket?aws_auth=sts&aws_profile=my_profile", encoding_version=EncodingVersion.V1
        )


def test_gcpxml_native_cfg_keys():
    adapter = GCPXMLLibraryAdapter(
        "gcpxml://my_endpoint:my_bucket?access=my_access&secret=my_secret", encoding_version=EncodingVersion.V1
    )

    native_config = adapter.native_config().as_gcpxml_settings()

    assert native_config.aws_auth == AWSAuthMethod.DISABLED
    assert native_config.access == "my_access"
    assert native_config.secret == "my_secret"


def test_gcp_repr():
    adapter = GCPXMLLibraryAdapter("gcpxml://my_endpoint:my_bucket?aws_auth=true", encoding_version=EncodingVersion.V1)
    assert repr(adapter) == "GCPXML(endpoint=my_endpoint, bucket=my_bucket)"


def test_gcp_config_library():
    adapter = GCPXMLLibraryAdapter("gcpxml://my_endpoint:my_bucket?aws_auth=true", encoding_version=EncodingVersion.V1)
    cfg_library = adapter.config_library
    assert cfg_library.library_path == "_arctic_cfg"


def test_gcp_path_prefix():
    adapter = GCPXMLLibraryAdapter(
        "gcpxml://my_endpoint:my_bucket?aws_auth=true&path_prefix=my_prefix", encoding_version=EncodingVersion.V1
    )
    assert adapter.path_prefix == "my_prefix"


@pytest.mark.parametrize(
    "uri, bucket, region, path_prefix",
    [
        ("s3://my-bucket?region=eu-west-2", "my-bucket", "eu-west-2", None),
        ("s3://my-bucket/?region=eu-west-2", "my-bucket", "eu-west-2", None),
        ("s3://my-bucket/team/arctic/?region=eu-west-2", "my-bucket", "eu-west-2", "team/arctic"),
        ("s3s://my-bucket/team?region=us-east-1", "my-bucket", "us-east-1", "team"),
        ("https://my-bucket.s3.eu-west-2.amazonaws.com", "my-bucket", "eu-west-2", None),
        ("https://my-bucket.s3.eu-west-2.amazonaws.com/team/arctic", "my-bucket", "eu-west-2", "team/arctic"),
        ("https://my-bucket.s3-us-gov-west-1.amazonaws.com/team", "my-bucket", "us-gov-west-1", "team"),
        ("https://s3.ap-southeast-1.amazonaws.com/my-bucket/team", "my-bucket", "ap-southeast-1", "team"),
        ("https://my-bucket.s3.amazonaws.com/team?region=us-east-1", "my-bucket", "us-east-1", "team"),
    ],
)
def test_s3_standard_uri(uri, bucket, region, path_prefix):
    assert S3LibraryAdapter.supports_uri(uri)
    adapter = S3LibraryAdapter(uri, encoding_version=EncodingVersion.V1)
    assert repr(adapter) == f"S3(endpoint=s3.{region}.amazonaws.com, bucket={bucket})"
    assert adapter.path_prefix == path_prefix

    override = adapter._get_s3_override()
    assert override.endpoint == f"s3.{region}.amazonaws.com"
    assert override.bucket_name == bucket
    assert override.region == region
    assert override.https
    assert override.use_virtual_addressing
    # like the AWS CLI and boto3, use the default credentials provider chain when no keys are given
    assert adapter.native_config().as_s3_settings().aws_auth == AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN
    assert override.credential_name == USE_AWS_CRED_PROVIDERS_TOKEN


def test_s3_standard_uri_matches_equivalent_legacy_uri():
    standard = S3LibraryAdapter("s3://my-bucket/team?region=eu-west-2&aws_auth=true", EncodingVersion.V1)
    legacy = S3LibraryAdapter(
        "s3s://s3.eu-west-2.amazonaws.com:my-bucket?path_prefix=team&aws_auth=true", EncodingVersion.V1
    )
    for attr in ("endpoint", "bucket_name", "region", "https", "use_virtual_addressing", "credential_name"):
        assert getattr(standard._get_s3_override(), attr) == getattr(legacy._get_s3_override(), attr), attr
    assert standard.path_prefix == legacy.path_prefix
    assert standard.config_library.library_path == "_arctic_cfg"


def test_s3_standard_uri_with_keys_and_options():
    adapter = S3LibraryAdapter(
        "s3://my-bucket/team?region=eu-west-2&access=my_access&secret=my_secret", EncodingVersion.V1
    )
    assert adapter.native_config().as_s3_settings().aws_auth == AWSAuthMethod.DISABLED
    assert adapter._get_s3_override().credential_name == "my_access"

    adapter = S3LibraryAdapter(
        "s3://my-bucket?region=eu-west-2&aws_auth=sts&aws_profile=my_profile", EncodingVersion.V1
    )
    assert adapter.native_config().as_s3_settings().aws_auth == AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER
    assert adapter.native_config().as_s3_settings().aws_profile == "my_profile"

    with pytest.raises(ValueError, match="Invalid query parameter"):
        S3LibraryAdapter("s3://my-bucket?region=eu-west-2&not_an_option=1", EncodingVersion.V1)


def test_s3_standard_uri_bucket_with_dots_uses_path_style():
    adapter = S3LibraryAdapter("s3://my.dotted.bucket/team?region=eu-west-2", EncodingVersion.V1)
    assert adapter._get_s3_override().bucket_name == "my.dotted.bucket"
    assert not adapter._get_s3_override().use_virtual_addressing


def test_s3_standard_uri_region_from_environment(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    assert repr(S3LibraryAdapter("s3://my-bucket", EncodingVersion.V1)) == (
        "S3(endpoint=s3.eu-central-1.amazonaws.com, bucket=my-bucket)"
    )
    monkeypatch.setenv("AWS_REGION", "eu-north-1")
    assert S3LibraryAdapter("s3://my-bucket", EncodingVersion.V1)._get_s3_override().region == "eu-north-1"
    # an explicit region wins over the environment
    assert S3LibraryAdapter("s3://my-bucket?region=us-west-2", EncodingVersion.V1)._get_s3_override().region == (
        "us-west-2"
    )


def test_s3_standard_uri_errors(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    with pytest.raises(ValueError, match="Can't determine the AWS region"):
        S3LibraryAdapter("s3://my-bucket/team", EncodingVersion.V1)
    with pytest.raises(ValueError, match="conflicts with path_prefix"):
        S3LibraryAdapter("s3://my-bucket/team?region=eu-west-2&path_prefix=other", EncodingVersion.V1)
    with pytest.raises(ValueError, match="conflicts with region"):
        S3LibraryAdapter("https://my-bucket.s3.eu-west-2.amazonaws.com?region=us-east-1", EncodingVersion.V1)
    with pytest.raises(ValueError, match="Invalid S3 URI"):
        S3LibraryAdapter("s3://Not_A_Valid_Bucket", EncodingVersion.V1)


@pytest.mark.parametrize(
    "uri",
    [
        "https://example.com/my-bucket",
        "https://my-bucket.s3.eu-west-2.amazonaws.com.evil.com/x",
        "http://s3.amazonaws.com/b",
    ],
)
def test_s3_standard_uri_unsupported_https_urls(uri):
    assert not S3LibraryAdapter.supports_uri(uri)


def test_s3_legacy_uri_still_supported():
    adapter = S3LibraryAdapter("s3://localhost:my-bucket?port=9000&region=eu-west-2", EncodingVersion.V1)
    assert repr(adapter) == "S3(endpoint=localhost:9000, bucket=my-bucket)"
    assert not adapter._get_s3_override().https
    adapter = S3LibraryAdapter("s3s://s3.eu-west-2.amazonaws.com:my-bucket?aws_auth=true", EncodingVersion.V1)
    assert adapter._get_s3_override().region == "eu-west-2"
    assert adapter._get_s3_override().use_virtual_addressing
