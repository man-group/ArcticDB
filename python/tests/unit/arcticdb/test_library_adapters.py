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
