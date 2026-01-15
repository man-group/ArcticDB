from arcticdb._core_api import (
    NativeVariantStorage,
    MetricsConfig,
    env_config_from_lib_config,
    NativeVariantStorageContentType,
)
from arcticdb_ext.storage import AWSAuthMethod, S3Settings as NativeS3Settings, GCPXMLSettings as NativeGCPXMLSettings
from arcticdb_ext.metrics.prometheus import MetricsConfigModel
from arcticdb_ext.storage import create_mem_config_resolver, LibraryIndex, OpenMode


def test_native_variant_storage_s3_accessors():
    s3_settings = NativeS3Settings(
        aws_auth=AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN,
        aws_profile="abc",
        use_internal_client_wrapper_for_testing=True,
    )
    native_setting = NativeVariantStorage(s3_settings)

    assert native_setting.setting_type == NativeVariantStorageContentType.S3

    retrieved_s3 = native_setting.as_s3_settings()
    assert retrieved_s3.aws_auth == s3_settings.aws_auth
    assert retrieved_s3.aws_profile == s3_settings.aws_profile
    assert retrieved_s3.use_internal_client_wrapper_for_testing == s3_settings.use_internal_client_wrapper_for_testing


def test_native_variant_storage_gcp_accessors():
    gcp_settings = NativeGCPXMLSettings()
    gcp_settings.bucket = "bucket"
    gcp_settings.endpoint = "endpoint"
    gcp_settings.access = "access"
    gcp_settings.secret = "secret"
    gcp_settings.aws_auth = AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN
    gcp_settings.prefix = "abc"
    gcp_settings.https = True
    gcp_settings.ssl = False
    gcp_settings.ca_cert_path = "ca_cert_path"
    gcp_settings.ca_cert_dir = "ca_cert_dir"
    native_setting = NativeVariantStorage(gcp_settings)

    assert native_setting.setting_type == NativeVariantStorageContentType.GCPXML

    retrieved_gcp = native_setting.as_gcpxml_settings()
    assert retrieved_gcp.aws_auth == gcp_settings.aws_auth
    assert retrieved_gcp.ca_cert_path == gcp_settings.ca_cert_path
    assert retrieved_gcp.ca_cert_dir == gcp_settings.ca_cert_dir
    assert retrieved_gcp.ssl == gcp_settings.ssl
    assert retrieved_gcp.https == gcp_settings.https
    assert retrieved_gcp.prefix == gcp_settings.prefix
    assert retrieved_gcp.endpoint == gcp_settings.endpoint
    assert retrieved_gcp.secret == gcp_settings.secret
    assert retrieved_gcp.access == gcp_settings.access
    assert retrieved_gcp.bucket == gcp_settings.bucket


def test_native_variant_storage_empty_accessor():
    native_setting = NativeVariantStorage()

    assert native_setting.setting_type == NativeVariantStorageContentType.EMPTY


def test_aws_auth_method_value():
    assert AWSAuthMethod.DISABLED.value == 0
    assert AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN.value == 1
    assert AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER.value == 2


def test_native_variant_storage_setting_type_value():
    assert NativeVariantStorageContentType.EMPTY.value == 0
    assert NativeVariantStorageContentType.S3.value == 1
    assert NativeVariantStorageContentType.GCPXML.value == 2


def test_metrics_config_accessors():
    config_values = {
        "host": "host",
        "port": "port",
        "job": "job",
        "instance": "instance",
        "prometheus_env": "prometheus_env",
        "metrics_config_model": MetricsConfigModel.PULL,
    }
    prom_conf = MetricsConfig(
        "host",
        "port",
        "job",
        "instance",
        "prometheus_env",
        MetricsConfigModel.PULL,
    )

    assert prom_conf.host == config_values["host"]
    assert prom_conf.port == config_values["port"]
    assert prom_conf.job_name == config_values["job"]
    assert prom_conf.instance == config_values["instance"]
    assert prom_conf.prometheus_env == config_values["prometheus_env"]
    assert prom_conf.model == config_values["metrics_config_model"]


def test_metrics_config_model_value():
    assert MetricsConfigModel.NO_INIT.value == 0
    assert MetricsConfigModel.PUSH.value == 1
    assert MetricsConfigModel.PULL.value == 2


def test_env_config_from_lib_config(lmdb_version_store_v1):
    lib_cfg = lmdb_version_store_v1.lib_cfg()
    env = lmdb_version_store_v1.env
    open_mode = lmdb_version_store_v1.open_mode()
    native_cfg = lmdb_version_store_v1.lib_native_cfg()

    envs_cfg = env_config_from_lib_config(lib_cfg, env)
    cfg_resolver = create_mem_config_resolver(envs_cfg)
    lib_idx = LibraryIndex.create_from_resolver(env, cfg_resolver)
    enterprise_open_mode = OpenMode(open_mode.value)

    result = lib_idx.get_library(lib_cfg.lib_desc.name, enterprise_open_mode, native_cfg)
    assert result.config == lmdb_version_store_v1._library.config
    assert result.open_mode == lmdb_version_store_v1._library.open_mode
    assert result.library_path == lmdb_version_store_v1._library.library_path
