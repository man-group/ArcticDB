from arcticdb._core_api import NativeVariantStorage, MetricsConfig, env_config_from_lib_config
from arcticdb_ext.storage import AWSAuthMethod, S3Settings as NativeS3Settings, GCPXMLSettings as NativeGCPXMLSettings
from arcticdb_ext.metrics.prometheus import MetricsConfigModel
from arcticdb_ext.storage import create_mem_config_resolver, LibraryIndex, OpenMode


def test_convert_native_variant_storage_to_py_tuple_s3():
    s3_settings = NativeS3Settings(AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN, "abc", True)
    native_setting = NativeVariantStorage(s3_settings)
    py_tuples = native_setting.__getstate__()
    assert len(py_tuples) == 4
    assert py_tuples[0].value == 0
    assert py_tuples[1].value == s3_settings.aws_auth.value
    assert py_tuples[2] == s3_settings.aws_profile
    assert py_tuples[3] == s3_settings.use_internal_client_wrapper_for_testing


def test_convert_native_variant_storage_to_py_tuple_gcp():
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
    py_tuples = native_setting.__getstate__()
    assert len(py_tuples) == 11
    assert py_tuples[0].value == 1
    assert py_tuples[1].value == gcp_settings.aws_auth.value
    assert py_tuples[2] == gcp_settings.ca_cert_path
    assert py_tuples[3] == gcp_settings.ca_cert_dir
    assert py_tuples[4] == gcp_settings.ssl
    assert py_tuples[5] == gcp_settings.https
    assert py_tuples[6] == gcp_settings.prefix
    assert py_tuples[7] == gcp_settings.endpoint
    assert py_tuples[8] == gcp_settings.secret
    assert py_tuples[9] == gcp_settings.access
    assert py_tuples[10] == gcp_settings.bucket


def test_aws_auth_method_value():
    assert AWSAuthMethod.DISABLED.value == 0
    assert AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN.value == 1
    assert AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER.value == 2


def test_convert_metrics_config_to_py_tuple():
    config = {
        "host": "host",
        "port": "port",
        "job": "job",
        "instance": "instance",
        "prometheus_env": "prometheus_env",
        "metrics_config_model": MetricsConfigModel.PULL,
    }
    prom_conf = MetricsConfig(
        config["host"],
        config["port"],
        config["job"],
        config["instance"],
        config["prometheus_env"],
        config["metrics_config_model"],
    )
    py_tuples = prom_conf.__getstate__()
    assert len(py_tuples) == 6
    assert py_tuples[0] == config["host"]
    assert py_tuples[1] == config["port"]
    assert py_tuples[2] == config["job"]
    assert py_tuples[3] == config["instance"]
    assert py_tuples[4] == config["prometheus_env"]
    assert py_tuples[5].value == config["metrics_config_model"].value


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
