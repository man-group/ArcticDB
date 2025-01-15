import pytest


class Dataframe:

    def df(feature:str) -> str:
        return f"df.{feature}"

    NO_INDEX = df("no_index")
    RANGE_INDEX = df("range_index")
    TIMESTAMP_INDEX = df("timestamp_index")
    EMPTY = df("df.empty")
    EMPTY_WITH_SCHEMA = df("df.empty_with_schema")


class Features:

    def feat(feature: str) -> str:
        return f"ft.{feature}"

    DYNAMIC_SHEMA = feat("dynamic_scema")
    ENCODING_V1 = feat("encoding_v1")
    ENCODING_V2 = feat("encoding_v2")


class Storage:

    def storage(type: str) -> str:
        return f"stor.{type}"

    AMAZON_S3 = storage("AmazonS3")
    LMDB = storage("LMDB")


class Functions:

    def func(function: str) -> str:
        return f"fn.{function}"

    def param(function: str, parameter: str) -> str:
        return f"{function}.{parameter}"
    
    ARCTIC = func("Arctic.__init__")
    ARCTIC_uri = param(ARCTIC, "uri")
    ARCTIC_enc_ver = param(ARCTIC, "encoding_version")
    CREATE_LIB = func("Arctic.create_library")
    CREATE_LIB_name = param(CREATE_LIB, "name")
    CREATE_LIB_lib_opts= param(CREATE_LIB, "library_options")
    CREATE_LIB_ent_lib_opts= param(CREATE_LIB, "enterprise_library_options")


class Marks:
    covers = pytest.mark.covers
    environment = pytest.mark.environment
    slow  = pytest.mark.slow
    prio0 = pytest.mark.prio0
    prio2 = pytest.mark.prio2
    desc  = pytest.mark.desc
    
    def not_ready(reason):
        return pytest.mark.not_ready(pytest.mark.skip(reason=reason))