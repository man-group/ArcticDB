import pytest


class Dataframe:

    def df(feature:str) -> str:
        return f"df.{feature}"

    NO_INDEX = df("no_index")
    RANGE_INDEX = df("range_index")
    TIMESTAMP_INDEX = df("timestamp_index")
    EMPTY = df("empty")
    EMPTY_WITH_SCHEMA = df("empty_with_schema")
    ONE_ROW = df("one_row")
    VERY_WIDE = df("very_wide")
    TINY = df("tiny") # All DF's less than 1k
    MEDIUM = df("tiny") # All DF's > 1k and 100k
    BIG = df("tiny") # All DF's > 100k
    MOSTLY_UNIQUE_VALUES = df("mostly_unique")
    NORMAL_VALUES = df("normal_unique") # there are many repeating values 

class Features:

    def feat(feature: str) -> str:
        return f"ft.{feature}"

    DYNAMIC_SHEMA = feat("dynamic_scema")
    ENCODING_V1 = feat("encoding_v1")
    ENCODING_V2 = feat("encoding_v2")
    COLUMN_SEGMENTS = feat("segments_cols")
    ROW_SEGMENTS = feat("segments_row")
    BOTH_SEGMENTS = feat("segments_both")


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
    CREATE_LIB = func("create_library")
    CREATE_LIB_name = param(CREATE_LIB, "name")
    CREATE_LIB_lib_opts= param(CREATE_LIB, "library_options")
    CREATE_LIB_ent_lib_opts= param(CREATE_LIB, "enterprise_library_options")
    DELETE_LIB = func("delete_library")
    DELETE_LIB_name = param(DELETE_LIB, "enterprise_library_options")
    GET_LIB = func("get_library")
    GET_LIB_name = param(GET_LIB, "name")
    GET_LIB_create_if_missing = param(GET_LIB, "create_if_missing")
    GET_LIB_library_options = param(GET_LIB, "library_options")
    GET_URI = func("get_uri")
    HAS_LIB = func("has_library") 
    HAS_LIB_name = param(HAS_LIB, "has_library_name") 
    LIST_LIB = func("llist_libraries")
    MODIFY_LIB = func("modify_library_option")
    MODIFY_LIB_library = param(MODIFY_LIB, "library")
    MODIFY_LIB_option= param(MODIFY_LIB, "option")
    MODIFY_LIB_option_value= param(MODIFY_LIB, "option_value")
    LIBRARY_OPTS = func("LibraryOptions.__init__")
    LIBRARY_OPTS_ = param(LIBRARY_OPTS, "dynamic_schema")
    LIBRARY_OPTS_ = param(LIBRARY_OPTS, "dedup")
    LIBRARY_OPTS_ = param(LIBRARY_OPTS, "rows_per_segment")
    LIBRARY_OPTS_ = param(LIBRARY_OPTS, "columns_per_segment")


class Marks:
    covers = pytest.mark.covers
    environment = pytest.mark.environment
    slow  = pytest.mark.slow
    prio0 = pytest.mark.prio0
    prio1 = pytest.mark.prio1
    prio2 = pytest.mark.prio2
    desc  = pytest.mark.desc
    bug  = pytest.mark.bug # bug id
    req  = pytest.mark.req # requirement id

    def not_ready(reason):
        return pytest.mark.not_ready(pytest.mark.skip(reason=reason))