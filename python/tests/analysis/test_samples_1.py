

import pytest
from tests.analysis.adb_meta_info import Dataframe, Features, Functions, Marks, Storage


@pytest.fixture
def fixture_lvl_1(request, fixture_lvl_2a):
    return request.param

@pytest.fixture
def fixture_lvl_2a(request):
    return request.param

#region annotations
@Marks.covers( Features.DYNAMIC_SHEMA, 
               Functions.ARCTIC_enc_ver, Functions.CREATE_LIB_name, Functions.DELETE_LIB_name,
               Dataframe.NO_INDEX, Dataframe.EMPTY_WITH_SCHEMA)
@Marks.environment(Storage.AMAZON_S3, Storage.LMDB) 
@Marks.prio0
@Marks.desc("""
    We should aim to have short goal and description of all cases as minimum
    GOAL: ...
    WHAT IT DOES: .....
""")
@Marks.slow
#endregion annotations
def test_demo_feature1():
    pass

#region annotations
@Marks.desc(""" 
            When planning tests we can leave empty bodies like this.
            We can start implementing things OR transfer to colleague for help,
            or implement much later some but keep notes where they are visible

            The test does following things:
            
            Prerequisites: something

            - step 1 describe
                validate: a, b, c
            - step 2 describe

            Cleanup: none

            """)
@Marks.not_ready # Automatically will skip the test
#endregion annotations
def test_demo_testonly_two(fixture_lvl_1):
    pass

#region annotations
@Marks.covers( Features.DYNAMIC_SHEMA, 
               Functions.ARCTIC_enc_ver, Functions.CREATE_LIB_name, Functions.DELETE_LIB_name, 
               Functions.GET_LIB_name,
               Dataframe.NO_INDEX, Dataframe.ONE_ROW)
@Marks.environment(Storage.AMAZON_S3, Storage.LMDB) 
@Marks.desc("""
    We should aim to have short goal and description of all cases as minimum
    GOAL: ...
    WHAT IT DOES: .....
""")
@Marks.bug("12346")
@Marks.prio2
#endregion annotations
def test_demo_feature2(fixture_lvl_1):
    pass

