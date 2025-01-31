

import pytest
from tests.analysis.adb_meta_info import Dataframe, Features, Functions, Marks, Storage

## Using custom marks that we control
## can help us override some things if needed at later stage
## see Marks class

#region annotations
@Marks.covers( Features.DYNAMIC_SHEMA, 
               Functions.ARCTIC_enc_ver,
               Dataframe.NO_INDEX)
@Marks.environment(Storage.AMAZON_S3, Storage.LMDB) 
@Marks.prio0
@Marks.desc("""
    We should aim to have short goal and description of all cases as minimum
    GOAL: ...
    WHAT IT DOES: .....
""")
@Marks.slow
#endregion annotations
def test_testonly_one():
    assert True

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
def test_testonly_two():
    assert True    

@Marks.environment(Storage.AMAZON_S3, Storage.LMDB) 
@Marks.prio0
@pytest.fixture(params=["A", "B"])
def factory_fixture_1(request):
    return request.param

@pytest.fixture
def fixture_lvl_1(request, fixture_lvl_2a, fixture_lvl_2b):
    return request.param

@pytest.fixture
def fixture_lvl_2a(request):
    return request.param

@pytest.fixture
def fixture_lvl_2b(request, fixture_lvl_3):
    return request.param

@pytest.fixture
def fixture_lvl_3(request):
    return request.param

@Marks.prio0
def test_example_fixture_1(factory_fixture_1, fixture_lvl_1):
    arg1 = factory_fixture_1
    print (arg1)

@pytest.fixture
def factory_fixture_2(request):
    environment = request.param
    result = None
    if (environment == Storage.AMAZON_S3):
        # Here storage can be  setup
        result = Storage.AMAZON_S3
    elif (environment == Storage.LMDB):
        result =  Storage.LMDB
    return result

## Parameterize fixture from the test
@Marks.environment(Storage.AMAZON_S3, Storage.LMDB) 
@pytest.mark.parametrize("factory_fixture_2", [ Storage.AMAZON_S3, Storage.AMAZON_S3 ], indirect=True)
def test_example_fixture_2(factory_fixture_2): 
    """
    Some test documentation ....
    """
    arg1 = factory_fixture_2
    print (arg1)