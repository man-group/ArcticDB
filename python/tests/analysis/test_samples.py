

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