

from tests.analysis.adb_meta_info import Dataframe, Features, Functions, Marks

@Marks.covers( Features.DYNAMIC_SHEMA, 
              Functions.ARCTIC_enc_ver,
              Dataframe.NO_INDEX)
@Marks.slow
def test_testonly_one():
    assert True

@Marks.desc(""" The test does following things:
            
            Prerequisites: something

            - step 1 describe
                validate: a, b, c
            - step 2 describe

            Cleanup: none

            """)
@Marks.not_ready
def test_testonly_two():
    assert True    