
from tests.analysis.tests_analysis import ArcticdbTestAnalysis

if __name__ == "__main__":

    # Use case to create various reports for analysis purposes
    # You can filter tests/fixtures based on:
    #    - their names (or exclude)
    #    - their test marks (or exclude)
    #    - their marks having certain argument (or exclude )
    adbt = (ArcticdbTestAnalysis().start_filter()
            .filter_pytests_named("test_demo")
            #.exclude_pytests_marked("prio1")
            #.exclude_pytests_marked("slow")
            )
    for test in adbt.get_tests():
        print(f"Test : {test.get_name()}")
        #print(f" doc : {test.get_docstring()}")
        #print(f"code : {test.get_code()}")
        print(f"link : {test.get_source_file()}, line {test.get_source_first_line()}")
        print("=="*40)

    adbt1 = ArcticdbTestAnalysis().start_filter().filter_pytests_marked("prio2")
    adbt1 = ArcticdbTestAnalysis().start_filter().filter_pytests_named("test_delete_batch").save_tests_to_file
    for test in adbt1.get_tests():
        print(f"Test       : {test.get_name()}")
        for f in test.get_all_fixtures():
            print(f"  Fixtures : {f}")
        



            