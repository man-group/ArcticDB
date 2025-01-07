import pytest

def pytest_addoption(parser):
    print("pytest_addoption")
    parser.addoption("--testlist", action="store", default=None, help="Path to file with list of tests to run")
    parser.addoption("--force", action="store_true", default=None, help="Parameter to force execution of tests even one or more are not found")

@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """
    This plugin gives ability pytest to run tests based on list stored in configuration file. List 
    should contain names of tests like:
       tests/integration/../test_module::test_a
       tests/integration/../test_module::test_b

    Later from command line execute:

        pytest --testlist=[file_path] [--force]
    """

    testlist_path = config.getoption("--testlist")

    if testlist_path:

        print(f"PYTEST PLUGIN - READ TESTS FROM FILE ACTIVATED")
        print(f"TOTAL TESTS Available:  ${len(items)}")

        dict_itms = {}
        for item in items:
            dict_itms[item.nodeid] = item

        print(f"TEST FILE TO READ DATA FROM: {testlist_path}")

        with open(testlist_path, 'r') as f:
            tests_to_run = set(line.strip() for line in f)

        print(f"TESTS SELECTED BE EXECUTED FROM TEST FILE: {tests_to_run}")
        
        test_names = dict_itms.keys()
        result = []
        not_found = []
        for test_to_run in tests_to_run:
            found = False
            for test_name in test_names:
                if test_to_run in test_name:
                    found = True
                    result.append(dict_itms[test_name])
            if not found:
                not_found.append(test_to_run)
        
        if (len(not_found) > 0):
            print("WARNING!")
            print(f"FOLLOWING TESTS ({len(not_found)}) REQUESTED BUT NOT FOUND IN LIBRARY")
            print(f"{not_found}")
            force = config.getoption("--force")
            if not force:
                raise Exception(f"There are ({len(not_found)}) tests requested for execution but not found (use '--force' to force execution to all found): {not_found}")

        items[:] = result
        print(f"Number of tests selected to Run: ${len(items)}")