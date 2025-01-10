import os
import inspect
import importlib
import sys
from types import FunctionType, ModuleType
from typing import Any, Callable, Dict, List
import pytest

from tests.analysis.adb_meta_info import Functions


def arcticdb_test(func): 
    """
    Defines special arcticdb tests.
    Must also be used at arcticdb decorators
    """
    func.artcticdb_test = True 
    return func


def arcticdb_test_decorator(func): 
    """
    Special decorator for arcticdb test decorators.
    All arcticdb test decorators must be decorated with it
    in order to be found
    """
    func.artcticdb_test_decorator = True 
    return func


def arcticdb_mark(func, outer_decorator_func):
    """
    To be used in custom decorators
    """
    func = arcticdb_test(func) # Decorate the function as test
    func.arcticdb_mark_name = outer_decorator_func.__name__
    return func


@arcticdb_test_decorator
def coverage(status=None, category=None):
    """
    Sample arcticdb test decorate.
    """
    def decorator(func):
        func = arcticdb_mark(func, coverage)
        func.status = status
        func.category = category
        return func
    return decorator


PROJECT_DIRECTORY = os.getcwd()


class ArcrticdbFixture:

    def __init__(self, fixture: FunctionType):
        self.fixture = fixture

    def get_fixture(self) -> FunctionType:
        return self.fixture
    
    def get_fixtures(self) -> List['ArcrticdbFixture']: 
        return ArcrticdbFixture.get_function_fixtures(self.fixture)

    @classmethod
    def get_function_fixtures(cls, func : FunctionType) -> List['ArcrticdbFixture']: 
        list: List[ArcrticdbFixture] = []
        # Get the function signature 
        signature = inspect.signature(func) 
        # Retrieve the parameters 
        parameters = signature.parameters 
        #  Print the parameter names and details 
        for name, param in parameters.items(): 
            fixture = ArcticdbTestAnalysis().get_fixture_by_name(name)
            if not fixture is None:
                list.append(ArcrticdbFixture(fixture))
        return list


class ArcrticdbTest:

    def __init__(self, function: FunctionType):
        self.func = function

    def get_test(self) -> FunctionType:
        return self.func

    def as_pytest_name(self) -> str:
        """
        Returns the test as:
            test/integration/../test_module.py::test

        This is the notation pytest uses for test selection
        """
        file = self.func.__module__.replace(".", "/") + ".py"
        return f"{file}::{self.func.__name__}"        

    def is_integration(self):
        return 'tests/integration' in self.as_pytest_name() 

    def is_stress(self):
        return 'tests/stress' in self.as_pytest_name() 

    def is_unit(self):
        return 'tests/unit' in self.as_pytest_name() 
    
    def __str__(self):
        return self.as_pytest_name()
    
    def get_fixtures(self) -> List[ArcrticdbFixture]: 
        return ArcrticdbFixture.get_function_fixtures(self.func)


class TestsFilterPipeline:
    '''
    Filtering pipeline for narrowing tests needed
    ''' 


    PYTEST_MARK_ATTR = 'pytestmark'
    ARCTICDB_MARK_ATTR = 'arcticdb_mark_name'
    ARCTICDB_TEST_ATTR = 'artcticdb_test'

    
    def __init__(self, list: List[FunctionType], verbose: bool = False): 
        self.verbose = verbose
        self.list: List[FunctionType] = list

    def get_tests(self) -> List[ArcrticdbTest]:
        """
        Returns all tests
        """
        return [ArcrticdbTest(x) for x in self.list]

    def save_tests_to_file(self, file_path):
        with open(file_path, "w") as file:
            for test in self.get_tests():
                file.write(f"{test}\n")
    
    def filter_pytests_marked(self, pytest_mark_name: str = None) -> 'TestsFilterPipeline':
        """
        Filters only test functions/methods marked with specified
        pytest mark
        """
        self.list = self.__filter_tests_pytest_marked(pytest_mark_name)
        return self
    
    def exclude_pytests_marked(self, pytest_mark_name: str = None) -> 'TestsFilterPipeline':
        """
        Exclude test functions/methods marked with specified
        pytest mark
        """
        self.list = self.__exclude_tests(self.__filter_tests_pytest_marked(pytest_mark_name))
        return self
    
    def __exclude_tests(self, tests_to_exclude: List[FunctionType]) -> List[FunctionType]:
        result: List[FunctionType] = [item for item in self.list if item not in tests_to_exclude]
        return result

    def __filter_tests_pytest_marked(self, pytest_mark_name: str = None) -> List[FunctionType]:
        functions: List[FunctionType] = []
        for func in self.list:
            has_pytest_mark = hasattr(func, self.PYTEST_MARK_ATTR)
            if has_pytest_mark: 
                if pytest_mark_name is None:
                    functions.append(func) 
                else:
                    markers = getattr(func, self.PYTEST_MARK_ATTR, [])
                    self.__log(f"----> Markers: {markers}")
                    for marker in markers:
                        if (str(pytest_mark_name).lower() in marker.name.lower()):
                            functions.append(func) 
                            break
        return functions
    
    def filter_pytests_where_parameter(self, pytest_mark_parameter: str, 
                                        condition_func: Callable[[Any], bool]) -> 'TestsFilterPipeline':
        """
        Filters tests which pytest mark has parameter value that meets 'condition_func'
        """
        self.list = self.__filter_tests_pytest_marked_parameter(pytest_mark_parameter, condition_func)
        return self
    
    def exclude_pytests_where_parameter(self, pytest_mark_parameter: str, 
                                        condition_func: Callable[[Any], bool]) -> 'TestsFilterPipeline':
        """
        Exclude tests which pytest mark has parameter value that meets 'condition_func'
        """
        self.list = self.__exclude_tests(
            self.__filter_tests_pytest_marked_parameter(pytest_mark_parameter, condition_func)
        )
        return self

    def __filter_tests_pytest_marked_parameter(self, pytest_mark_parameter: str, 
                                             condition_func: Callable[[Any], bool]) -> List[FunctionType]:
        functions: List[FunctionType] = []
        for func in self.list:
            if hasattr(func, self.PYTEST_MARK_ATTR): 
                markers = getattr(func, self.PYTEST_MARK_ATTR, [])
                for mark in markers: 
                    if pytest_mark_parameter in mark.kwargs:
                        value = mark.kwargs.get(pytest_mark_parameter)
                        if condition_func(value): 
                            functions.append(func)
        return functions
    
    def filter_pytests_where_argument_is(self, 
                                         marker_argument_value: str) -> 'TestsFilterPipeline':
        """
        Filters tests which pytest mark has parameter value that meets 'condition_func'
        """
        self.list = self.__filter_tests_pytest_marked_where_argument_is(marker_argument_value)
        return self
    
    def exclude_pytests_where_argument_is(self, 
                                          marker_argument_value: str) -> 'TestsFilterPipeline':
        """
        Exclude tests which pytest mark has parameter value that meets 'condition_func'
        """
        self.list = self.__exclude_tests(
            self.__filter_tests_pytest_marked_where_argument_is(marker_argument_value)
        )
        return self

    def __filter_tests_pytest_marked_where_argument_is(self, 
                                                     marker_argument_value: str) -> List[FunctionType]:
        functions: List[FunctionType] = []
        for func in self.list:
            if hasattr(func, self.PYTEST_MARK_ATTR): 
                markers = getattr(func, self.PYTEST_MARK_ATTR, [])
                for mark in markers: 
                    if marker_argument_value in mark.args: 
                        functions.append(func)
        return functions

    def filter_tests_arcticdb(self, arcticdb_mark_name: str = None) -> 'TestsFilterPipeline': 
        """
        Filters only tests decorated with arcticdb test decorator
        """
        functions: List[FunctionType] = []
        for func in self.list:
            has_custom_attrs = hasattr(func, self.ARCTICDB_TEST_ATTR) 
            if has_custom_attrs: 
                if arcticdb_mark_name is None:
                    functions.append(func) 
                else:
                    has_mark_attrs = hasattr(func, self.ARCTICDB_MARK_ATTR) 
                    if has_mark_attrs:
                        marker = getattr(func, self.ARCTICDB_MARK_ATTR, [])
                        if marker == arcticdb_mark_name:
                            functions.append(func)  
        self.list = functions
        return self
    
    def __log(self, msg: str):
        if self.verbose:
            print(msg)


class ArcticdbTestAnalysis:
    """
    Special class to provide way to inspect arcticdb tests and return

    NOTE: Currently 'tests.hypotesis.*' modules/files cannot be loaded
    due to conflict with hypotesis library overshadowing it, one way to resolve this is to
    rename the package in tests
    """

    __modules: List[ModuleType] = []
    __test_functions: List[FunctionType] = []
    __fixtures: List[FunctionType] = []
    __fixtures_dict: Dict[str, FunctionType] = {}


    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        if (ArcticdbTestAnalysis.__modules is None) or (len(ArcticdbTestAnalysis.__modules) < 1):
            ArcticdbTestAnalysis.__modules = self.__load_test_modules_from_project()
        if (ArcticdbTestAnalysis.__test_functions is None) or (len(ArcticdbTestAnalysis.__test_functions) < 1):
            ArcticdbTestAnalysis.__test_functions = self.__find_test_functions()
        if (ArcticdbTestAnalysis.__fixtures is None) or (len(ArcticdbTestAnalysis.__fixtures) < 1):
            ArcticdbTestAnalysis.__fixtures = self.__find_pytest_fixtures()
            for f in ArcticdbTestAnalysis.__fixtures:
                ArcticdbTestAnalysis.__fixtures_dict[f.__name__] = f
    

    def get_modules(self) -> List[ModuleType]:
        """
        Returns all modules loaded
        """
        return ArcticdbTestAnalysis.__modules

    def get_test_functions(self) -> List[FunctionType]:
        """
        Returns all tests
        """
        return ArcticdbTestAnalysis.__test_functions

    def get_fixtures(self) -> List[FunctionType]:
        """
        Returns all fixtures
        """
        return ArcticdbTestAnalysis.__fixtures
    
    def get_fixture_by_name(self, fixture_name) -> List[FunctionType]:
        """
        Returns fixture by name of None is there is no such found
        """
        return ArcticdbTestAnalysis.__fixtures_dict.get(fixture_name, None)

    
    def start_filter(self) -> TestsFilterPipeline:
        return TestsFilterPipeline(self.get_test_functions(), self.verbose)

    def __log(self, msg: str):
        if self.verbose:
            print(msg)

    def __find_test_functions(self) -> List[FunctionType]: 
        all_functions: List[FunctionType] = []
        for module in self.get_modules():
            functions =  [func for name, func in inspect.getmembers(module, inspect.isfunction) 
                          if name.startswith("test_")]
            all_functions.extend(functions)
        return all_functions

    def __find_pytest_fixtures(self) -> List[FunctionType]: 
        all_functions: List[FunctionType] = []
        for module in self.get_modules():
            functions =  [func for name, func in inspect.getmembers(module) 
                          if hasattr(func, '_pytestfixturefunction')]
            all_functions.extend(functions)
        return all_functions        

    def __load_test_modules_from_project(self) -> List[ModuleType]:
        modules: List[ModuleType] = []

        project_dir = PROJECT_DIRECTORY

        python_files = f"{project_dir}/python"
        path_to_tests = f"{python_files}/tests"

        sys.path.insert(0,python_files)
        sys.path.insert(0,path_to_tests)
        

        for root, _, files in os.walk(python_files):
            for file in files:
                if file.endswith(".py") and file != "__init__.py":
                    file_path = os.path.join(root, file)
                    if file_path.startswith(path_to_tests):
                        try:

                            module_name = file_path.replace(python_files, "").lstrip("/").replace("/", ".")[:-3]
                            importlib.invalidate_caches()
                            module = importlib.import_module(module_name)

                            modules.append(module)
                            self.__log(f"Imported : {module.__file__}")

                        except (Exception, BaseException) as ex:
                            if isinstance(ex, ImportError):
                                self.__log(f"Failed to import module: {module_name}. Exception message: {ex.msg}") 
                                raise ex
                            else:
                                self.__log(f"Error importing: {file_path}")
                                self.__log(f"Error : {ex}")
                    else:
                        self.__log(f"File is filtered out intentionally (not python test file): {file_path}")

        return modules

## Some sample test functions
##########################

@pytest.fixture
def fix1(request):
    return 1

@arcticdb_test
def test_me():
    print("test_me")
    assert True 

def test_me_2():
    print("test_2")
    assert True 

@pytest.mark.mymark
@pytest.mark.slow
def test_me_3():
    print("test_3")
    assert True 

@coverage(status = "ABC", category = "any")
def test_me_4():
    print("test_4")
    assert True 

@pytest.mark.mymark
@pytest.mark.slow(status = "completed")
def test_me_5():
    print("test_5")
    assert True 

@pytest.mark.mymark("first")
def test_me_6(fix1):
    print("test_6")
    assert True 

@pytest.mark.category('slow', 'prio0') 
def test_slow_case_1(): 
    print("test_slow_case_1")
    assert True 
    
@pytest.mark.category('fast', 'prio0') 
def test_fast_case_1():
    print("test_fast_case_1")
    assert True 

@pytest.mark.category('slow') 
def test_slow_case_2():
    print("test_slow_case_2")
    assert True 


## Some util functions
###############################

def print_function_list(tests_list: List[FunctionType]):
    for func in tests_list:
        print(f"Function: {func}")
    print(f"Total : {len(tests_list)}")


def print_test_list(tests_list: List[ArcrticdbTest]):
    for test in tests_list:
        print(f"Pytest : {test}  ({test.func})")
    print(f"Total : {len(tests_list)}")


# Example usage
if __name__ == "__main__":

    ArcticdbTestAnalysis(True) ## Just trigger logging to see what modules are loaded and what not

    functions: List[FunctionType] = ArcticdbTestAnalysis().get_test_functions()
    print_function_list(functions)

    functions: List[ArcrticdbTest] = ArcticdbTestAnalysis().start_filter().filter_pytests_marked().get_tests()
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ArcticdbTestAnalysis().start_filter().filter_pytests_marked("slow").get_tests()
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ArcticdbTestAnalysis().start_filter().filter_tests_arcticdb().get_tests()
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_pytests_marked("slow")
                                     .filter_pytests_where_parameter("status", lambda value: value == "completed")
                                     .filter_pytests_where_parameter("status", lambda value: value.startswith("com") if value else False)
                                     .get_tests())
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ArcticdbTestAnalysis().start_filter().filter_tests_arcticdb("coverage").get_tests()
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ArcticdbTestAnalysis().start_filter().filter_tests_arcticdb("scoverages").get_tests()
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_pytests_marked("mymark")
                                     .filter_pytests_where_argument_is("first")
                                     .get_tests())
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_pytests_marked("category")
                                     .filter_pytests_where_argument_is("slow")
                                     .get_tests())
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_pytests_marked("category")
                                     .filter_pytests_where_argument_is("prio0")
                                     .get_tests())
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_pytests_marked("category")
                                     .filter_pytests_where_argument_is("prio0")
                                     .filter_pytests_where_argument_is("fast")
                                     .get_tests())
    print_test_list(functions)

    functions: List[ArcrticdbTest] = ( ArcticdbTestAnalysis().get_fixtures())
    print_function_list(functions)

    """
        With ability to save filtered tests to files we can later organize
        test execution based on those files
    """
    adbt = (ArcticdbTestAnalysis().start_filter()
            .filter_pytests_marked("mymark")
            .exclude_pytests_marked("slow"))
    functions: List[ArcrticdbTest] = adbt.get_tests()
    print_test_list(functions)
    adbt.save_tests_to_file("/tmp/tests.txt")

    fix_list = adbt.get_tests()[0].get_fixtures()
    print(len(fix_list))
    print(fix_list[0].get_fixture())
    print(fix_list[0].get_fixtures())

    """
        Let's see statistics about skipif
    """
    adbt = ArcticdbTestAnalysis().start_filter().filter_pytests_marked("skipif")
    for test in adbt.get_tests():
        fix_list = test.get_fixtures()
        if len(fix_list) < 1:
            pass
        else:
            print(f"Test : {test.get_test()}")
            for fix in fix_list:
               print(f"  fixture - {fix.get_fixture()}")
    print(f"SKIPIF is used at {len(adbt.get_tests())} tests")

    print("End")
