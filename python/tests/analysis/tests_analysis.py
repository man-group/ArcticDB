import logging
import os
import inspect
import importlib
import sys
from types import FunctionType, ModuleType
from typing import Any, Callable, Dict, List
import pytest

from tests.analysis.adb_meta_info import Functions

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger("Analysis")
logger.setLevel(logging.INFO)

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

class TestFunction:

    PYTEST_MARK_ATTR = 'pytestmark'

    def __init__(self, func: FunctionType):
        self.func: FunctionType = func
        self.__markers = None

    def get_function(self) -> FunctionType:
        return self.func
    
    def get_name(self) -> str:
        return self.func.__name__
    
    def get_docstring(self) -> str:
        return self.func.__doc__

    def get_code(self) -> str:
        return inspect.getsource(self.get_function())
    
    def get_source_file(self) -> str:
        return inspect.getsourcefile(self.get_function())
    
    def get_source_first_line(self) -> str:
        return inspect.getsourcelines(self.get_function())[1]
    
    def get_source_link(self) -> str:
        """
        A string representation of the file and the line
        (when in console output the IDE can locate directly there)
        """
        return f"{self.get_source_file()}, line {self.get_source_first_line()}"


    def is_marked(self) -> bool: 
        return hasattr(self.func, self.PYTEST_MARK_ATTR)

    def get_markers(self) -> List[pytest.Mark]:
        """
        Get all markers attached to the function
        """
        if (self.__markers is None):
            if (self.is_marked()):
                markers = getattr(self.func, self.PYTEST_MARK_ATTR, [])
                self.__log(f"----> Markers: {markers}")
                self.__markers = markers
                return markers
            return []
        else:
            return self.__markers 

    def get_marker_arguments(self, marker_name) -> tuple[Any, ...]:
        """
        Returns a list of arguments for the specified marker
        """
        if self.has_marker(marker_name):
            markers: List[pytest.Mark] = getattr(self.func, self.PYTEST_MARK_ATTR, [])
            for marker in markers:
                if (str(marker_name).lower() in marker.name.lower()):
                    return marker.args
            return ()
        
    def get_markers_argument_values(self) -> List[str]:
        """
        Returns a list of all argument values for all markers decorating this function
        """
        markers: List[pytest.Mark] = self.get_markers()
        result = []
        for mark in markers: 
            for arg in mark.args: 
                result.append(arg)
        return result

    def have_markers_argument_value(self, marker_argument_value):
        """
        Check to see if supplied value is part of any mark's arguments list (args)
        """
        for arg in self.get_markers_argument_values():
            if marker_argument_value == arg:
                return True
        return False

    def have_markers_all_argument_values(self, *marker_argument_values):
        """
        Check to see if supplied value is part of any mark's arguments list (args)
        """
        for val in marker_argument_values:
            if not self.have_markers_argument_value():
                return False
        return True


    def has_marker(self, mark_name: str) -> bool:
        """
        The test is marked with given marker
        """
        markers: List[pytest.Mark] = self.get_markers()
        for marker in markers:
            if (str(mark_name).lower() == marker.name.lower()):
                return True
        return False
    
    def has_markers(self, *mark_names) -> bool:
        """
        The test is marked with given marker
        """
        if (len(mark_names) == 0):
            return len(self.get_markers()) > 0
        for mark_name in mark_names:
            if not self.has_marker(mark_name):
                return False
        return True    
    
    def check_test_mark_parameter(self, pytest_mark_parameter: str, 
                                        condition_func: Callable[[Any], bool]) -> bool:
        """
        Checks marked test if certain parameter matches given condition
        """
        markers: List[pytest.Mark] = self.get_markers()
        for mark in markers: 
            if pytest_mark_parameter in mark.kwargs:
                value = mark.kwargs.get(pytest_mark_parameter)
                if condition_func(value): 
                    return True
        return False
    
    def __str__(self):
        return f"[Function - {self.get_function().__name__}]"
    
    def __repr__(self):
        return self.__str__

    def __log(self, msg: str):
        logger.debug(msg)


class ArcrticdbFixture(TestFunction):

    def __init__(self, fixture: FunctionType):
        super().__init__(fixture)
        self.__fixtures = None
        self.__immediate_fixtures = None

    def get_fixture(self) -> FunctionType:
        return self.get_function()
 
    def __str__(self):
        return f"[Fixture - {self.get_function().__name__}]"

    def __repr__(self):
        return self.__str__

    def get_fixtures(self) -> List['ArcrticdbFixture']: 
        if self.__immediate_fixtures is None:
            self.__immediate_fixtures = ArcrticdbFixture.get_function_fixtures(self.func)
        return self.__immediate_fixtures
    
    def get_all_fixtures(self) -> List['ArcrticdbFixture']: 
        if self.__fixtures is None:
            self.__fixtures = ArcrticdbFixture.get_all_function_fixtures(self.func)
        return self.__fixtures

    @classmethod
    def get_all_function_fixtures(cls, func : FunctionType) -> List['ArcrticdbFixture']: 
        all: List[ArcrticdbFixture] = []
        immediate = ArcrticdbFixture.get_function_fixtures(func)
        if len(immediate) < 1:
            return []
        all.extend(immediate)
        next: List[ArcrticdbFixture] = []
        for fixture in immediate:
            others = ArcrticdbFixture.get_all_function_fixtures(fixture.get_function())
            if (len(others) > 0):
                next.extend(others)
        if (len(next) > 0):
            all.extend(next)
        return all
    
    @classmethod
    def get_function_fixtures(cls, func : FunctionType) -> List['ArcrticdbFixture']: 
        list: List[ArcrticdbFixture] = []
        # Get the function signature 
        try:
            signature = inspect.signature(func) 
        except:
            return list
        # Retrieve the parameters 
        parameters = signature.parameters 
        #  Print the parameter names and details 
        for name, param in parameters.items(): 
            fixture = ArcticdbTestAnalysis().get_fixture_by_name(name)
            if not fixture is None:
                list.append(ArcrticdbFixture(fixture))
        return list


class ArcrticdbTest(TestFunction):

    def __init__(self, function: FunctionType):
        super().__init__(function)
        self.__fixtures = None
        self.__immediate_fixtures = None

    def get_test(self) -> FunctionType:
        return self.get_function()

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
    
    def get_all_fixtures(self) -> List['ArcrticdbFixture']: 
        if self.__fixtures is None:
            self.__fixtures = ArcrticdbFixture.get_all_function_fixtures(self.get_function())
        return self.__fixtures

    def get_fixtures(self) -> List['ArcrticdbFixture']: 
        if self.__immediate_fixtures is None:
            self.__immediate_fixtures = ArcrticdbFixture.get_function_fixtures(self.get_function())
        return self.__immediate_fixtures
    
    def __str__(self):
        return f"[Pytest - {self.get_function().__name__}]"
    
    def __repr__(self):
        return self.__str__


class TestsFilterPipeline:
    '''
    Filtering pipeline for narrowing tests needed
    ''' 

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

    def filter_pytests_marked(self, *pytest_mark_names) -> 'TestsFilterPipeline':
        """
        Filters only test functions/methods marked with specified
        pytest mark
        """
        self.list = self.__filter_tests_pytest_marked(*pytest_mark_names)
        return self
    
    def exclude_pytests_marked(self, *pytest_mark_names) -> 'TestsFilterPipeline':
        """
        Exclude test functions/methods marked with specified
        pytest mark
        """
        self.list = self.__exclude_tests(self.__filter_tests_pytest_marked(*pytest_mark_names))
        return self
    
    def __exclude_tests(self, tests_to_exclude: List[FunctionType]) -> List[FunctionType]:
        result: List[FunctionType] = [item for item in self.list if item not in tests_to_exclude]
        return result

    def __filter_tests_pytest_marked(self, *pytest_mark_names) -> List[FunctionType]:
        functions: List[FunctionType] = []
        for func in self.list:
            if len(pytest_mark_names) < 1:
                if (TestFunction(func).is_marked()):
                    functions.append(func) 
            else:
                if (TestFunction(func).has_markers(*pytest_mark_names)):
                    functions.append(func) 
        return functions

    def filter_pytests_named(self, *pytest_names_have) -> 'TestsFilterPipeline':
        """
        Filters only tests which names(or part of names) are listed as parameters
        """
        self.list = self.__filter_tests_pytest_named(*pytest_names_have)
        return self

    def exclude_pytests_named(self, *pytest_names_have) -> 'TestsFilterPipeline':
        """
        Exlude only tests which names(or part of names) are listed as parameters
        pytest mark
        """
        self.list = self.__exclude_tests(self.__filter_tests_pytest_named(*pytest_names_have))
        return self

    def __filter_tests_pytest_named(self, *pytest_names_have) -> List[FunctionType]:
        functions: List[FunctionType] = []
        for func in self.list:
            if len(pytest_names_have) < 1:
                raise Exception("No test names to filter")
            else:
                test_name = TestFunction(func).get_name()
                for name in pytest_names_have:
                    if (name in test_name):
                        functions.append(func) 
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
            if TestFunction(func).check_test_mark_parameter(pytest_mark_parameter, condition_func):
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
            if (TestFunction(func).have_markers_argument_value(marker_argument_value)):
                functions.append(func)
        return functions
    
    def __filter_tests_which_fixture_is_marked(self, 
                                                *marker_names) -> List[FunctionType]:
        functions: List[FunctionType] = []
        for func in self.list:
            fixtures = ArcrticdbTest(func).get_all_fixtures()
            for fixture in fixtures: 
                if (fixture.has_markers(*marker_names)):
                    functions.append(func)
                    break
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
        #sys.path.append(path_to_tests)
        #sys.path.append(python_files)
        

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
    print(fix_list[0].get_function())
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
               print(f"  fixture - {fix.get_function()}")
    print(f"SKIPIF is used at {len(adbt.get_tests())} tests")

    adbt = ArcticdbTestAnalysis().start_filter().filter_pytests_marked("prio0")
    for test in adbt.get_tests():
        fix_list = test.get_fixtures()
        print(f"Test : {test.get_test()}")
        for fix in fix_list:
            print(f"  fixture - {fix.get_function()}")
            print(f"    Environment - {fix.get_marker_arguments('environment')}")
            print(f"    has - {fix.has_markers('environment')}")
            print(f"    has - {fix.has_markers('prio0')}")
            print(f"    has - {fix.has_markers('prio0', 'environment')}")
            print(f"    has - {fix.has_markers('prio0', 'environment', 'd')}")
            print(f"    has - {fix.has_markers('prio0', 'environme')}")
            print(f"    has - {fix.has_markers('pri0', 'environment')}")
            print(f"    has - {fix.has_markers()}")
    print(f"prio0 used at {len(adbt.get_tests())} tests")

    print("NEW")
    tests = ArcticdbTestAnalysis().start_filter().filter_pytests_marked("prio0").get_tests()
    print_test_list(tests)
    tests = (ArcticdbTestAnalysis().start_filter()
            .filter_pytests_marked("mymark")
            .exclude_pytests_marked("slow")).get_tests()
    print_test_list(tests)

    print("NEW 1")
    adbt = ArcticdbTestAnalysis().start_filter().filter_pytests_marked("prio0")
    for test in adbt.get_tests():
        print(f"Test : {test.get_test()}")
        for f in test.get_fixtures():
            print(f"Fixtures : {f}") 
        for f in test.get_all_fixtures():
            print(f"All Fixtures : {f}")

    print("NEW 2")
    adbt = ArcticdbTestAnalysis().start_filter().filter_pytests_named("test_example").exclude_pytests_named("fixture_1")
    for test in adbt.get_tests():
        print(f"Test : {test.get_name()}")
        print(f" doc : {test.get_docstring()}")
        print(f"code : {test.get_code()}")
        print(f"link : {test.get_source_file()}, line {test.get_source_first_line()}")

    print("End")
