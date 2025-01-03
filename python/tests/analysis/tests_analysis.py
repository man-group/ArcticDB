import os
import inspect
import importlib
import sys
from types import FunctionType, ModuleType
from typing import Any, Callable, List
import pytest


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

    def get_tests(self) -> List[FunctionType]:
        return self.list
    
    def filter_tests_pytest_marked(self, pytest_mark_name: str = None) -> 'TestsFilterPipeline':
        """
        Filters only test functions/methods marked with specified
        pytest mark
        """
        self.list = self.__filter_tests_pytest_marked(pytest_mark_name)
        return self
    
    def exclude_tests_pytest_marked(self, pytest_mark_name: str = None) -> 'TestsFilterPipeline':
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
    
    def filter_tests_pytest_marked_parameter(self, pytest_mark_parameter: str, 
                                             condition_func: Callable[[Any], bool]) -> 'TestsFilterPipeline':
        """
        Filters tests which pytest mark has parameter value that meets 'condition_func'
        """
        self.list = self.__filter_tests_pytest_marked_parameter(pytest_mark_parameter, condition_func)
        return self
    
    def exclude_tests_pytest_marked_parameter(self, pytest_mark_parameter: str, 
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
    
    def filter_tests_pytest_marked_where_argument_is(self, 
                                                     marker_argument_value: str) -> 'TestsFilterPipeline':
        """
        Filters tests which pytest mark has parameter value that meets 'condition_func'
        """
        self.list = self.__filter_tests_pytest_marked_where_argument_is(marker_argument_value)
        return self
    
    def exclude_tests_pytest_marked_where_argument_is(self, 
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


    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        if (ArcticdbTestAnalysis.__modules is None) or (len(ArcticdbTestAnalysis.__modules) < 1):
            ArcticdbTestAnalysis.__modules = self.__load_test_modules_from_project()
        if (ArcticdbTestAnalysis.__test_functions is None) or (len(ArcticdbTestAnalysis.__test_functions) < 1):
            ArcticdbTestAnalysis.__test_functions = self.__find_test_functions()

    def get_modules(self) -> List[ModuleType]:
        """
        Returns all modules loaded
        """
        return ArcticdbTestAnalysis.__modules

    def get_tests(self) -> List[FunctionType]:
        """
        Returns all tests
        """
        return ArcticdbTestAnalysis.__test_functions
    
    def start_filter(self) -> TestsFilterPipeline:
        return TestsFilterPipeline(self.get_tests(), self.verbose)

    def __log(self, msg: str):
        if self.verbose:
            print(msg)

    def __find_test_functions(self) -> List[FunctionType]: 
        all_functions: List[FunctionType] = []
        for module in self.get_modules():
            functions =  [func for name, 
                          func in inspect.getmembers(module, inspect.isfunction) 
                          if name.startswith("test_")]
            all_functions.extend(functions)
        return all_functions

    def __load_test_modules_from_project(self) -> List[ModuleType]:
        modules: List[ModuleType] = []

        project_dir = os.getcwd()

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

@arcticdb_test
def test_me():
    return 1

def test_me_2():
    return 1

@pytest.mark.mymark
@pytest.mark.slow
def test_me_3():
    return 1

@coverage(status = "ABC", category = "any")
def test_me_4():
    return 1

@pytest.mark.mymark
@pytest.mark.slow(status = "completed")
def test_me_5():
    return 1

@pytest.mark.mymark("first")
def test_me_6():
    return 1

@pytest.mark.category('slow', 'prio0') 
def test_slow_case_1(): 
    assert True 
    
@pytest.mark.category('fast', 'prio0') 
def test_fast_case_1():
    assert True 

@pytest.mark.category('slow') 
def test_slow_case_2():
    assert True 


## Some util functions
###############################

def print_function_list(funs_list: List[FunctionType]):
    for func in funs_list:
        print(f"Function: {func} : {func.__name__}")
    print(f"Total : {len(funs_list)}")

# Example usage
if __name__ == "__main__":

    ArcticdbTestAnalysis(True) ## Just trigger logging to see what modules are loaded and what not

    functions: List[FunctionType] = ArcticdbTestAnalysis().get_tests()
    print_function_list(functions)

    functions: List[FunctionType] = ArcticdbTestAnalysis().start_filter().filter_tests_pytest_marked().get_tests()
    print_function_list(functions)

    functions: List[FunctionType] = ArcticdbTestAnalysis().start_filter().filter_tests_pytest_marked("slow").get_tests()
    print_function_list(functions)

    functions: List[FunctionType] = ArcticdbTestAnalysis().start_filter().filter_tests_arcticdb().get_tests()
    print_function_list(functions)

    functions: List[FunctionType] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_tests_pytest_marked("slow")
                                     .filter_tests_pytest_marked_parameter("status", lambda value: value == "completed")
                                     .filter_tests_pytest_marked_parameter("status", lambda value: value.startswith("com") if value else False)
                                     .get_tests())
    print_function_list(functions)

    functions: List[FunctionType] = ArcticdbTestAnalysis().start_filter().filter_tests_arcticdb("coverage").get_tests()
    print_function_list(functions)

    functions: List[FunctionType] = ArcticdbTestAnalysis().start_filter().filter_tests_arcticdb("scoverages").get_tests()
    print_function_list(functions)

    functions: List[FunctionType] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_tests_pytest_marked("mymark")
                                     .filter_tests_pytest_marked_where_argument_is("first")
                                     .get_tests())
    print_function_list(functions)

    functions: List[FunctionType] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_tests_pytest_marked("category")
                                     .filter_tests_pytest_marked_where_argument_is("slow")
                                     .get_tests())
    print_function_list(functions)

    functions: List[FunctionType] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_tests_pytest_marked("category")
                                     .filter_tests_pytest_marked_where_argument_is("prio0")
                                     .get_tests())
    print_function_list(functions)

    functions: List[FunctionType] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_tests_pytest_marked("category")
                                     .filter_tests_pytest_marked_where_argument_is("prio0")
                                     .filter_tests_pytest_marked_where_argument_is("fast")
                                     .get_tests())
    print_function_list(functions)

    functions: List[FunctionType] = ( ArcticdbTestAnalysis().start_filter()
                                     .filter_tests_pytest_marked("mymark")
                                     .exclude_tests_pytest_marked("slow")
                                     .get_tests())
    print_function_list(functions)



    print("End")
