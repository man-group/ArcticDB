from arcticdb.version_store import NativeVersionStore
from IPython.display import display, Markdown


def check_and_adapt_library(lib):
    """
    Verifies that the type of the library is compatible with the diagnostics engine, and if necessary,
    adapts the library to the type used internally by the engine
    """
    if isinstance(lib, NativeVersionStore):
        return lib
    else:
        display(Markdown(f"the provided library is not supported"))
        return None


def check_symbol_exists(lib, symbol, as_of=None):
    """
    Verifies that the symbol and version (in case it is specified) exist
    """
    if as_of == None:
        string_version = "the latest version"
    else:
        string_version = "the version number " + str(as_of)

    if not lib.has_symbol(symbol, as_of=as_of):
        display(Markdown(f"Symbol does not exists for {string_version} from symbol {symbol}"))
        return False
    else:
        return True
