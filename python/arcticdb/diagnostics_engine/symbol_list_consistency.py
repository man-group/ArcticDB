from IPython.display import display, Markdown
from .library_utils import check_and_adapt_library


def get_non_existent_symbols(lib, sym_list):
    """
    Checks which of the symbols from the provided list are no longer present in the library.
    """
    not_in_lib = []
    for sym in sym_list:
        if not lib.has_symbol(sym):
            not_in_lib.append(sym)
    return not_in_lib


def get_symbols_list_from_cache(lib):
    """
    Retrieves and returns a list of symbols from the cached structure of the provided library.
    """
    return lib.list_symbols(use_symbol_list=True)


def get_symbols_list_from_library(lib):
    """
    Retrieves and returns a list of symbols directly from the provided library.
    """
    return lib.list_symbols(use_symbol_list=False)


def get_all_symbols_from_library(lib):
    """
    Retrieves and returns a list of all the symbols from the provided library, including the deleted ones.
    """
    return lib.list_symbols(use_symbol_list=False, all_symbols=True)


def symbol_list_consistency(lib):
    """
    Analyzes the consistency between the cached symbol list and the actual symbol list in the library.

    This function compares the set of symbols in the cached list with those in the actual library.
    It identifies any discrepancies and provides a detailed report on the state of symbols: alive,
    not cached, cached, deleted, and deleted but still present in the cache.

    Parameters:
    -----------
    lib : object
        The library object for which to perform the consistency check.

    Returns:
    --------
    None
    Outputs a Markdown report detailing the consistency analysis results.

    Example:
    --------
    symbol_list_consistency(my_library)
    """
    lib = check_and_adapt_library(lib)
    if lib is None:
        return

    non_cached_symbol_list_set = set(get_symbols_list_from_library(lib))
    cached_symbol_list = get_symbols_list_from_cache(lib)
    cached_symbol_list_set = set(cached_symbol_list)
    if non_cached_symbol_list_set == cached_symbol_list_set:
        symbols = ", ".join(non_cached_symbol_list_set)
        response = (
            "The symbol list is consistent with the actual set of symbols.\n\n- The set of alive symbols in your"
            f" library are: ```\n{symbols}\n```."
        )
    else:
        diff_set = ", ".join(non_cached_symbol_list_set.difference(cached_symbol_list_set))
        cached_symbols = ", ".join(cached_symbol_list_set)
        response = (
            "The symbol list is not consistent with the actual set of symbols.\n\n- The set of symbols not cached in"
            f" the symbol list are: \n\n```\n{diff_set}\n```.\n\n- The set of symbols cached in the symbol list"
            f" are:\n\n```\n{cached_symbols}\n```."
        )

    deleted_symbols_list = list(set(get_all_symbols_from_library(lib)) - non_cached_symbol_list_set)
    if deleted_symbols_list:
        deleted_symbols = ", ".join(deleted_symbols_list)
        response += f"\n\n- The set of deleted symbols in your library are: ```\n{deleted_symbols}\n```."

    deleted_symbols_present_in_cache_list = get_non_existent_symbols(lib, cached_symbol_list)
    if deleted_symbols_present_in_cache_list:
        deleted_symbols_present_in_cache = ", ".join(deleted_symbols_present_in_cache_list)
        response += (
            "\n\n- The set of deleted symbols in your library that are still present in the symbol list cache list"
            f" are: ```\n{deleted_symbols}\n```."
        )

    display(Markdown(response))
