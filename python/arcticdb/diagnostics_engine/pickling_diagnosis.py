from IPython.display import display, Markdown
from .library_utils import check_and_adapt_library


def pickling_diagnosis(lib, frame):
    """
    This function checks if the data (frame) to be stored in the ArcticDB library (lib) will be pickled or not,
    and provides implications if the data needs to be pickled. It also suggests recommendations to avoid pickling.

    Parameters:
    ----------
    lib : str
        The library where the data is to be stored.
    frame : DataFrame
        The data to be stored.

    Returns:
    -------
    None
    """
    lib = check_and_adapt_library(lib)
    if lib is None:
        return

    data_will_be_pickled = lib.will_item_be_pickled(frame)

    if data_will_be_pickled:
        description = f"""
        \nThe data you are trying to store cannot be normalized and therefore will be pickled. This has several implications:

        \n1. **Increased Storage Space**: Pickled data consumes more storage space compared to normalized data.

        \n2. **Reduced Performance**: In arcticDB, pickled data is less performant for both read and write operations compared to normalized data. Unpickling is a Python-layer process, and it may be slower than reading non-pickled data.

        \n3. **Limited Operations**: Certain operations available for non-pickled data are not supported for pickled data. You cannot append to, update, perform advanced read query features, or any kind of filtering on pickled data due to the lack of indexing or column information in the database's index layer.

        \n**Recommendation**: If possible, consider transforming your data into a format that can be normalized, which allows for more efficient storage and operations.
        """
    else:
        description = f"""the data can be normalised. Therefore it will not be pickled"""
    display(Markdown(description))
