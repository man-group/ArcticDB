from IPython.display import display, Markdown
from .library_utils import check_and_adapt_library
from arcticdb.options import LibraryOptions

EXPLANATIONS = {
    "segment_row_size": """
    \n This parameter determines the maximum number of rows per segment. It affects how the data is segmented in your 
    library. Data is segmented based on the number of rows, up to the limit defined by this parameter.

    \n For example, if `segment_row_size` is set to 200,000 and your data contains 300,000 rows, your data will be 
    divided into two segments. The first segment will contain the first 200,000 rows, and the second segment will 
    contain the remaining 100,000 rows. 

    \n This division can lead to inefficiencies, especially in smaller segments. Smaller segments can result in poor 
    storage usage and reduced performance. If you frequently encounter this situation, you might want to consider 
    increasing the `segment_row_size` or performing larger appends or updates. However, proceed with caution as 
    this could have other implications and might affect other symbols in the same library.""",
    "column_group_size": """
    \n This parameter controls the maximum number of columns per segment when dynamic_schema is False. 
    If dynamic_schema is enabled, this feature is not used unless bucketize_dynamic is also enabled.
    
    \n The `column_group_size` affects how the data is segmented in your library. Data is segmented according to the 
    number of columns, up to the limit defined by this parameter. For example, if `column_group_size` is set to 50, 
    and your data contains 60 columns, then your data will be divided into two segments. The first segment will 
    contain the first 50 columns, and the second segment will contain the remaining 10 columns""",
    "prune_previous_version": """
    \n The prune_previous_version setting in Arctic is a parameter that, when enabled, allows the system to
    automatically prune or delete previous versions of data upon a new write operation.
    \n Here's a more detailed explanation:
    
    \n- Automatic Pruning: When prune_previous_version is set to True, any write, append or update operation will
    automatically delete all the older versions of the data. This means that only the latest version of the data 
    is retained.
    \n- Method-Level Configuration: prune_previous_version can also be set as an argument during the method invocation
    in Python. When configured this way, it will override the library-level configuration. This provides flexibility
    to control the pruning behavior on a per-operation basis.
    
    \n This feature can be useful for maintaining a clean and compact data library, particularly when storage space
    is a concern or when old versions of the data are no longer needed. However, it's important to use this feature
    carefully as the deletion of old versions is irreversible.
    """,
    "de_duplication": """
    \n If enabled, Arctic checks for identical segments in the previous version of the symbol being written and
    skips writing those again. This feature is most effective when the new version of your data is the previous
    version plus additional data added to the end. It's crucial to note that the additional data must be only
    at the end for de-duplication to work effectively.

    \n If any data is inserted in the middle, all segments occurring after that modification will differ and
    de-duplication will not be effective. This is because Arctic creates new segments at fixed intervals and
    only performs de-duplication when the hashes of the data segments are identical. Even a one-row offset
    will prevent de-duplication.

    \n Append and update operations do not look for deduplicated segments. It's also important to understand
    that de-duplication is symbol-specific. It only checks for duplications within the same symbol, not across different symbols.
    """,
    "dynamic_strings": """
    \n This parameter controls how string columns are handled in your data:

    \n-  If `dynamic_strings` is disabled (False), you can't mix strings with None / NaNs in your column. The
    column data type is a numpy fixed-width string array, which doesn't allow for None / NaNs. Also, each string
    in the column is stored at its full, fixed length, which can lead to data bloat if the strings vary greatly
    in length or consist of many short strings.
    \n-  If `dynamic_strings` is enabled (True), which is the default setting, you can mix strings with None / NaNs
    in your column. The column data type is a deduplicated UTF8 string, which allows for None / NaNs. In the C++ layer
    each string is stored at its actual length, which can greatly reduce data size compared to fixed-width strings.

    It's important to note that this is a bit of a legacy option. Arctic used to store string columns as numpy
    fixed-width string arrays by default, but now the default is to store deduplicated UTF8 strings instead.
    """,
    "recursive_normalizers": """
    \n A recursive normalizer is a functionality that allows for the storage of complex data structures by
    breaking them down, or "normalizing" them, into their constituent parts. This is particularly useful when dealing
    with nested data structures like dictionaries and arrays that contain other data types, such as data from pandas or numpy.
    \n Here's a deeper explanation:
    
    \n- Normalization of Containers: Recursive normalizers can handle containers, which are data structures that
    hold or contain other data or objects. Examples of containers include dictionaries and arrays, amongst others.
    The recursive normalizer can traverse through these containers, reach each individual data item, and normalize it.
    \n- Support for Custom Types: Beyond standard data types and containers, recursive normalizers can also handle
    custom data types. This is achieved by using a custom normalizer function, which you define. This function should
    be able to take your custom data type and return a standard container with normalized data.
    \n- Registration of Custom Normalizers: In order for the recursive normalizer to know how to handle your custom
    data type, you must register your custom normalizer function. This way, when the recursive normalizer encounters
    your custom data type, it will know to use your function to normalize it.
    
    \n This functionality can be leveraged to efficiently store complex data structures like dictionaries containing
    pandas DataFrames as values, without necessarily having to pickle the individual DataFrames. This can lead to a
    more efficient and flexible storage system
    """,
    "pickle_on_failure": "Determines if a pickle operation should be performed if an object cannot be normalized.",
    "delayed_deletes": """
    \n The delayed_deletes setting in ArcticDB is a mechanism designed to improve the efficiency of deletion operations.
    When delayed_deletes is enabled, the deletion of symbols happens in two stages.
    \n Here's a more detailed explanation:
    
    \n- Immediate Logical Deletion: As soon as a deletion operation is invoked, a marker, often called a "tombstone",
    is written to indicate that the symbol or version is marked for deletion. This means that for subsequent read
    operations, the data appears as if it has been deleted straight away.
    \n- Asynchronous Physical Deletion: The actual removal of the data, which involves deleting the underlying data
    from the storage, is performed by an asynchronous process. This process operates in the background and physically
    deletes the data marked by the tombstones.
    
    \n This approach has improved performance benefits. Deletion operations can be time-consuming, especially if the
    underlying storage devices have performance constraints. By enabling delayed_deletes, the time-consuming physical
    deletion of data is offloaded to a background job, making the delete operation appear faster from the user's perspective.
    """,
    "allow_sparse": """
    \n Should be enabled only for libraries written by the Arctic Native Streaming Collector tools. If True, Arctic
    will expect sparse data in the library and read it correctly. Currently, sparse data can only be written by
    the Arctic Native Streaming Collector tools.""",
    "consistency_check": "This feature flag is due for removal and shouldn't be changed from the default value.",
    "dynamic_schema": """    
    \n The dynamic_schema setting in Arctic allows for flexibility in terms of the data structure for a symbol
    (a kind of identifier for your data). When dynamic_schema is set to True, the schema of a symbol can be 
    modified during append and update operations. This enables you to add new data with more, fewer, or
    different columns compared to the existing data.
    \n Here are some key points to consider:
    
    \n- Appending Additional Columns: If your library has dynamic_schema enabled, you can append data with additional
    columns to existing symbols. This can be beneficial if your data structure evolves over time.
    \n- Modifying on Empty Libraries: Changing the dynamic_schema setting is only supported for empty libraries.
    You should never enable dynamic_schema on a library that already contains data. This means that once data
    has been written to a library, it is not supported to change the dynamic_schema property.
    \n- Memory Considerations: Enabling dynamic_schema can have implications on memory and storage efficiency
    in certain scenarios, specifically when the number of columns in the data exceeds the column group size.
    In these cases, an internal operation called column slicing is typically triggered, which can actually
    lead to more efficient memory usage. However, when dynamic_schema is enabled, it prevents column slicing
    from happening, which might increase memory and storage consumption. An existing feature, bucketize_dynamic,
    may help manage this impact, though its usage is not widespread.
    \n- Performance Considerations: Disabling dynamic_schema can lead to improved performance. If you know that
    a library will not require schema changes in the future, it is recommended to keep dynamic_schema disabled
    to benefit from this performance boost.
    
    \n Remember that the decision to enable or disable dynamic_schema should be made considering the specific
     requirements and constraints of your use case.
    """,
    "incomplete": """
    \n This feature should be enabled only for libraries written by the Arctic Native Streaming Collector tools.
    These tools write incomplete segments to storage. These temporary segments are not part of the overall symbol
    index, making them slower to read, but their existence ensures that new streaming data events can be read
    quickly. Incomplete segments are later compacted. This setting controls whether Arctic should scan for
    incomplete segments during read operations.""",
    "set_tz": (
        "This feature should be enabled only for libraries written by the Arctic Native Streaming Collector tools."
    ),
    "ignore_sort_order": """
    \n If True, this allows append operations to add data to the end of existing data, even if the index of the
    appended data does not begin after the existing data. If False, the index of the data being appended must
    begin after the end of the existing data's index.""",
    "fast_tombstone_all": "This feature flag is due for removal and shouldn't be changed from the default value.",
    "bucketize_dynamic": """
    \n This feature can be used only on libraries that have Dynamic Schema enabled. If enabled, Arctic will segment
    data across a number of buckets to improve performance with a large number of columns (in the order of
    thousands).""",
    "max_num_buckets": "The maximum number of buckets to be used when bucketize_dynamic is enabled",
    "snapshot_dedup": """
    \n If enabled, Arctic will scan all snapshots containing the given symbol for data segments to deduplicate
    against if the prior version of the symbol has been deleted. This can be time consuming and is not
    recommended unless data is typically kept alive through the use of snapshots.""",
    "compact_incomplete_dedup_rows": """
    \n This feature should be enabled only for libraries written by the Arctic Native Streaming Collector tools.
    These tools write incomplete segments to storage. These temporary segments are slower to process but ensure
    that new streaming data events can be read quickly. Incomplete segments are later compacted. This setting
    controls whether to deduplicate identical rows when compacting said incomplete segments.""",
    "use_norm_failure_handler_known_types": """
    \n This is a setting in ArcticDB that deals with the storage of data that cannot be natively handled. If this flag
    is enabled, Arctic will resort to "pickling" the data when it encounters a type that it can't store in its native format.
    
    \n- Pickling is a process in Python for serializing and de-serializing a Python object structure. Any object
    in Python can be pickled so that it can be saved on disk, and then later, the pickled file can be unpickled
    back into the original object.
    \n- However, pickling comes with certain limitations. One key drawback is that pickled data cannot be indexed.
    This means you can't effectively perform operations like date range queries or column slicing, which are
    common in data analysis tasks.
    \n- Additionally, if a Pandas DataFrame contains even a single non-primitive data type object (i.e., not an
    integer, float, string, or boolean), the entire DataFrame will need to be pickled. This could potentially
    impact performance and the ease of data manipulation.
    
    \n For a more detailed understanding of pickling, including how it works and its various use cases and
    limitations, please refer to the specific 'Pickling' section in this notebook.
    """,
    "symbol_list": """
    \n If enabled, the symbol list caching functionality of Arctic will be used. This is designed to improve
    performance of list_symbols operations. For a more detailed understanding of this feature, including how it
    works and its various use cases and limitations, please refer to the specific 'Symbol list consistency
    diagnosis' section in this notebook.""",
    "strict_mode": """
    \n If normalisation is running in strict mode, writing pickled data is disabled""",
}


class LibraryInfoGetter:
    """
    This class provides a method for displaying and explaining the various options
    associated with a specific library. It aims to provide users with a clear understanding
    of each option, including its purpose, usage implications, and potential impact on
    library operations.

    Methods:
    --------
    display_library_options():
        Fetches and displays the options for a given library, along with detailed
        explanations for each option.

    Example Usage:
    --------------
    info_getter = LibraryInfoGetter(lib, EXPLANATIONS)
    info_getter.display_library_options()
    """

    def __init__(self, lib, is_internal_api, explanations):
        self.response_obj = lib._lib_cfg.lib_desc.version
        self.is_internal_api = is_internal_api
        self.explanations = explanations
        self.response_non_default_str = (
            "## Next, see the list of your library options that has been modified with respect to the default"
            " values:\n\n"
        )
        self.response_default_str = (
            "## Next, see the list of your library options that remains with the default values:\n\n"
        )

        # Default values defined internally
        self.c_plus_plus_overwritten_when_zero = {
            "column_group_size": 127,
            "segment_row_size": 100000,
            "max_num_buckets": 150,
            "max_blob_size": 16777216,
        }

        self.internal_default_values = {
            "prune_previous_version": True,
            "dynamic_strings": True,
            "use_tombstones": True,
            "fast_tombstone_all": True,
            "symbol_list": True,
        }

        # values that are not a library option, starting from lib._lib_cfg.lib_desc.version
        self.exclusion_set = {
            "write_options.sync_disabled.enabled",
            "write_options.sync_passive.enabled",
            "write_options.sync_active.enabled",
            "failure_sim.write_failure_prob",
            "failure_sim.read_failure_prob",
            "deleted",
            "prometheus_config.instance",
            "prometheus_config.host",
            "prometheus_config.port",
            "prometheus_config.job_name",
            "prometheus_config.prometheus_env",
            "prometheus_config.prometheus_model",
            "event_logger_config.logstash_config.event_type",
            "event_logger_config.logstash_config.host",
            "event_logger_config.logstash_config.port",
            "event_logger_config.logstash_config.allow_name_lookup",
            "event_logger_config.file_logger.file_path",
            "storage_fallthrough",
        }

        self.external_library_to_proto_names = {
            "dedup": "de_duplication",
            "rows_per_segment": "segment_row_size",
            "columns_per_segment": "column_group_size",
        }

    def traverse_descriptor(self, obj, path=""):
        """
        Traverses the fields of the provided protobuf object, compares their values with default ones,
        and updates response strings accordingly. The function is recursive for nested protobuf message types.
        """
        default_response_obj = type(obj)()
        for field in obj.DESCRIPTOR.fields:
            value = getattr(obj, field.name)
            full_name = path + "." + field.name if path else field.name
            if field.message_type:
                self.traverse_descriptor(value, full_name)
            elif full_name not in self.exclusion_set:
                default_value = (
                    getattr(default_response_obj, field.name)
                    if (field.name not in self.internal_default_values)
                    else self.internal_default_values[field.name]
                )
                explanation = self.explanations.get(field.name, "No explanation provided.")
                if value != default_value:
                    if default_value == 0 and field.name in self.c_plus_plus_overwritten_when_zero:
                        default_value = self.c_plus_plus_overwritten_when_zero[field.name]
                    self.response_non_default_str += (
                        f"\n### **{field.name}**: {value} (Default: {default_value})\n\n{explanation}\n\n"
                    )
                else:
                    if value == 0 and field.name in self.c_plus_plus_overwritten_when_zero:
                        value = self.c_plus_plus_overwritten_when_zero[field.name]
                    self.response_default_str += f"\n### **{field.name}**: {value}\n\n{explanation}\n\n"

    def traverse_library_options(self, obj):
        """
        Traverses the fields of library_options, compares their values with default ones,
        and updates response strings accordingly
        """
        default_library_options = LibraryOptions()
        attributes = vars(default_library_options)
        for attribute, default_value in attributes.items():
            field_proto_name = (
                attribute
                if attribute not in self.external_library_to_proto_names
                else self.external_library_to_proto_names[attribute]
            )
            value = self.find_library_value_from_proto_obj(obj, field_proto_name)
            if value is not None:
                explanation = self.explanations.get(field_proto_name, "No explanation provided.")
                if value != default_value:
                    self.response_non_default_str += (
                        f"\n### **{attribute}**: {value} (Default: {default_value})\n\n{explanation}\n\n"
                    )
                else:
                    self.response_default_str += f"\n### **{attribute}**: {value}\n\n{explanation}\n\n"

    def find_library_value_from_proto_obj(self, obj, field_name):
        for proto_field in obj.DESCRIPTOR.fields:
            if proto_field.message_type:
                value = self.find_library_value_from_proto_obj(getattr(obj, proto_field.name), field_name)
                if value is not None:
                    return value
            elif field_name == proto_field.name:
                return getattr(obj, field_name)
        return None

    def display_library_options(self):
        self.traverse_descriptor(self.response_obj) if self.is_internal_api else self.traverse_library_options(
            self.response_obj
        )
        display(Markdown(self.response_non_default_str))
        display(Markdown(self.response_default_str))


# function to fetch and display library information
def display_library_options(lib):
    """
    Fetches and presents the options for a specified library.

    Parameters
    ----------
    lib : object
        The library object whose options are to be fetched and displayed.

    Returns
    -------
    None

    Notes
    -----

    For a valid library, a `LibraryInfoGetter` instance is created, using the library and a predefined
    dictionary of option explanations (`EXPLANATIONS`). The `display_library_options` method of this instance
    is then invoked to display the library's options.

    The library options are presented in terms of both default and non-default settings. Non-default settings
    refer to options that have been altered from their defaults. For each setting, an explanation is provided
    to clarify its role and impact.
    """
    lib, is_internal_api = check_and_adapt_library(lib)
    if lib is None:
        return
    info_getter = LibraryInfoGetter(lib, is_internal_api, EXPLANATIONS)
    info_getter.display_library_options()
