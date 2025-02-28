
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import GeneralAppendSetup, Storage, StorageInfo
from arcticdb.util.utils import TimestampNumber

class LargeAppendDataModifyCache:
    """
    Stores pre-generated information for `AWSLargeAppendDataModify` test
    dictionary with keys that are for parameter values
    """

    def __init__(self, ):
        self.storage_info: StorageInfo = None
        self.write_and_append_dict = {}
        self.update_full_dict = {}
        self.update_full_dict = {}
        self.update_half_dict = {}
        self.update_upsert_dict = {}
        self.update_single_dict = {}
        self.append_single_dict = {}


set_env = (GeneralAppendSetup(storage=Storage.AMAZON, 
                                    prefix="TEST_BUG54343",
                                    library_options=LibraryOptions(rows_per_segment=1000,columns_per_segment=1000)
                                    )
                                    .set_default_columns(30_000))
num_rows = 2_500


writes_list = set_env.generate_chained_writes(num_rows, 4)
cached_results: LargeAppendDataModifyCache = LargeAppendDataModifyCache()
cached_results.write_and_append_dict[num_rows] = writes_list



timestamp_number = set_env.get_initial_time_number()
end_timestamp_number = timestamp_number + num_rows
set_env.logger().info(f"Frame START-LAST Timestamps {timestamp_number} == {end_timestamp_number}")

# update 2nd half of initial date range
half = (num_rows // 2) 
timestamp_number.inc(half - 3) 
cached_results.update_half_dict[num_rows] = set_env.generate_dataframe(half, timestamp_number)
time_range = set_env.get_first_and_last_timestamp([cached_results.update_half_dict[num_rows]])
set_env.logger().info(f"Time range HALF update { time_range }")

pid = "PID"
set_env.remove_all_modifiable_libraries(True)
set_env.delete_modifiable_library(pid)
lib = set_env.get_modifiable_library(pid)
set_env.logger().info(f"library { lib}")

symbol = set_env.get_symbol_name_template(f"_pid-{pid}")
lib.write(symbol, writes_list[0])
set_env.logger().info(f"Timestamps { set_env.get_first_and_last_timestamp([writes_list[0]])}")

appends_list = writes_list[1:]

print("#    def time_update_half(self, cache, num_rows):")
for i in range(3):
    lib.update(symbol, cached_results.update_half_dict[num_rows])
    set_env.logger().info(f"Timestamps { set_env.get_first_and_last_timestamp([cached_results.update_half_dict[num_rows]])}")
