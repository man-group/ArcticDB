import pandas as pd
import numpy as np
import pytest
from arcticdb import OutputFormat


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize(
    "date_range_start,date_range_end",
    [
        (0, 0),
        (0, 1),
        (1, 1),
        (1, 2),
        (2, 2),
        (2, 3),
        (3, 3),
        (3, 4),
        (4, 4),
        (4, 5),
        (5, 5),
        (5, 6),
        (6, 6),
    ],
)
def test_bool_only(version_store_factory, date_range_start, date_range_end, dynamic_schema):
    lib = version_store_factory(
        segment_row_size=2, column_group_size=2, dynamic_schema=dynamic_schema, dynamic_strings=True
    )
    lib.set_output_format(OutputFormat.PYARROW)
    df = pd.DataFrame(
        data={
            "col_bool": [True, False, True, True, False, False, False],
        },
        index=pd.date_range(pd.Timestamp(0), freq="ns", periods=7),
    )
    sym = f"test_bool_only_{date_range_start}_{date_range_end}_{dynamic_schema}"
    lib.write(sym, df)

    date_range = (pd.Timestamp(date_range_start), pd.Timestamp(date_range_end))
    expected_df = lib.read(sym, date_range=date_range, output_format=OutputFormat.PANDAS).data
    actual = lib.read(sym, date_range=date_range).data
    actual_df = actual.to_pandas()
    pd.testing.assert_frame_equal(expected_df, actual_df)
