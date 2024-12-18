- Recreate my profiling script from before
- Stage everything to some persistent place so I can repeatedly finalize?
- Finalize and profile with massif

- Mismatched count, 4602 to finalize, end with 3982 finalized and 3364 still staged?

Here is the code i am using to load the parquet file to arcticdb. I lied when I said I an splitting the files by record batches. I am splitting it by number of rows. (edited)

```
class TickDataReader(ABC):
    """High level abstract interface for reading parquet files in batches."""

    def __init__(self, batch_size: int) -> None:
        """Initializes the reader using arguments."""
        self.batch_size = batch_size
        # Subclass must define this for their file system.
        self.filesystem: fs.FileSystem

    @abstractmethod
    def _get_base_dir(self, event_date: date, exbyte: str) -> str:
        """Returns the file pattern for the specific file system.

        Subclasses must override this function for their specific filesystem.
        """

    def read(self, universe: Universe) -> Iterator[tuple[DateExbyte, pd.DataFrame]]:
        """Returns an iterator over the underlying parquet file."""
        for cur_date, exbyte in universe.keys():
            filenames = self.get_parquet_file_list(cur_date, exbyte)
            for filename in filenames:
                logger.info(
                    "Processing exbyte=%s cur_date=%s key=%s",
                    exbyte,
                    cur_date,
                    filename,
                )
                for df in self.read_single_file(filename):
                    yield (cur_date, exbyte), df

    @staticmethod
    def _get_most_recent_version(candidates: list[str]) -> list[str]:
        """Returns only the most recent version of the parquet file(s).

        Candidates is a list of parquet file names (strings) that contain data for the
        given exchange byte and date. Note that the function does no validation on
        whether these files are parquet files.
        These parquet files follow a specific naming convention: YYYYMMDD_HHMMSS_*.
        The date prefix refers to the datetime when the file was generated (hence the
        version). We only load the most recent version.
        """
        filename_to_path = {PurePath(path).name: path for path in candidates}
        # If there are spurious files in directory, then this will fail.
        file_dates = set(name[:_DATE_PREFIX_LENGTH] for name in filename_to_path.keys())
        if not file_dates:
            logger.warning("No files matching the desired pattern found.")
            return []
        # Conveniently lexically sorting date strings is same as sorting the dates.
        # So we do not convert strings to dates.
        max_ts = max(file_dates)
        # Lexical sort of filenames should also sort them temporally.
        return sorted(
            [
                path
                for filename, path in filename_to_path.items()
                if filename.startswith(max_ts)
            ]
        )

    def get_parquet_file_list(self, event_date: date, exbyte: str) -> list[str]:
        """Returns tickdata files for the given exchange byte and date.

        Args:
            event_date: Date for which to list the files.
            exbyte: Exchange byte as hex string that begins with 0x.

        Return:
            An iterator over the list of files for the given date and exbyte.
        """
        pattern = self._get_base_dir(event_date, exbyte)
        obj_list = self.filesystem.get_file_info(
            fs.FileSelector(pattern, recursive=True, allow_not_found=True)
        )
        candidates = [item.path for item in obj_list if item.type == fs.FileType.File]
        return self._get_most_recent_version(candidates)

    def read_single_file(self, filename: str) -> Iterator[pd.DataFrame]:
        """Read a single parquet file from S3."""
        try:
            logger.debug(
                "Processing file %s with batch size %s",
                filename,
                self.batch_size,
            )
            # Sometimes, we do get an exception reading a legit file. It will
            # manifest itself in the logs as:
            # OSError: When reading information for key '<key>' in bucket
            # '<bucket>': AWS Error UNKNOWN (HTTP status 503) during HeadObject
            # operation: No response body.
            # We will just retry more than the default number of times in such a case.
            pq_file = ParquetFile(
                filename,
                filesystem=self.filesystem,
            )
            for batch in pq_file.iter_batches(
                batch_size=self.batch_size, columns=ALLOWED_COLUMNS
            ):
                dataframe = batch.to_pandas()
                dataframe = normalize_dataframe(dataframe)
                yield dataframe
        except pyarrow.lib.ArrowInvalid as ex:
            logger.error(
                "Error occurred for filename=%s, err=%s",
                filename,
                ex,
            )


class S3ParquetReader(TickDataReader):
    """Reads parquet file from AWS S3."""

    # This is the filename pattern we currently follow in bqe-managed-finml bucket.
    _FILE_PATTERN = (
        "{bucket}/{prefix}/prod-bucket-{year:04}{month:02}/{day:02}/{exbyte}"
    )

    def __init__(
        self,
        batch_size: int,
        bucket: str,
        prefix: str,
        **aws_client_args: dict[str, object],
    ) -> None:
        """Setup AWS client for the reader."""
        super(S3ParquetReader, self).__init__(batch_size)
        self.bucket = bucket
        self.prefix = prefix

        # AWS Client is used solely to auto refresh credentials periodically.
        self.aws_client = awsutl.AwsClient(
            token_refresh_threshold=timedelta(hours=2),
            **aws_client_args,  # type: ignore
        )
        # Unfortunately, S3FileSystem does not use boto3. So we have do some funny
        # business to bridge the differences. Luckily these will kick in mainly during
        # tests.
        self.filesystem = fs.S3FileSystem(
            endpoint_override=aws_client_args.get("endpoint_url", None),
            # Default transport scheme is HTTPS.
            proxy_options=os.environ.get("HTTPS_PROXY", None),
            retry_strategy=fs.AwsStandardS3RetryStrategy(max_attempts=10),
        )

    def _get_base_dir(self, event_date: date, exbyte: str) -> str:
        return self._FILE_PATTERN.format(
            bucket=self.bucket,
            prefix=self.prefix,
            year=event_date.year,
            month=event_date.month,
            day=event_date.day,
            exbyte=exbyte,
        )

    def read_single_file(self, filename: str) -> Iterator[pd.DataFrame]:
        """Read the single parquet file in S3."""
        try:
            self.aws_client._refresh_token_if_required()
            return super().read_single_file(filename)
        except Exception as ex:
            logger.error(
                "Exception reading file  filename=%s: %s,"
                "current_time=%s, token_expiration_time=%s",
                filename,
                ex,
                datetime.now(),
                self.aws_client._token_expiration,
            )
            raise ex

    def get_parquet_file_list(self, event_date: date, exbyte: str) -> list[str]:
        """Returns tickdata files for the given exchange byte and date."""
        self.aws_client._refresh_token_if_required()
        return super().get_parquet_file_list(event_date, exbyte)
```

Writer:

```
class TickDataArcticDBWriter:
    """Class for writing tickdata to arcticdb datastore."""

    def __init__(
        self,
        arcticdb_url: str,
        arcticdb_lib: str,
        universe: Universe,
        reader: TickDataReader,
        source_data_sorted: bool = False,
    ) -> None:
        """Initialize an arcticdb writer based on the configuration.

        Args:
            arcticdb_url: The URL where arctidb data store is located. A new one
                will be created if it does not exist.
            arcticdb_lib: The name of the arcticdb library.
            universe: Tick data universe to process. This provides mapping from tpoids
                to figi+pcs and relevant dates.
            reader: Reader to read the tickdata from.
            source_data_sorted: Flag indicating whether the source data are sorted.
        """
        logger.info(
            "Initializing TickDataArcticDBWriter for arcticdb_url=%s, "
            "arcticdb_library=%s, source_data_sorted=%s",
            arcticdb_url,
            arcticdb_lib,
            source_data_sorted,
        )
        self._writer = WriterFactory.get_writer(arcticdb_url, arcticdb_lib)
        self._reader = reader
        self._universe = universe
        self._source_data_sorted = source_data_sorted

    def _process_chunk(
        self, cur_date: date, exbyte: str, dataframe: pd.DataFrame
    ) -> set[str]:
        unique_tpoids_in_universe = set(self._universe[(cur_date, exbyte)].keys())
        unique_tpoids_in_df = set(dataframe["BAHD_KEY_NAME"])
        unique_tpoids_in_common = unique_tpoids_in_df.intersection(
            unique_tpoids_in_universe
        )
        dataframe_for_symbols = dataframe[
            dataframe.BAHD_KEY_NAME.isin(unique_tpoids_in_common)
        ]
        symbols = set()
        for tpoid in unique_tpoids_in_common:
            symbol = get_symbol_name(self._universe, cur_date, tpoid)
            assert symbol  # symbol should never be None here
            dataframe_for_symbol = dataframe_for_symbols[
                dataframe_for_symbols.BAHD_KEY_NAME == tpoid
            ]
            if not dataframe_for_symbol.empty:
                symbols.add(symbol)
            self._writer.stage_dataframe_for_write(dataframe_for_symbol, symbol)
            logger.debug(
                "Write to arcticdb symbol=%s, rows=%d",
                symbol,
                len(dataframe_for_symbol.index),
            )
        return symbols

    @staticmethod
    def _update_symbol_dict(src: dict[str, int], dest: dict[str, int]) -> None:
        for key, value in src.items():
            dest[key] = dest[key] + value if key in dest else value

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(IOError),
    )
    def _process_filename(
        self,
        cur_date: date,
        exbyte: str,
        filename: str,
    ) -> set[str]:
        logger.info(
            "Processing file %s in a thread with retry for date %s and exchange %s",
            filename,
            cur_date,
            exbyte,
        )
        symbols = set()
        try:
            for dataframe in self._reader.read_single_file(filename):
                symbols.update(self._process_chunk(cur_date, exbyte, dataframe))
        except Exception as exc:
            logger.error("Exception while processing file %s: %s", filename, exc)
        return symbols

    def write(self) -> None:
        """Write tick data to arcticdb.

        Args:
            universe: The universe which defines exchanges, object ids and dates
                of interest and maps the object ids to figi+pcs.
            reader: Reader object used to fetch raw data.
        """
        # Dictionary for the number of non-empty dataframes for symbol.
        symbols: set[str] = set()

        for cur_date, exbyte in self._universe.keys():
            filenames = self._reader.get_parquet_file_list(cur_date, exbyte)
            with ThreadPoolExecutor(max_workers=10) as executor:
                fname_to_future = {
                    fname: executor.submit(
                        self._process_filename,
                        cur_date,
                        exbyte,
                        fname,
                    )
                    for fname in filenames
                }
                for fname, future in fname_to_future.items():
                    symbols.update(future.result())

        self._finalize_write(symbols)

    def _finalize_write(self, symbols: set[str]) -> None:
        logger.info(
            "Finalizing all the symbols now, symbol_count=%d, symbols=%s",
            len(symbols),
            symbols,
        )
        failures = 0
        for symbol in symbols:
            # Default mode is APPEND so rerun the same universe will FAIL
            # because the index will be less than the existing ones
            err = ""
            try:
                self._writer.finalize_staged_data(symbol, sort=False, retry_attempts=1)
            except ArcticDBWriteError as ex:
                if "UnsortedDataException" in str(ex):
                    try:
                        self._writer.finalize_staged_data(
                            symbol, sort=True, retry_attempts=1
                        )
                    except ArcticDBWriteError as ex2:
                        err = (
                            f"Error occurred while writing symbol={symbol} to arcticdb "
                            f"using sort_and_finalize operation, err={ex2}"
                        )
                else:
                    err = (
                        f"Error occurred while writing symbol={symbol} to arcticdb "
                        f"using unsorted finalize operation, err={ex}"
                    )

            if err:
                logger.error(err)
                failures += 1
                self._writer.rollback_staged_data(symbol)

        logger.info("Total staged symbols=%d, failures=%d", len(symbols), failures)
```

I just realized that some comments may not make much sense as we have been shuffling things around and did a poor job of updating the comments.

- How big are the incomplete segments? Do we break them up properly upon write?
- Symbols count `list_symbols` vs `get_staged_symbols`

- Retry massif and see if anything dumb is still happening
- Is my testing anything like their's? Test on S3? If reading fast from storage and compute queue building up, we could
get in to this problem. Put a sleep in the CPU bound tasks to simulate??
- Write a repro that is like their's
- Try with sorting
- Simulate a crash somewhere eg `VersionedItem LocalVersionedEngine::compact_incomplete_dynamic(` and check the symbol counts
  (smaller example should be OK for this?)

BBG presumably have existing symbols?

My testing - 137MiB at peak - is that just about the size of the staged segment decompressed?? Check in Pandas
Do I have 7 columns?

223.5MiB at peak with some sleeps (up from ~180MiB and only a few smallish incompletes)

Backpressure here?

```
            // TODO We should apply back pressure to the work we are generating here, to bound memory use
            write_futures.emplace_back(store->write(pk, std::move(segment)));
```

Break up the incompletes based on row size? Rather than just writing one huge one?


With 10 incompletes each 1M rows, peak RSS 1842 (with sleeps) (Massif 45678)
Without sleeps: peak RSS 1568 (Massif 48034)

Without sleeps, With 100 incompletes each 1M rows, peak RSS 1603 (Massif 48881)


Ideas so far:
- Backpressure when accumulating the `write_futures`
- Break up the incompletes to follow the row_size parameter

Plan
- Script to see if Vasil's idea about the symbol counts is correct
  Interrupted during cleanup of the data keys. See if ranges in the data keys are in the symbols.
  Advice for BBG about how to clean them up. (/)
- Profile repeated finalizations (/) (all fine)
- Instructions to profile with memray (/)
- Chunk up the incompletes based on the library row size setting rather than writing them in the size that the user
  had them in memory
- Experiment with simulating very slow IO tasks and, separately, very slow CPU tasks to see effect on memory growth
- (tangent) In `do_compact` we are loading incomplete segments with `sk.segment()` in serial -> better to queue these loads in advance
  Downside: May increase memory use!


With repeated finalization, all looked good (Massif 73558)

```
20241218 10:37:51.707631 73558 I arcticdb | Simulating 0ms sleep writing key_seg V:staged-22:1:0x482dff849da0abc4@1734518271703672355[0,0]
20241218 10:37:51.766600 73558 I arcticdb | Simulating 0ms sleep writing key_seg r:staged-22
Step 1: Memory use after finalize 911.796MB

20241218 10:48:08.626907 73558 I arcticdb | Simulating 0ms sleep writing key_seg r:staged-11
Step 29: Memory use after finalize 951.684MB
Step 30: Finalizing symbol staged-3
```

1M rows per incomplete


- Debug build works well with Memray, release build does not
- Debug build CI run - https://github.com/man-group/ArcticDB/actions/runs/12391751526 - test the wheel it makes with memray

- Diagnostic debug logging - work on this - new logger? Binary with the allocations logger?

arcticdb_ext.so is 49M without symbols

- Perhaps they have very large existing symbols?
  Test with existing >>> appending
  Do I have logging for this case?


## Debugging steps

From a venv with the debug ArcticDB wheel installed and also memray,

```
pip install memray
```

Then launch a repro script showing growing memory use:

`memray run --native your_script.py`

Then share the result file with me.

memray slows things down - you don't need to run until you OOM crash. You just need an example where you have a process
finalizing staged data where you can observe growing memory use.

Also set up ArcticDB debug logging by putting,

```
from arcticdb.config import set_log_level
set_log_level(specific_log_levels={"version": "DEBUG"})
```

at the top of your repro script. This will log debug info to stderr. Share that with me too.

## For screen share

How big is each incomplete?

```
from arcticdb_ext.storage import KeyType
lt = lib._dev_tools.library_tool()
aks = lt.find_keys(KeyType.APPEND_DATA)
k = aks[0]

print(lt.read_to_dataframe(k).shape)
print(lt.read_to_dataframe(k).dtypes)
```

## Summary

- How big are the incompletes?
- Do the symbols already exist? How big?
- Finalize staged data or sort and finalize?
- I'll prepare a debug build with extra logging
  - Do you have a repro with growing memory use that doesn't OOM or that executes a bit faster?

### Experiments

BBG will have at least 300 chunks per symbol? One chunk per day?
We know that they have about 4000 symbols
How many rows for each chunk?
TODO run this experiment next


## Small chunks, small number of chunks, 2000 symbols

massif 274809

```
num_rows_initially = int(1e5)
num_chunks = 5
num_rows_per_chunk = int(1e3)
num_symbols = 2000
cachedDF = CachedDFGenerator(num_rows_initially + 1, size_string_flds_array=[10])
```

Result: Nothing surprising

## No existing data, 200 1M row chunks for each of 7 symbols

```
num_rows_initially = int(1e6) ## not used
num_chunks = 200
num_rows_per_chunk = int(1e6)
num_symbols = 7
cachedDF = CachedDFGenerator(num_rows_initially + 1, size_string_flds_array=[10])
```

massif 384206

Also try memray? Massif seems to under-report a bit?

## No existing data, 200 1M row chunks for 1 symbol

```
num_rows_initially = int(1e7)
num_chunks = 200
num_rows_per_chunk = int(1e6)
num_symbols = 1
cachedDF = CachedDFGenerator(num_rows_initially + 1, size_string_flds_array=[10])
```

massif 403471
memray 

348.2MiB

## No existing data, 20 1M row chunks for 1 symbol

```
num_rows_initially = int(1e7)
num_chunks = 20
num_rows_per_chunk = int(1e6)
num_symbols = 1
cachedDF = CachedDFGenerator(num_rows_initially + 1, size_string_flds_array=[10])
```

347.1MiB

It's completely a function of the size of the chunk...

I think same memory use as the experiment above shows this is OK?

## Trial fixes

- Release segment properly
  version_core.hpp:309
  Try just releasing it and see what happens (unlikely to help but try it...)

```
num_rows_initially = int(1e7)
num_chunks = 10
num_rows_per_chunk = int(1e6)
num_symbols = 1
cachedDF = CachedDFGenerator(num_rows_initially + 1, size_string_flds_array=[10])
```

- Chunk up the incompletes when they are written
- Finish my KeySegmentPair refactor?

Breaking up...
`write_incomplete_frame`
- The chaining with `next_key` for the tick collectors - separate code path?
- The weird sorting stuff, ignore that for now?
  I don't understand why we have the sorting stuff at all. Do in Pandas?
