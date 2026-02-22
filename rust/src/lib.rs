/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

//! Rust bindings for the ArcticDB C API.
//!
//! Provides safe wrappers around `libarcticdb_c.so` for opening LMDB-backed libraries,
//! writing test data, reading via Arrow C Stream Interface, and listing symbols.
//!
//! # Example
//!
//! ```no_run
//! use arcticdb::ArcticLibrary;
//!
//! let lib = ArcticLibrary::open_lmdb("/tmp/test_db").unwrap();
//! lib.write_test_data("prices", 1000, 5).unwrap();
//! let result = lib.read_stream("prices").unwrap();
//! println!("Read {} rows in {} batches", result.total_rows, result.batch_count);
//! ```

use serde::Serialize;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fmt;
use std::ptr;

// ── FFI types ────────────────────────────────────────────────────────────────

/// Opaque handle to an ArcticDB library (C side).
#[repr(C)]
pub struct ArcticLibraryHandle {
    _opaque: [u8; 0],
}

/// ArcticError: `{ int code; char message[512]; }` — 516 bytes.
#[repr(C)]
pub struct ArcticError {
    pub code: c_int,
    pub message: [u8; 512],
}

impl ArcticError {
    fn new() -> Self {
        Self {
            code: 0,
            message: [0u8; 512],
        }
    }

    fn get_message(&self) -> &str {
        let nul_pos = self.message.iter().position(|&b| b == 0).unwrap_or(512);
        std::str::from_utf8(&self.message[..nul_pos]).unwrap_or("<invalid utf8>")
    }
}

/// Arrow C Stream Interface: 4 function pointers + private_data (40 bytes on x86_64).
#[repr(C)]
pub struct ArcticArrowArrayStream {
    pub get_schema:
        Option<unsafe extern "C" fn(*mut ArcticArrowArrayStream, *mut ArrowSchema) -> c_int>,
    pub get_next:
        Option<unsafe extern "C" fn(*mut ArcticArrowArrayStream, *mut ArrowArray) -> c_int>,
    pub get_last_error:
        Option<unsafe extern "C" fn(*mut ArcticArrowArrayStream) -> *const c_char>,
    pub release: Option<unsafe extern "C" fn(*mut ArcticArrowArrayStream)>,
    pub private_data: *mut c_void,
}

/// ArrowSchema (72 bytes on x86_64).
#[repr(C)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

/// ArrowArray (80 bytes on x86_64).
#[repr(C)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

// ── Extern C bindings ────────────────────────────────────────────────────────

extern "C" {
    fn arctic_library_open_lmdb(
        path: *const c_char,
        out: *mut *mut ArcticLibraryHandle,
        err: *mut ArcticError,
    ) -> c_int;

    fn arctic_library_close(lib: *mut ArcticLibraryHandle);

    fn arctic_write_test_data(
        lib: *mut ArcticLibraryHandle,
        symbol: *const c_char,
        num_rows: i64,
        num_columns: i64,
        err: *mut ArcticError,
    ) -> c_int;

    fn arctic_read_stream(
        lib: *mut ArcticLibraryHandle,
        symbol: *const c_char,
        version: i64,
        out: *mut ArcticArrowArrayStream,
        err: *mut ArcticError,
    ) -> c_int;

    fn arctic_list_symbols(
        lib: *mut ArcticLibraryHandle,
        out_symbols: *mut *mut *mut c_char,
        out_count: *mut i64,
        err: *mut ArcticError,
    ) -> c_int;

    fn arctic_free_symbols(symbols: *mut *mut c_char, count: i64);
}

// ── Error type ───────────────────────────────────────────────────────────────

/// Error returned by ArcticDB operations.
#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArcticDB error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

fn check_error(rc: c_int, err: &ArcticError) -> Result<()> {
    if rc != 0 {
        Err(Error {
            code: err.code,
            message: err.get_message().to_string(),
        })
    } else {
        Ok(())
    }
}

// ── High-level wrapper ───────────────────────────────────────────────────────

/// Column data extracted from Arrow arrays.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ColumnData {
    Float64(Vec<f64>),
    Int64(Vec<i64>),
}

/// A DataFrame read from ArcticDB with column-oriented data.
#[derive(Debug, Clone, Serialize)]
pub struct DataFrame {
    pub column_names: Vec<String>,
    pub column_types: Vec<String>,
    pub columns: Vec<ColumnData>,
    pub num_rows: i64,
}

/// Summary of data read from an Arrow stream.
#[derive(Debug)]
pub struct ReadResult {
    /// Column names from the Arrow schema (includes index and data columns).
    pub column_names: Vec<String>,
    /// Total number of rows across all batches.
    pub total_rows: i64,
    /// Number of Arrow record batches consumed.
    pub batch_count: i32,
}

/// Safe wrapper around an ArcticDB library handle.
///
/// Implements [`Drop`] for deterministic cleanup.
pub struct ArcticLibrary {
    handle: *mut ArcticLibraryHandle,
}

// The C API is thread-safe; the handle can be sent across threads.
unsafe impl Send for ArcticLibrary {}

impl ArcticLibrary {
    /// Open an LMDB-backed ArcticDB library at the given path.
    ///
    /// Creates the directory if it does not exist.
    pub fn open_lmdb(path: &str) -> Result<Self> {
        let c_path = CString::new(path).expect("path contains null byte");
        let mut handle: *mut ArcticLibraryHandle = ptr::null_mut();
        let mut err = ArcticError::new();

        let rc = unsafe { arctic_library_open_lmdb(c_path.as_ptr(), &mut handle, &mut err) };
        check_error(rc, &err)?;

        Ok(Self { handle })
    }

    /// Write synthetic test data: a timeseries-indexed DataFrame with float64 columns
    /// named `col_0` .. `col_{num_columns-1}`.
    pub fn write_test_data(
        &self,
        symbol: &str,
        num_rows: i64,
        num_columns: i64,
    ) -> Result<()> {
        let c_symbol = CString::new(symbol).expect("symbol contains null byte");
        let mut err = ArcticError::new();

        let rc = unsafe {
            arctic_write_test_data(self.handle, c_symbol.as_ptr(), num_rows, num_columns, &mut err)
        };
        check_error(rc, &err)
    }

    /// Read the latest version of a symbol as a streaming Arrow result.
    pub fn read_stream(&self, symbol: &str) -> Result<ReadResult> {
        self.read_stream_version(symbol, -1)
    }

    /// Read a specific version of a symbol (`-1` for latest).
    pub fn read_stream_version(&self, symbol: &str, version: i64) -> Result<ReadResult> {
        let c_symbol = CString::new(symbol).expect("symbol contains null byte");
        let mut err = ArcticError::new();
        let mut stream = unsafe { std::mem::zeroed::<ArcticArrowArrayStream>() };

        let rc = unsafe {
            arctic_read_stream(self.handle, c_symbol.as_ptr(), version, &mut stream, &mut err)
        };
        check_error(rc, &err)?;

        // 1. Get schema
        let mut schema = unsafe { std::mem::zeroed::<ArrowSchema>() };
        let get_schema = stream.get_schema.expect("get_schema is null");
        let schema_rc = unsafe { get_schema(&mut stream, &mut schema) };
        if schema_rc != 0 {
            if let Some(release) = stream.release {
                unsafe { release(&mut stream) };
            }
            return Err(Error {
                code: schema_rc,
                message: "get_schema failed".into(),
            });
        }

        // Read column names from schema children
        let mut column_names = Vec::new();
        if schema.n_children > 0 && !schema.children.is_null() {
            for i in 0..schema.n_children {
                let child_ptr = unsafe { *schema.children.add(i as usize) };
                if !child_ptr.is_null() {
                    let child = unsafe { &*child_ptr };
                    if !child.name.is_null() {
                        let name = unsafe { CStr::from_ptr(child.name) }
                            .to_string_lossy()
                            .into_owned();
                        column_names.push(name);
                    }
                }
            }
        }

        // Release schema
        if let Some(release) = schema.release {
            unsafe { release(&mut schema) };
        }

        // 2. Consume batches
        let get_next = stream.get_next.expect("get_next is null");
        let mut total_rows: i64 = 0;
        let mut batch_count: i32 = 0;

        loop {
            let mut array = unsafe { std::mem::zeroed::<ArrowArray>() };
            let next_rc = unsafe { get_next(&mut stream, &mut array) };
            if next_rc != 0 {
                if let Some(release) = stream.release {
                    unsafe { release(&mut stream) };
                }
                return Err(Error {
                    code: next_rc,
                    message: "get_next failed".into(),
                });
            }

            // release == None means end of stream
            if array.release.is_none() {
                break;
            }

            total_rows += array.length;
            batch_count += 1;

            // Release this array batch
            if let Some(release) = array.release {
                unsafe { release(&mut array) };
            }
        }

        // 3. Release stream
        if let Some(release) = stream.release {
            unsafe { release(&mut stream) };
        }

        Ok(ReadResult {
            column_names,
            total_rows,
            batch_count,
        })
    }

    /// Read a symbol as a DataFrame, returning actual column data.
    pub fn read_dataframe(&self, symbol: &str, version: i64) -> Result<DataFrame> {
        let c_symbol = CString::new(symbol).expect("symbol contains null byte");
        let mut err = ArcticError::new();
        let mut stream = unsafe { std::mem::zeroed::<ArcticArrowArrayStream>() };

        let rc = unsafe {
            arctic_read_stream(self.handle, c_symbol.as_ptr(), version, &mut stream, &mut err)
        };
        check_error(rc, &err)?;

        // 1. Get schema — extract column names and formats
        let mut schema = unsafe { std::mem::zeroed::<ArrowSchema>() };
        let get_schema = stream.get_schema.expect("get_schema is null");
        let schema_rc = unsafe { get_schema(&mut stream, &mut schema) };
        if schema_rc != 0 {
            if let Some(release) = stream.release {
                unsafe { release(&mut stream) };
            }
            return Err(Error {
                code: schema_rc,
                message: "get_schema failed".into(),
            });
        }

        let mut column_names = Vec::new();
        let mut column_formats = Vec::new();
        if schema.n_children > 0 && !schema.children.is_null() {
            for i in 0..schema.n_children {
                let child_ptr = unsafe { *schema.children.add(i as usize) };
                if !child_ptr.is_null() {
                    let child = unsafe { &*child_ptr };
                    let name = if !child.name.is_null() {
                        unsafe { CStr::from_ptr(child.name) }
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        format!("col_{i}")
                    };
                    let fmt = if !child.format.is_null() {
                        unsafe { CStr::from_ptr(child.format) }
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        String::new()
                    };
                    column_names.push(name);
                    column_formats.push(fmt);
                }
            }
        }

        if let Some(release) = schema.release {
            unsafe { release(&mut schema) };
        }

        let n_cols = column_names.len();

        // Map Arrow format strings to type names
        let column_types: Vec<String> = column_formats
            .iter()
            .map(|fmt| match fmt.as_str() {
                "g" => "float64".into(),
                "f" => "float32".into(),
                "l" => "int64".into(),
                "i" => "int32".into(),
                "ttn" => "int64".into(), // timestamp ns → int64
                "tsn:" => "int64".into(), // timestamp ns with tz → int64
                other if other.starts_with("tsn:") => "int64".into(),
                _ => "float64".into(), // fallback
            })
            .collect();

        // Prepare column accumulators
        let mut columns: Vec<Vec<f64>> = vec![Vec::new(); n_cols];
        let mut int_columns: Vec<Vec<i64>> = vec![Vec::new(); n_cols];
        let mut total_rows: i64 = 0;

        // 2. Consume batches — copy data from Arrow arrays
        let get_next = stream.get_next.expect("get_next is null");

        loop {
            let mut array = unsafe { std::mem::zeroed::<ArrowArray>() };
            let next_rc = unsafe { get_next(&mut stream, &mut array) };
            if next_rc != 0 {
                if let Some(release) = stream.release {
                    unsafe { release(&mut stream) };
                }
                return Err(Error {
                    code: next_rc,
                    message: "get_next failed".into(),
                });
            }

            if array.release.is_none() {
                break;
            }

            let batch_len = array.length as usize;
            total_rows += array.length;

            if array.n_children as usize == n_cols && !array.children.is_null() {
                for col_idx in 0..n_cols {
                    let child_ptr = unsafe { *array.children.add(col_idx) };
                    if child_ptr.is_null() {
                        continue;
                    }
                    let child = unsafe { &*child_ptr };
                    // buffers[1] is the data buffer in Arrow columnar format
                    if child.n_buffers >= 2 && !child.buffers.is_null() {
                        let data_buf = unsafe { *child.buffers.add(1) };
                        if !data_buf.is_null() {
                            match column_types[col_idx].as_str() {
                                "float64" => {
                                    let slice = unsafe {
                                        std::slice::from_raw_parts(
                                            data_buf as *const f64,
                                            batch_len,
                                        )
                                    };
                                    columns[col_idx].extend_from_slice(slice);
                                }
                                "float32" => {
                                    let slice = unsafe {
                                        std::slice::from_raw_parts(
                                            data_buf as *const f32,
                                            batch_len,
                                        )
                                    };
                                    columns[col_idx].extend(slice.iter().map(|&v| v as f64));
                                }
                                "int64" => {
                                    let slice = unsafe {
                                        std::slice::from_raw_parts(
                                            data_buf as *const i64,
                                            batch_len,
                                        )
                                    };
                                    int_columns[col_idx].extend_from_slice(slice);
                                }
                                "int32" => {
                                    let slice = unsafe {
                                        std::slice::from_raw_parts(
                                            data_buf as *const i32,
                                            batch_len,
                                        )
                                    };
                                    int_columns[col_idx]
                                        .extend(slice.iter().map(|&v| v as i64));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            if let Some(release) = array.release {
                unsafe { release(&mut array) };
            }
        }

        // 3. Release stream
        if let Some(release) = stream.release {
            unsafe { release(&mut stream) };
        }

        // Build final ColumnData from the accumulators
        let final_columns: Vec<ColumnData> = (0..n_cols)
            .map(|i| {
                if column_types[i] == "int64" || column_types[i] == "int32" {
                    ColumnData::Int64(std::mem::take(&mut int_columns[i]))
                } else {
                    ColumnData::Float64(std::mem::take(&mut columns[i]))
                }
            })
            .collect();

        Ok(DataFrame {
            column_names,
            column_types,
            columns: final_columns,
            num_rows: total_rows,
        })
    }

    /// List all symbols in this library.
    pub fn list_symbols(&self) -> Result<Vec<String>> {
        let mut err = ArcticError::new();
        let mut symbols_ptr: *mut *mut c_char = ptr::null_mut();
        let mut count: i64 = 0;

        let rc = unsafe {
            arctic_list_symbols(self.handle, &mut symbols_ptr, &mut count, &mut err)
        };
        check_error(rc, &err)?;

        let mut result = Vec::new();
        if count > 0 && !symbols_ptr.is_null() {
            for i in 0..count {
                let str_ptr = unsafe { *symbols_ptr.add(i as usize) };
                if !str_ptr.is_null() {
                    let s = unsafe { CStr::from_ptr(str_ptr) }
                        .to_string_lossy()
                        .into_owned();
                    result.push(s);
                }
            }
            unsafe { arctic_free_symbols(symbols_ptr, count) };
        }

        Ok(result)
    }
}

impl Drop for ArcticLibrary {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { arctic_library_close(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}
