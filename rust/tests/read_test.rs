/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

use arcticdb::ArcticLibrary;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "arcticdb_rust_test_{}_{}",
        std::process::id(),
        id
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_open_close() {
    let dir = temp_dir();
    let db_path = dir.join("db1");
    {
        let lib = ArcticLibrary::open_lmdb(db_path.to_str().unwrap()).unwrap();
        drop(lib);
    }
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_write_and_list_symbols() {
    let dir = temp_dir();
    let db_path = dir.join("db2");
    {
        let lib = ArcticLibrary::open_lmdb(db_path.to_str().unwrap()).unwrap();
        lib.write_test_data("sym_a", 10, 2).unwrap();
        lib.write_test_data("sym_b", 20, 3).unwrap();

        let symbols = lib.list_symbols().unwrap();
        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains(&"sym_a".to_string()));
        assert!(symbols.contains(&"sym_b".to_string()));
    }
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_read_stream() {
    let dir = temp_dir();
    let db_path = dir.join("db3");
    {
        let lib = ArcticLibrary::open_lmdb(db_path.to_str().unwrap()).unwrap();
        lib.write_test_data("prices", 100, 3).unwrap();

        let result = lib.read_stream("prices").unwrap();

        assert_eq!(result.total_rows, 100);
        assert!(result.batch_count >= 1);
        // The schema includes the timestamp index + 3 data columns
        assert!(
            result.column_names.iter().any(|n| n.contains("col_0")),
            "Expected col_0 in {:?}",
            result.column_names
        );
        assert!(
            result.column_names.iter().any(|n| n.contains("col_1")),
            "Expected col_1 in {:?}",
            result.column_names
        );
        assert!(
            result.column_names.iter().any(|n| n.contains("col_2")),
            "Expected col_2 in {:?}",
            result.column_names
        );
    }
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_read_specific_version() {
    let dir = temp_dir();
    let db_path = dir.join("db4");
    {
        let lib = ArcticLibrary::open_lmdb(db_path.to_str().unwrap()).unwrap();
        lib.write_test_data("versioned", 50, 2).unwrap(); // version 0
        lib.write_test_data("versioned", 75, 2).unwrap(); // version 1

        let v0 = lib.read_stream_version("versioned", 0).unwrap();
        assert_eq!(v0.total_rows, 50);

        let v1 = lib.read_stream_version("versioned", 1).unwrap();
        assert_eq!(v1.total_rows, 75);

        // Latest should be v1
        let latest = lib.read_stream("versioned").unwrap();
        assert_eq!(latest.total_rows, 75);
    }
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_read_missing_symbol_errors() {
    let dir = temp_dir();
    let db_path = dir.join("db5");
    {
        let lib = ArcticLibrary::open_lmdb(db_path.to_str().unwrap()).unwrap();
        let result = lib.read_stream("nonexistent");
        assert!(result.is_err(), "Expected error for missing symbol");
    }
    fs::remove_dir_all(&dir).ok();
}
