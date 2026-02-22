/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arcticdb::{ArcticLibrary, ColumnData, DataFrame};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "arcticdb-gateway", about = "HTTP gateway for ArcticDB Excel integration")]
struct Cli {
    /// Port to listen on
    #[arg(long, default_value = "8787", env = "ARCTICDB_GATEWAY_PORT")]
    port: u16,
}

// ── Application state ────────────────────────────────────────────────────────

struct AppState {
    libraries: Mutex<HashMap<String, ArcticLibrary>>,
}

type SharedState = Arc<AppState>;

// ── Request / Response types ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct OpenLibraryRequest {
    name: String,
    path: String,
}

#[derive(Deserialize)]
struct WriteTestRequest {
    symbol: String,
    rows: i64,
    cols: i64,
}

#[derive(Deserialize)]
struct ReadQuery {
    version: Option<i64>,
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// Row-oriented DataFrame for the wire format expected by Excel.
#[derive(Serialize)]
struct DataFrameResponse {
    column_names: Vec<String>,
    column_types: Vec<String>,
    data: Vec<Vec<serde_json::Value>>,
    num_rows: i64,
}

impl From<DataFrame> for DataFrameResponse {
    fn from(df: DataFrame) -> Self {
        let num_rows = df.num_rows as usize;
        let n_cols = df.columns.len();

        // Convert column-oriented data to row-oriented
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(n_cols);
            for col in &df.columns {
                let val = match col {
                    ColumnData::Float64(v) => {
                        serde_json::Value::from(v.get(row_idx).copied().unwrap_or(f64::NAN))
                    }
                    ColumnData::Int64(v) => {
                        serde_json::Value::from(v.get(row_idx).copied().unwrap_or(0))
                    }
                };
                row.push(val);
            }
            rows.push(row);
        }

        DataFrameResponse {
            column_names: df.column_names,
            column_types: df.column_types,
            data: rows,
            num_rows: df.num_rows,
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn ok_json() -> Json<OkResponse> {
    Json(OkResponse { ok: true })
}

fn err_response(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: msg.into(),
        }),
    )
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn health() -> Json<OkResponse> {
    ok_json()
}

async fn open_library(
    State(state): State<SharedState>,
    Json(req): Json<OpenLibraryRequest>,
) -> impl IntoResponse {
    let lib = match ArcticLibrary::open_lmdb(&req.path) {
        Ok(lib) => lib,
        Err(e) => return err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    state.libraries.lock().unwrap().insert(req.name, lib);
    ok_json().into_response()
}

async fn close_library(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let removed = state.libraries.lock().unwrap().remove(&name);
    if removed.is_some() {
        ok_json().into_response()
    } else {
        err_response(StatusCode::NOT_FOUND, format!("library '{name}' not found")).into_response()
    }
}

async fn list_symbols(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let libs = state.libraries.lock().unwrap();
    let lib = match libs.get(&name) {
        Some(lib) => lib,
        None => return err_response(StatusCode::NOT_FOUND, format!("library '{name}' not found")).into_response(),
    };
    match lib.list_symbols() {
        Ok(symbols) => Json(symbols).into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn read_symbol(
    State(state): State<SharedState>,
    Path((name, symbol)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> impl IntoResponse {
    let libs = state.libraries.lock().unwrap();
    let lib = match libs.get(&name) {
        Some(lib) => lib,
        None => return err_response(StatusCode::NOT_FOUND, format!("library '{name}' not found")).into_response(),
    };
    let version = query.version.unwrap_or(-1);
    match lib.read_dataframe(&symbol, version) {
        Ok(df) => Json(DataFrameResponse::from(df)).into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn write_test(
    State(state): State<SharedState>,
    Path(name): Path<String>,
    Json(req): Json<WriteTestRequest>,
) -> impl IntoResponse {
    let libs = state.libraries.lock().unwrap();
    let lib = match libs.get(&name) {
        Some(lib) => lib,
        None => return err_response(StatusCode::NOT_FOUND, format!("library '{name}' not found")).into_response(),
    };
    match lib.write_test_data(&req.symbol, req.rows, req.cols) {
        Ok(()) => ok_json().into_response(),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let state: SharedState = Arc::new(AppState {
        libraries: Mutex::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/libraries", post(open_library))
        .route("/api/libraries/:name", delete(close_library))
        .route("/api/libraries/:name/symbols", get(list_symbols))
        .route("/api/libraries/:name/read/:symbol", get(read_symbol))
        .route("/api/libraries/:name/write-test", post(write_test))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cli.port);
    println!("ArcticDB gateway listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
