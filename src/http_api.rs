// =============================================================
// HTTP API (#11) — axum REST wrapper around SharedLsmEngine
//
// Endpoints
// ---------
//   GET    /key/:key            → 200 + JSON body / 404
//   PUT    /key/:key            → 200 (body = value bytes)
//   DELETE /key/:key            → 200
//   GET    /scan?from=&to=      → 200 + JSON array of {key,value}
//   GET    /prefix/:prefix      → 200 + JSON array of {key,value}
//   GET    /snapshot            → 200 + JSON {seq, entries:[{key,value}]}
//   GET    /stats               → 200 + JSON stats
//
// Column family is always "default".  Extend via query param ?cf=name
// if multi-CF support is needed.
//
// Keys and values are transmitted as UTF-8 strings.  Binary keys are
// not supported via this HTTP layer (use the Rust API directly).
// =============================================================

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Json},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use crate::metrics::Metrics;
use crate::SharedLsmEngine;

// ---- shared state ----------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    db:      SharedLsmEngine,
    metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(db: SharedLsmEngine) -> Self {
        Self { db, metrics: Metrics::new() }
    }

    pub fn with_metrics(db: SharedLsmEngine, metrics: Arc<Metrics>) -> Self {
        Self { db, metrics }
    }
}

// ---- wire types ------------------------------------------------------------

#[derive(Serialize)]
struct KvPair {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct SnapshotResponse {
    seq: u64,
    entries: Vec<KvPair>,
}

#[derive(Deserialize)]
struct CfParam {
    cf: Option<String>,
}

// ---- handlers --------------------------------------------------------------

async fn get_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<CfParam>,
) -> impl IntoResponse {
    let cf = params.cf.as_deref().unwrap_or("default");
    match state.db.get_cf(cf, key.as_bytes()) {
        Ok(Some(val)) => {
            let s = String::from_utf8_lossy(&val).into_owned();
            (StatusCode::OK, Json(serde_json::json!({ "key": key, "value": s }))).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "not found" }))).into_response(),
        Err(e)  => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn put_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<CfParam>,
    body: String,
) -> impl IntoResponse {
    let cf = params.cf.as_deref().unwrap_or("default");
    match state.db.put_cf(cf, key.as_bytes().to_vec(), body.into_bytes()) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn delete_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<CfParam>,
) -> impl IntoResponse {
    let cf = params.cf.as_deref().unwrap_or("default");
    match state.db.delete_cf(cf, key.as_bytes().to_vec()) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn scan_range(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let from = params.get("from").cloned().unwrap_or_default();
    let to   = params.get("to").cloned().unwrap_or_default();
    let cf   = params.get("cf").map(|s| s.as_str()).unwrap_or("default").to_owned();
    match state.db.scan_cf(&cf, from.as_bytes(), to.as_bytes()) {
        Ok(pairs) => {
            let body: Vec<KvPair> = pairs.into_iter().map(|(k, v)| KvPair {
                key:   String::from_utf8_lossy(&k).into_owned(),
                value: String::from_utf8_lossy(&v).into_owned(),
            }).collect();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn scan_prefix_handler(
    State(state): State<AppState>,
    Path(prefix): Path<String>,
    Query(params): Query<CfParam>,
) -> impl IntoResponse {
    let cf = params.cf.as_deref().unwrap_or("default");
    match state.db.scan_prefix_cf(cf, prefix.as_bytes()) {
        Ok(pairs) => {
            let body: Vec<KvPair> = pairs.into_iter().map(|(k, v)| KvPair {
                key:   String::from_utf8_lossy(&k).into_owned(),
                value: String::from_utf8_lossy(&v).into_owned(),
            }).collect();
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn snapshot_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.snapshot() {
        Ok(snap) => {
            let entries: Vec<KvPair> = snap.iter().map(|(k, v)| KvPair {
                key:   String::from_utf8_lossy(k).into_owned(),
                value: String::from_utf8_lossy(v).into_owned(),
            }).collect();
            (StatusCode::OK, Json(SnapshotResponse { seq: snap.seq(), entries })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.db.stats();
    Json(serde_json::json!({
        "memtable_bytes":   s.mem_table_size_bytes,
        "sstable_count":    s.total_ss_table_files,
        "level_counts":     s.level_file_counts,
        "immutable_count":  s.immutable_count,
        "column_families":  s.column_families,
    }))
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics.prometheus();
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static("text/plain; version=0.0.4"));
    (StatusCode::OK, headers, body)
}

// ---- router factory --------------------------------------------------------

/// Build and return the axum Router.  Caller binds it to a TcpListener.
///
/// ```no_run
/// let db  = SharedLsmEngine::open("/tmp/mydb").unwrap();
/// let app = lsmdb::http_api::make_router(db);
/// let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
/// axum::serve(listener, app).await.unwrap();
/// ```
pub fn make_router(db: SharedLsmEngine) -> Router {
    let state = AppState::new(db);
    Router::new()
        .route("/key/{key}", get(get_key)
            .put(put_key)
            .delete(delete_key))
        .route("/scan",         get(scan_range))
        .route("/prefix/{prefix}", get(scan_prefix_handler))
        .route("/snapshot",     get(snapshot_handler))
        .route("/stats",        get(stats_handler))
        .route("/metrics",      get(metrics_handler))
        .with_state(state)
}
