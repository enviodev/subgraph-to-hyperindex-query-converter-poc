use axum::{
    extract::{Json, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use dotenv;
// use reqwest; // avoid bringing reqwest::StatusCode into scope
use serde_json::Value;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing;
use tracing_subscriber;

mod conversion;
#[cfg(test)]
mod integration_tests;

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    tracing_subscriber::fmt::init();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([axum::http::Method::POST, axum::http::Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        .route("/", post(handle_query))
        .route("/debug", post(handle_debug))
        .route("/chainId/:chain_id", post(handle_chain_query))
        .route("/chainId/:chain_id/debug", post(handle_chain_debug))
        .layer(cors);

    let addr: SocketAddr = "0.0.0.0:3000".parse().unwrap();
    tracing::info!("listening on {}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handle_query(Json(payload): Json<Value>) -> impl IntoResponse {
    tracing::info!("Received query: {:?}", payload);

    match conversion::convert_subgraph_to_hyperindex(&payload, None) {
        Ok(converted_query) => {
            tracing::info!("Converted query: {:?}", converted_query);

            // Forward the converted query to Hyperindex
            match forward_to_hyperindex(&converted_query).await {
                Ok(response) => {
                    tracing::info!("Hyperindex response: {:?}", response);
                    // If upstream returned GraphQL errors, surface them with debug info
                    if response.get("errors").is_some() {
                        let hyperindex_url =
                            std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");
                        let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
                        // Log both original and converted queries for debugging
                        let original_query = payload
                            .get("query")
                            .and_then(|q| q.as_str())
                            .unwrap_or_default();
                        let converted_query_str = converted_query
                            .get("query")
                            .and_then(|q| q.as_str())
                            .unwrap_or_default();
                        tracing::error!(
                            original_query = original_query,
                            converted_query = converted_query_str,
                            "Upstream GraphQL returned errors for converted query"
                        );
                        let debug = serde_json::json!({
                            "originalQuery": original_query,
                            "convertedQuery": converted_query_str,
                            "hyperindexUrl": hyperindex_url,
                        });
                        return (
                            StatusCode::BAD_GATEWAY,
                            Json(serde_json::json!({
                                "errors": response.get("errors").cloned().unwrap_or_default(),
                                "debug": debug,
                                "subgraphResponse": subgraph_debug,
                            })),
                        );
                    }

                    let transformed = transform_response_to_subgraph_shape(response);
                    (StatusCode::OK, Json(transformed))
                }
                Err(e) => {
                    tracing::error!("Hyperindex request error: {}", e);
                    let hyperindex_url =
                        std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");
                    let details = e.to_string();
                    let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
                    // Log both original and converted queries for debugging
                    let original_query = payload
                        .get("query")
                        .and_then(|q| q.as_str())
                        .unwrap_or_default();
                    let converted_query_str = converted_query
                        .get("query")
                        .and_then(|q| q.as_str())
                        .unwrap_or_default();
                    tracing::error!(
                        original_query = original_query,
                        converted_query = converted_query_str,
                        error = %details,
                        "Error forwarding converted query to Hyperindex"
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": "Hyperindex request failed",
                            "details": details,
                            "debug": {
                                "originalQuery": original_query,
                                "convertedQuery": converted_query_str,
                                "hyperindexUrl": hyperindex_url,
                            },
                            "subgraphResponse": subgraph_debug,
                        })),
                    )
                }
            }
        }
        Err(e) => {
            tracing::error!("Conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": serde_json::Value::Null,
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            )
        }
    }
}

async fn handle_chain_query(
    Path(chain_id): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    tracing::info!(
        "Received chain query for chain_id: {}, payload: {:?}",
        chain_id,
        payload
    );

    match conversion::convert_subgraph_to_hyperindex(&payload, Some(&chain_id)) {
        Ok(converted_query) => {
            tracing::info!("Converted chain query: {:?}", converted_query);

            // Forward the converted query to Hyperindex
            match forward_to_hyperindex(&converted_query).await {
                Ok(response) => {
                    tracing::info!("Hyperindex response: {:?}", response);
                    if response.get("errors").is_some() {
                        let hyperindex_url =
                            std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");
                        let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
                        // Log both original and converted queries for debugging
                        let original_query = payload
                            .get("query")
                            .and_then(|q| q.as_str())
                            .unwrap_or_default();
                        let converted_query_str = converted_query
                            .get("query")
                            .and_then(|q| q.as_str())
                            .unwrap_or_default();
                        tracing::error!(
                            original_query = original_query,
                            converted_query = converted_query_str,
                            chain_id = %chain_id,
                            "Upstream GraphQL returned errors for converted chain query"
                        );
                        let debug = serde_json::json!({
                            "originalQuery": original_query,
                            "convertedQuery": converted_query_str,
                            "hyperindexUrl": hyperindex_url,
                            "chainId": chain_id,
                        });
                        return (
                            StatusCode::BAD_GATEWAY,
                            Json(serde_json::json!({
                                "errors": response.get("errors").cloned().unwrap_or_default(),
                                "debug": debug,
                                "subgraphResponse": subgraph_debug,
                            })),
                        );
                    }

                    let transformed = transform_response_to_subgraph_shape(response);
                    (StatusCode::OK, Json(transformed))
                }
                Err(e) => {
                    tracing::error!("Hyperindex request error: {}", e);
                    let hyperindex_url =
                        std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");
                    let details = e.to_string();
                    let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
                    // Log both original and converted queries for debugging
                    let original_query = payload
                        .get("query")
                        .and_then(|q| q.as_str())
                        .unwrap_or_default();
                    let converted_query_str = converted_query
                        .get("query")
                        .and_then(|q| q.as_str())
                        .unwrap_or_default();
                    tracing::error!(
                        original_query = original_query,
                        converted_query = converted_query_str,
                        chain_id = %chain_id,
                        error = %details,
                        "Error forwarding converted chain query to Hyperindex"
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": "Hyperindex request failed",
                            "details": details,
                            "debug": {
                                "originalQuery": original_query,
                                "convertedQuery": converted_query_str,
                                "hyperindexUrl": hyperindex_url,
                                "chainId": chain_id,
                            },
                            "subgraphResponse": subgraph_debug,
                        })),
                    )
                }
            }
        }
        Err(e) => {
            tracing::error!("Conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": chain_id,
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            )
        }
    }
}

async fn handle_debug(Json(payload): Json<Value>) -> impl IntoResponse {
    tracing::info!("Received debug query: {:?}", payload);

    match conversion::convert_subgraph_to_hyperindex(&payload, None) {
        Ok(converted_query) => {
            tracing::info!("Converted debug query: {:?}", converted_query);
            (StatusCode::OK, Json(converted_query))
        }
        Err(e) => {
            tracing::error!("Debug conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": serde_json::Value::Null,
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            )
        }
    }
}

async fn handle_chain_debug(
    Path(chain_id): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    tracing::info!(
        "Received chain debug for chain_id: {}, payload: {:?}",
        chain_id,
        payload
    );

    match conversion::convert_subgraph_to_hyperindex(&payload, Some(&chain_id)) {
        Ok(converted_query) => {
            tracing::info!("Converted chain debug query: {:?}", converted_query);
            (StatusCode::OK, Json(converted_query))
        }
        Err(e) => {
            tracing::error!("Chain debug conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": chain_id,
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            )
        }
    }
}

async fn forward_to_hyperindex(
    query: &Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let hyperindex_url = std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");

    let client = reqwest::Client::new();
    let response = client
        .post(&hyperindex_url)
        .header("Content-Type", "application/json")
        .json(query)
        .send()
        .await?;

    let response_json: Value = response.json().await?;
    Ok(response_json)
}

fn transform_response_to_subgraph_shape(resp: Value) -> Value {
    let mut root = match resp {
        Value::Object(map) => map,
        other => return other,
    };

    if let Some(Value::Object(data_obj)) = root.get_mut("data") {
        let mut new_data = serde_json::Map::new();
        for (key, value) in data_obj.clone().into_iter() {
            let new_key = if key.ends_with("_by_pk") {
                key.trim_end_matches("_by_pk").to_ascii_lowercase()
            } else if is_pascal_case(&key) {
                pluralize_lowercase(&key)
            } else {
                key
            };
            new_data.insert(new_key, value);
        }
        *data_obj = new_data;
    }

    Value::Object(root)
}

fn is_pascal_case(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphabetic())
}

fn pluralize_lowercase(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with('y') {
        let pre = lower.chars().rev().nth(1).unwrap_or('a');
        if !matches!(pre, 'a' | 'e' | 'i' | 'o' | 'u') {
            return format!("{}ies", &lower[..lower.len() - 1]);
        }
    }
    if lower.ends_with("ch")
        || lower.ends_with("sh")
        || lower.ends_with('x')
        || lower.ends_with('z')
        || lower.ends_with('s')
        || lower.ends_with('o')
    {
        return format!("{}es", lower);
    }
    format!("{}s", lower)
}

async fn maybe_fetch_subgraph_debug(payload: Value) -> Option<Value> {
    let url = match std::env::var("SUBGRAPH_DEBUG_URL") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return None,
    };

    let client = reqwest::Client::new();
    let mut req = client
        .post(url)
        .header("Content-Type", "application/json")
        .json(&payload);

    // Optional auth headers for compatible subgraph endpoints
    // Priority: explicit custom header/value → bearer token → x-api-key fallbacks
    if let (Ok(header_name), Ok(header_value)) = (
        std::env::var("SUBGRAPH_AUTH_HEADER"),
        std::env::var("SUBGRAPH_AUTH_VALUE"),
    ) {
        if !header_name.trim().is_empty() && !header_value.trim().is_empty() {
            req = req.header(header_name, header_value);
        }
    } else if let Ok(token) = std::env::var("SUBGRAPH_BEARER_TOKEN") {
        if !token.trim().is_empty() {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
    } else if let Ok(key) = std::env::var("SUBGRAPH_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    } else if let Ok(key) = std::env::var("THEGRAPH_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    } else if let Ok(key) = std::env::var("TEST_THEGRAPH_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(_) => return None,
    };

    let status = resp.status().as_u16();
    let body: Value = match resp.json().await {
        Ok(b) => b,
        Err(_) => return None,
    };

    Some(serde_json::json!({
        "status": status,
        "body": body,
    }))
}

#[cfg(test)]
mod response_shape_tests {
    use super::*;

    #[test]
    fn test_pluralize_lowercase_basic() {
        assert_eq!(pluralize_lowercase("Stream"), "streams");
        assert_eq!(pluralize_lowercase("Batch"), "batches");
        assert_eq!(pluralize_lowercase("Asset"), "assets");
        assert_eq!(pluralize_lowercase("Action"), "actions");
    }

    #[test]
    fn test_transform_data_keys() {
        let resp = serde_json::json!({
            "data": {
                "Stream": [ {"id": 1} ],
                "Batch": [ {"id": 2} ],
                "stream_by_pk": {"id": 3}
            }
        });
        let out = transform_response_to_subgraph_shape(resp);
        let data = out.get("data").unwrap();
        assert!(data.get("streams").is_some());
        assert!(data.get("batches").is_some());
        assert!(data.get("stream").is_some());
        assert!(data.get("Stream").is_none());
        assert!(data.get("Batch").is_none());
        assert!(data.get("stream_by_pk").is_none());
    }
}
