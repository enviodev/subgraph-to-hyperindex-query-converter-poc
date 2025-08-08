use axum::{
    extract::{Json, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use dotenv;
use reqwest;
use serde_json::Value;
use std::net::SocketAddr;
use tokio::net::TcpListener;
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

    let app = Router::new()
        .route("/", post(handle_query))
        .route("/debug", post(handle_debug))
        .route("/chainId/:chain_id", post(handle_chain_query))
        .route("/chainId/:chain_id/debug", post(handle_chain_debug));

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
                    let transformed = transform_response_to_subgraph_shape(response);
                    (StatusCode::OK, Json(transformed))
                }
                Err(e) => {
                    tracing::error!("Hyperindex request error: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": format!("Hyperindex request failed: {}", e)
                        })),
                    )
                }
            }
        }
        Err(e) => {
            tracing::error!("Conversion error: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": e.to_string()
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
                    let transformed = transform_response_to_subgraph_shape(response);
                    (StatusCode::OK, Json(transformed))
                }
                Err(e) => {
                    tracing::error!("Hyperindex request error: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": format!("Hyperindex request failed: {}", e)
                        })),
                    )
                }
            }
        }
        Err(e) => {
            tracing::error!("Conversion error: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": e.to_string()
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
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": e.to_string()
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
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": e.to_string()
                })),
            )
        }
    }
}

async fn forward_to_hyperindex(query: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let hyperindex_url = std::env::var("HYPERINDEX_URL")
        .unwrap_or_else(|_| "https://indexer.hyperindex.xyz/53b7e25/v1/graphql".to_string());

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
