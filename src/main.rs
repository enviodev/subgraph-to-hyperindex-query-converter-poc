use axum::{extract::Json, http::StatusCode, response::IntoResponse, routing::post, Router};
use reqwest;
use serde_json::Value;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing;
use tracing_subscriber;

mod conversion;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/", post(handle_query))
        .route("/debug", post(handle_debug));

    let addr: SocketAddr = "0.0.0.0:3000".parse().unwrap();
    tracing::info!("listening on {}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handle_query(Json(payload): Json<Value>) -> impl IntoResponse {
    tracing::info!("Received query: {:?}", payload);

    match conversion::convert_subgraph_to_hyperindex(&payload) {
        Ok(converted_query) => {
            tracing::info!("Converted query: {:?}", converted_query);

            // Forward the converted query to Hyperindex
            match forward_to_hyperindex(&converted_query).await {
                Ok(response) => {
                    tracing::info!("Hyperindex response: {:?}", response);
                    (StatusCode::OK, Json(response))
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
    let converted_query = conversion::convert_subgraph_to_hyperindex(&payload).unwrap();
    tracing::info!("Converted debug query: {:?}", converted_query);
    (StatusCode::OK, Json(converted_query))
}

async fn forward_to_hyperindex(query: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let response = client
        .post("https://indexer.hyperindex.xyz/53b7e25/v1/graphql")
        .header("Content-Type", "application/json")
        .json(query)
        .send()
        .await?;

    let response_json: Value = response.json().await?;
    Ok(response_json)
}
