use serde_json::{json, Value};
use std::env;
use tokio;

use crate::conversion;

#[tokio::test]
async fn test_actions_and_assets_query() {
    let query = r#"{
  actions(first: 5) {
    id
    block
    category
    chainId
  }
  assets(first: 5) {
    id
    address
    chainId
    decimals
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!("Converted query: {}", result["query"].as_str().unwrap());

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            println!("Response: {:?}", response_json);

            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Query returned errors: {:?}", errors);
                }
            }

            // Check for data
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();

                    // Check for actions
                    if let Some(actions) = data_obj.get("Action") {
                        if actions.is_array() {
                            let actions_array = actions.as_array().unwrap();
                            println!("Found {} actions", actions_array.len());
                            assert!(actions_array.len() > 0, "Expected actions to return data");
                        }
                    }

                    // Check for assets
                    if let Some(assets) = data_obj.get("Asset") {
                        if assets.is_array() {
                            let assets_array = assets.as_array().unwrap();
                            println!("Found {} assets", assets_array.len());
                            assert!(assets_array.len() > 0, "Expected assets to return data");
                        }
                    }
                }
            } else {
                panic!("No data field in response");
            }
        }
        Err(e) => {
            panic!("Failed to forward query to Hyperindex: {}", e);
        }
    }
}

#[tokio::test]
async fn test_streams_with_order_by_query() {
    let query = r#"{
  streams(orderBy: id, skip: 10) {
    alias
    asset {
      address
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!("Converted query: {}", result["query"].as_str().unwrap());

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            println!("Response: {:?}", response_json);

            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Query returned errors: {:?}", errors);
                }
            }

            // Check for data
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();

                    // Check for streams
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!("Found {} streams", streams_array.len());
                            assert!(streams_array.len() > 0, "Expected streams to return data");

                            // Check that streams have the expected structure
                            if streams_array.len() > 0 {
                                let first_stream = &streams_array[0];
                                if let Some(alias) = first_stream.get("alias") {
                                    assert!(alias.is_string(), "Expected alias to be a string");
                                }
                                if let Some(asset) = first_stream.get("asset") {
                                    if asset.is_object() {
                                        let asset_obj = asset.as_object().unwrap();
                                        if let Some(address) = asset_obj.get("address") {
                                            assert!(
                                                address.is_string(),
                                                "Expected asset address to be a string"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in response");
            }
        }
        Err(e) => {
            panic!("Failed to forward query to Hyperindex: {}", e);
        }
    }
}

#[tokio::test]
async fn test_streams_with_filter_query() {
    let query = r#"{
  streams(orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    let converted_query = result["query"].as_str().unwrap();
    std::fs::write("converted_query.txt", converted_query).expect("Unable to write file");
    println!("\n================ CONVERTED QUERY WRITTEN TO FILE ================\n");

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            println!("Response: {:?}", response_json);

            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Query returned errors: {:?}", errors);
                }
            }

            // Check for data
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();

                    // Check for streams
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!("Found {} streams with filter", streams_array.len());

                            // Check that filtered streams contain "113" in their alias
                            for stream in streams_array {
                                if let Some(alias) = stream.get("alias") {
                                    if let Some(alias_str) = alias.as_str() {
                                        assert!(
                                            alias_str.contains("113"),
                                            "Expected stream alias to contain '113', got: {}",
                                            alias_str
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in response");
            }
        }
        Err(e) => {
            panic!("Failed to forward query to Hyperindex: {}", e);
        }
    }
}

#[tokio::test]
async fn test_streams_with_order_by_and_skip_query() {
    let query = r#"{
  streams(orderBy: id, skip: 10) {
    alias
    asset {
      address
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!("Converted query: {}", result["query"].as_str().unwrap());

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            println!("Response: {:?}", response_json);

            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Query returned errors: {:?}", errors);
                }
            }

            // Check for data
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();

                    // Check for streams
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!(
                                "Found {} streams with orderBy and skip",
                                streams_array.len()
                            );
                            assert!(streams_array.len() > 0, "Expected streams to return data");

                            // Check that streams have the expected structure
                            if streams_array.len() > 0 {
                                let first_stream = &streams_array[0];
                                if let Some(alias) = first_stream.get("alias") {
                                    assert!(alias.is_string(), "Expected alias to be a string");
                                }
                                if let Some(asset) = first_stream.get("asset") {
                                    if asset.is_object() {
                                        let asset_obj = asset.as_object().unwrap();
                                        if let Some(address) = asset_obj.get("address") {
                                            assert!(
                                                address.is_string(),
                                                "Expected asset address to be a string"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in response");
            }
        }
        Err(e) => {
            panic!("Failed to forward query to Hyperindex: {}", e);
        }
    }
}

#[tokio::test]
async fn test_streams_with_order_by_skip_and_filter_query() {
    let query = r#"{
  streams(orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!("Converted query: {}", result["query"].as_str().unwrap());

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            println!("Response: {:?}", response_json);

            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Query returned errors: {:?}", errors);
                }
            }

            // Check for data
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();

                    // Check for streams
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!(
                                "Found {} streams with orderBy, skip, and filter",
                                streams_array.len()
                            );
                            assert!(streams_array.len() > 0, "Expected streams to return data");

                            // Check that filtered streams contain "113" in their alias
                            for stream in streams_array {
                                if let Some(alias) = stream.get("alias") {
                                    if let Some(alias_str) = alias.as_str() {
                                        assert!(
                                            alias_str.contains("113"),
                                            "Expected stream alias to contain '113', got: {}",
                                            alias_str
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in response");
            }
        }
        Err(e) => {
            panic!("Failed to forward query to Hyperindex: {}", e);
        }
    }
}

async fn forward_to_hyperindex(query: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let hyperindex_url = env::var("TEST_HYPERINDEX_URL")
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
