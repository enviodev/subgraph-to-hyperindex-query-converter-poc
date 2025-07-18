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

#[tokio::test]
async fn test_complex_nested_query_with_multiple_filters() {
    let query = r#"{
  streams(
    first: 10,
    skip: 5,
    where: {
      alias_contains: "test",
      amount_gte: 1000,
      user: { name_starts_with: "john" }
    }
  ) {
    id
    alias
    amount
    user {
      id
      name
      profile {
        avatar
        bio
      }
    }
    asset {
      address
      decimals
      symbol
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!(
        "Converted complex nested query: {}",
        result["query"].as_str().unwrap()
    );

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Complex nested query returned errors: {:?}", errors);
                }
            }

            // Check for data structure
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!("Found {} streams in complex query", streams_array.len());

                            // Verify nested structure
                            if streams_array.len() > 0 {
                                let first_stream = &streams_array[0];
                                assert!(
                                    first_stream.get("id").is_some(),
                                    "Expected stream to have id"
                                );
                                assert!(
                                    first_stream.get("alias").is_some(),
                                    "Expected stream to have alias"
                                );
                                assert!(
                                    first_stream.get("amount").is_some(),
                                    "Expected stream to have amount"
                                );

                                // Check nested user object
                                if let Some(user) = first_stream.get("user") {
                                    if user.is_object() {
                                        let user_obj = user.as_object().unwrap();
                                        assert!(
                                            user_obj.get("id").is_some(),
                                            "Expected user to have id"
                                        );
                                        assert!(
                                            user_obj.get("name").is_some(),
                                            "Expected user to have name"
                                        );

                                        // Check nested profile
                                        if let Some(profile) = user_obj.get("profile") {
                                            if profile.is_object() {
                                                let profile_obj = profile.as_object().unwrap();
                                                assert!(
                                                    profile_obj.get("avatar").is_some()
                                                        || profile_obj.get("bio").is_some(),
                                                    "Expected profile to have avatar or bio"
                                                );
                                            }
                                        }
                                    }
                                }

                                // Check nested asset object
                                if let Some(asset) = first_stream.get("asset") {
                                    if asset.is_object() {
                                        let asset_obj = asset.as_object().unwrap();
                                        assert!(
                                            asset_obj.get("address").is_some(),
                                            "Expected asset to have address"
                                        );
                                        assert!(
                                            asset_obj.get("decimals").is_some(),
                                            "Expected asset to have decimals"
                                        );
                                        assert!(
                                            asset_obj.get("symbol").is_some(),
                                            "Expected asset to have symbol"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in complex nested query response");
            }
        }
        Err(e) => {
            panic!(
                "Failed to forward complex nested query to Hyperindex: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_multiple_entities_single_query() {
    let query = r#"{
  streams(first: 5, where: { amount_gt: 100 }) {
    id
    alias
    amount
  }
  users(first: 3, where: { name_contains: "john" }) {
    id
    name
    email
  }
  assets(first: 2) {
    id
    address
    symbol
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!(
        "Converted multiple entities query: {}",
        result["query"].as_str().unwrap()
    );

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Multiple entities query returned errors: {:?}", errors);
                }
            }

            // Check for data structure
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();

                    // Check streams
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!("Found {} streams", streams_array.len());
                            assert!(streams_array.len() <= 5, "Expected max 5 streams");
                        }
                    }

                    // Check users
                    if let Some(users) = data_obj.get("User") {
                        if users.is_array() {
                            let users_array = users.as_array().unwrap();
                            println!("Found {} users", users_array.len());
                            assert!(users_array.len() <= 3, "Expected max 3 users");
                        }
                    }

                    // Check assets
                    if let Some(assets) = data_obj.get("Asset") {
                        if assets.is_array() {
                            let assets_array = assets.as_array().unwrap();
                            println!("Found {} assets", assets_array.len());
                            assert!(assets_array.len() <= 2, "Expected max 2 assets");
                        }
                    }
                }
            } else {
                panic!("No data field in multiple entities query response");
            }
        }
        Err(e) => {
            panic!(
                "Failed to forward multiple entities query to Hyperindex: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_advanced_filter_combinations() {
    let query = r#"{
  streams(
    first: 20,
    where: {
      amount_gte: 1000,
      amount_lte: 10000,
      alias_contains: "test",
      alias_not_contains: "invalid",
      user: {
        name_starts_with: "john",
        email_ends_with: "@example.com",
        age_gt: 18,
        age_lt: 65
      }
    }
  ) {
    id
    alias
    amount
    user {
      id
      name
      email
      age
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!(
        "Converted advanced filters query: {}",
        result["query"].as_str().unwrap()
    );

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Advanced filters query returned errors: {:?}", errors);
                }
            }

            // Check for data structure
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!(
                                "Found {} streams with advanced filters",
                                streams_array.len()
                            );

                            // Verify filter results
                            for stream in streams_array {
                                if let Some(amount) = stream.get("amount") {
                                    if let Some(amount_num) = amount.as_f64() {
                                        assert!(
                                            amount_num >= 1000.0,
                                            "Expected amount >= 1000, got {}",
                                            amount_num
                                        );
                                        assert!(
                                            amount_num <= 10000.0,
                                            "Expected amount <= 10000, got {}",
                                            amount_num
                                        );
                                    }
                                }

                                if let Some(alias) = stream.get("alias") {
                                    if let Some(alias_str) = alias.as_str() {
                                        assert!(
                                            alias_str.contains("test"),
                                            "Expected alias to contain 'test'"
                                        );
                                        assert!(
                                            !alias_str.contains("invalid"),
                                            "Expected alias to not contain 'invalid'"
                                        );
                                    }
                                }

                                // Check nested user filters
                                if let Some(user) = stream.get("user") {
                                    if user.is_object() {
                                        let user_obj = user.as_object().unwrap();
                                        if let Some(name) = user_obj.get("name") {
                                            if let Some(name_str) = name.as_str() {
                                                assert!(
                                                    name_str.starts_with("john"),
                                                    "Expected name to start with 'john'"
                                                );
                                            }
                                        }
                                        if let Some(email) = user_obj.get("email") {
                                            if let Some(email_str) = email.as_str() {
                                                assert!(
                                                    email_str.ends_with("@example.com"),
                                                    "Expected email to end with '@example.com'"
                                                );
                                            }
                                        }
                                        if let Some(age) = user_obj.get("age") {
                                            if let Some(age_num) = age.as_f64() {
                                                assert!(
                                                    age_num > 18.0,
                                                    "Expected age > 18, got {}",
                                                    age_num
                                                );
                                                assert!(
                                                    age_num < 65.0,
                                                    "Expected age < 65, got {}",
                                                    age_num
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in advanced filters query response");
            }
        }
        Err(e) => {
            panic!(
                "Failed to forward advanced filters query to Hyperindex: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_pagination_and_ordering_edge_cases() {
    let query = r#"{
  streams(
    first: 1,
    skip: 999,
    orderBy: id,
    orderDirection: desc,
    where: { amount_gt: 0 }
  ) {
    id
    alias
    amount
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!(
        "Converted pagination edge case query: {}",
        result["query"].as_str().unwrap()
    );

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Pagination edge case query returned errors: {:?}", errors);
                }
            }

            // Check for data structure
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!(
                                "Found {} streams with pagination edge case",
                                streams_array.len()
                            );

                            // With skip: 999, we might get 0 or 1 results
                            assert!(
                                streams_array.len() <= 1,
                                "Expected max 1 stream with high skip"
                            );
                        }
                    }
                }
            } else {
                panic!("No data field in pagination edge case query response");
            }
        }
        Err(e) => {
            panic!(
                "Failed to forward pagination edge case query to Hyperindex: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_string_vs_numeric_filter_values() {
    let query = r#"{
  streams(
    where: {
      amount_gt: "1000",
      amount_lt: 5000,
      user: {
        age_gte: "18",
        age_lte: 65
      }
    }
  ) {
    id
    amount
    user {
      id
      age
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!(
        "Converted mixed type filters query: {}",
        result["query"].as_str().unwrap()
    );

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Mixed type filters query returned errors: {:?}", errors);
                }
            }

            // Check for data structure
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!(
                                "Found {} streams with mixed type filters",
                                streams_array.len()
                            );

                            // Verify numeric filter results
                            for stream in streams_array {
                                if let Some(amount) = stream.get("amount") {
                                    if let Some(amount_num) = amount.as_f64() {
                                        assert!(
                                            amount_num > 1000.0,
                                            "Expected amount > 1000, got {}",
                                            amount_num
                                        );
                                        assert!(
                                            amount_num < 5000.0,
                                            "Expected amount < 5000, got {}",
                                            amount_num
                                        );
                                    }
                                }

                                // Check nested user filters
                                if let Some(user) = stream.get("user") {
                                    if user.is_object() {
                                        let user_obj = user.as_object().unwrap();
                                        if let Some(age) = user_obj.get("age") {
                                            if let Some(age_num) = age.as_f64() {
                                                assert!(
                                                    age_num >= 18.0,
                                                    "Expected age >= 18, got {}",
                                                    age_num
                                                );
                                                assert!(
                                                    age_num <= 65.0,
                                                    "Expected age <= 65, got {}",
                                                    age_num
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in mixed type filters query response");
            }
        }
        Err(e) => {
            panic!(
                "Failed to forward mixed type filters query to Hyperindex: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_case_sensitive_vs_insensitive_filters() {
    let query = r#"{
  streams(
    where: {
      alias_contains: "TEST",
      alias_contains_nocase: "test",
      user: {
        name_starts_with: "JOHN",
        name_starts_with_nocase: "john"
      }
    }
  ) {
    id
    alias
    user {
      id
      name
    }
  }
}"#;

    let payload = json!({
        "query": query
    });

    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    println!(
        "Converted case sensitivity query: {}",
        result["query"].as_str().unwrap()
    );

    // Forward to Hyperindex
    let response = forward_to_hyperindex(&result).await;
    match response {
        Ok(response_json) => {
            // Check for errors
            if let Some(errors) = response_json.get("errors") {
                if errors.is_array() && errors.as_array().unwrap().len() > 0 {
                    panic!("Case sensitivity query returned errors: {:?}", errors);
                }
            }

            // Check for data structure
            if let Some(data) = response_json.get("data") {
                if data.is_object() {
                    let data_obj = data.as_object().unwrap();
                    if let Some(streams) = data_obj.get("Stream") {
                        if streams.is_array() {
                            let streams_array = streams.as_array().unwrap();
                            println!(
                                "Found {} streams with case sensitivity filters",
                                streams_array.len()
                            );

                            // Verify case sensitivity results
                            for stream in streams_array {
                                if let Some(alias) = stream.get("alias") {
                                    if let Some(alias_str) = alias.as_str() {
                                        // Should contain "TEST" (case sensitive) or "test" (case insensitive)
                                        assert!(
                                            alias_str.contains("TEST") || alias_str.to_lowercase().contains("test"),
                                            "Expected alias to contain 'TEST' or 'test' (case insensitive)"
                                        );
                                    }
                                }

                                // Check nested user filters
                                if let Some(user) = stream.get("user") {
                                    if user.is_object() {
                                        let user_obj = user.as_object().unwrap();
                                        if let Some(name) = user_obj.get("name") {
                                            if let Some(name_str) = name.as_str() {
                                                // Should start with "JOHN" (case sensitive) or "john" (case insensitive)
                                                assert!(
                                                    name_str.starts_with("JOHN") || name_str.to_lowercase().starts_with("john"),
                                                    "Expected name to start with 'JOHN' or 'john' (case insensitive)"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                panic!("No data field in case sensitivity query response");
            }
        }
        Err(e) => {
            panic!(
                "Failed to forward case sensitivity query to Hyperindex: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_response_format_comparison() {
    // Test query from the example - add first: 10 to limit results
    let subgraph_query = r#"{
  streams(first: 10, orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;

    // Convert the query to Hyperindex format
    let payload = json!({
        "query": subgraph_query
    });

    let converted_result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
    let converted_query = converted_result["query"].as_str().unwrap();

    println!("Original subgraph query: {}", subgraph_query);
    println!("Converted Hyperindex query: {}", converted_query);

    // Make request to TheGraph
    let thegraph_response = make_thegraph_request(subgraph_query).await;
    println!("TheGraph response: {:?}", thegraph_response);

    // Make request to Hyperindex
    let hyperindex_response = forward_to_hyperindex(&converted_result).await;
    println!("Hyperindex response: {:?}", hyperindex_response);

    // Compare the responses
    match (thegraph_response, hyperindex_response) {
        (Ok(thegraph_data), Ok(hyperindex_data)) => {
            // Extract the actual data from both responses
            let thegraph_streams = extract_streams_from_response(&thegraph_data);
            let hyperindex_streams = extract_streams_from_response(&hyperindex_data);

            println!("TheGraph streams count: {}", thegraph_streams.len());
            println!("Hyperindex streams count: {}", hyperindex_streams.len());

            // For now, just check that both returned some data
            // In a real scenario, you'd want to compare the actual data structure
            assert!(
                !thegraph_streams.is_empty(),
                "TheGraph should return some streams"
            );
            assert!(
                !hyperindex_streams.is_empty(),
                "Hyperindex should return some streams"
            );

            // Compare the structure of the first few items
            if !thegraph_streams.is_empty() && !hyperindex_streams.is_empty() {
                let thegraph_first = &thegraph_streams[0];
                let hyperindex_first = &hyperindex_streams[0];

                // Check that both have the expected fields
                assert!(
                    thegraph_first.get("alias").is_some(),
                    "TheGraph stream should have alias"
                );
                assert!(
                    hyperindex_first.get("alias").is_some(),
                    "Hyperindex stream should have alias"
                );
                assert!(
                    thegraph_first.get("asset").is_some(),
                    "TheGraph stream should have asset"
                );
                assert!(
                    hyperindex_first.get("asset").is_some(),
                    "Hyperindex stream should have asset"
                );

                // Check asset structure
                if let (Some(thegraph_asset), Some(hyperindex_asset)) =
                    (thegraph_first.get("asset"), hyperindex_first.get("asset"))
                {
                    assert!(
                        thegraph_asset.get("address").is_some(),
                        "TheGraph asset should have address"
                    );
                    assert!(
                        hyperindex_asset.get("address").is_some(),
                        "Hyperindex asset should have address"
                    );
                }

                // Verify that the filter is working correctly
                // All returned streams should contain "113" in their alias
                for stream in &thegraph_streams {
                    let alias = stream.get("alias").unwrap().as_str().unwrap();
                    assert!(
                        alias.contains("113"),
                        "TheGraph stream alias should contain '113'"
                    );
                }

                for stream in &hyperindex_streams {
                    let alias = stream.get("alias").unwrap().as_str().unwrap();
                    assert!(
                        alias.contains("113"),
                        "Hyperindex stream alias should contain '113'"
                    );
                }

                println!("✅ Both endpoints returned data with correct structure and filtering");
                println!(
                    "✅ TheGraph: {} streams, Hyperindex: {} streams",
                    thegraph_streams.len(),
                    hyperindex_streams.len()
                );
            }
        }
        (Err(thegraph_err), Ok(_)) => {
            panic!("TheGraph request failed: {}", thegraph_err);
        }
        (Ok(_), Err(hyperindex_err)) => {
            panic!("Hyperindex request failed: {}", hyperindex_err);
        }
        (Err(thegraph_err), Err(hyperindex_err)) => {
            panic!(
                "Both requests failed. TheGraph: {}, Hyperindex: {}",
                thegraph_err, hyperindex_err
            );
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

async fn make_thegraph_request(query: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    let payload = json!({
        "query": query
    });

    let response = client
        .post("https://gateway.thegraph.com/api/subgraphs/id/AvDAMYYHGaEwn9F9585uqq6MM5CfvRtYcb7KjK7LKPCt")
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer ac5c29b9c91daa7f78e37fc73860ff60")
        .json(&payload)
        .send()
        .await?;

    let response_json: Value = response.json().await?;
    Ok(response_json)
}

fn extract_streams_from_response(response: &Value) -> Vec<Value> {
    if let Some(data) = response.get("data") {
        if let Some(streams) = data.get("streams") {
            if let Some(streams_array) = streams.as_array() {
                return streams_array.clone();
            }
        }
        // Also check for "Stream" (Hyperindex format)
        if let Some(streams) = data.get("Stream") {
            if let Some(streams_array) = streams.as_array() {
                return streams_array.clone();
            }
        }
    }
    Vec::new()
}
