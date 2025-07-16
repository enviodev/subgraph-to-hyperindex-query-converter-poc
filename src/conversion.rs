use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Invalid GraphQL query format")]
    InvalidQueryFormat,
    #[error("Unsupported query structure")]
    UnsupportedQuery,
    #[error("Missing required field: {0}")]
    MissingField(String),
}

pub fn convert_subgraph_to_hyperindex(payload: &Value) -> Result<Value, ConversionError> {
    // Extract the query from the payload
    let query = payload
        .get("query")
        .ok_or(ConversionError::MissingField("query".to_string()))?
        .as_str()
        .ok_or(ConversionError::InvalidQueryFormat)?;

    tracing::info!("Converting query: {}", query);

    // Parse the GraphQL query (simplified parsing for now)
    let converted_query = convert_query_structure(query)?;

    Ok(serde_json::json!({
        "query": converted_query
    }))
}

fn convert_query_structure(query: &str) -> Result<String, ConversionError> {
    // Find the entity and its parameters
    let (entity, params, selection) = extract_entity_and_params(query)?;
    let entity_cap = capitalize_first(&entity);
    let limit = params
        .get("first")
        .cloned()
        .unwrap_or_else(|| "10".to_string());
    let offset = params
        .get("skip")
        .cloned()
        .unwrap_or_else(|| "0".to_string());
    let order_by_field = params
        .get("orderBy")
        .cloned()
        .unwrap_or_else(|| "id".to_string());
    let order_direction = params
        .get("orderDirection")
        .cloned()
        .unwrap_or_else(|| "asc".to_string());
    let hyperindex_query = format!(
        "query {{\n  {}(limit: {}, offset: {}, order_by: {{{}: {}}}) {}\n}}",
        entity_cap, limit, offset, order_by_field, order_direction, selection
    );
    Ok(hyperindex_query)
}

fn extract_entity_and_params(
    query: &str,
) -> Result<(String, HashMap<String, String>, String), ConversionError> {
    // Find the first entity (e.g., posts(first: 5, ...))
    let open_brace = query.find('{').ok_or(ConversionError::InvalidQueryFormat)?;
    let after_brace = &query[open_brace + 1..];
    let entity_start = after_brace
        .find(|c: char| c.is_alphabetic())
        .ok_or(ConversionError::InvalidQueryFormat)?;
    let after_entity = &after_brace[entity_start..];
    let entity_end = after_entity
        .find(|c: char| c == '(' || c.is_whitespace())
        .ok_or(ConversionError::InvalidQueryFormat)?;
    let entity = &after_entity[..entity_end];
    let mut params = HashMap::new();
    let mut selection = String::new();
    if let Some(param_start) = after_entity.find('(') {
        if let Some(param_end) = after_entity.find(')') {
            let params_str = &after_entity[param_start + 1..param_end];
            for param in params_str.split(',') {
                let parts: Vec<&str> = param.trim().split(':').collect();
                if parts.len() == 2 {
                    let key = parts[0].trim();
                    let value = parts[1].trim();
                    params.insert(key.to_string(), value.to_string());
                }
            }
            // The rest is the selection set
            let selection_start = after_entity[param_end + 1..]
                .find('{')
                .ok_or(ConversionError::InvalidQueryFormat)?
                + param_end
                + 2;
            let selection_end = query
                .rfind('}')
                .ok_or(ConversionError::InvalidQueryFormat)?;
            selection = query[selection_start..selection_end].trim().to_string();
        }
    }
    Ok((
        entity.to_string(),
        params,
        format!("{{\n    {}\n  }}", selection),
    ))
}

fn capitalize_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}
