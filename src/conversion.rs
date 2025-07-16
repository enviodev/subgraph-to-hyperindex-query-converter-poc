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
    let entity_cap = singularize_and_capitalize(&entity);
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
    // Add where clause for chainId (temporary dev note)
    let where_clause = if entity_cap == "Stream" {
        ", where: {chainId: {_eq: \"1\"}}"
    } else {
        ""
    };

    let hyperindex_query = format!(
        "query {{\n  {}(limit: {}, offset: {}{}) {}\n}}",
        entity_cap, limit, offset, where_clause, selection
    );
    Ok(hyperindex_query)
}

fn extract_entity_and_params(
    query: &str,
) -> Result<(String, HashMap<String, String>, String), ConversionError> {
    // Find the first entity (e.g., streams(first: 2, ...))
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

    // Find the parameters section
    let mut after_params = after_entity;
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
            after_params = &after_entity[param_end + 1..];
        }
    }
    // Find the selection set after the parameters (or directly after entity if no params)
    if let Some(selection_start) = after_params.find('{') {
        let selection_content = &after_params[selection_start + 1..];
        if let Some(selection_end) = find_matching_brace(selection_content) {
            selection = selection_content[..selection_end].trim().to_string();
        }
    }

    Ok((
        entity.to_string(),
        params,
        format!("{{\n    {}\n  }}", selection),
    ))
}

fn find_matching_brace(content: &str) -> Option<usize> {
    let mut brace_count = 1; // already inside one {
    for (i, ch) in content.chars().enumerate() {
        match ch {
            '{' => brace_count += 1,
            '}' => {
                brace_count -= 1;
                if brace_count == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn singularize_and_capitalize(s: &str) -> String {
    // Naive singularization: remove trailing 's' if present
    let singular = if s.ends_with('s') && s.len() > 1 {
        &s[..s.len() - 1]
    } else {
        s
    };
    let mut c = singular.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}
