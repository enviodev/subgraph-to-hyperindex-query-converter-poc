use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Invalid GraphQL query format")]
    InvalidQueryFormat,
    #[error("Missing required field: {0}")]
    MissingField(String),
    #[error("Unsupported filter: {0}")]
    UnsupportedFilter(String),
    #[error("Complex _meta queries are not supported. Only _meta {{ block {{ number }} }} is currently available")]
    ComplexMetaQuery,
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
    // Check for _meta query first
    if query.contains("_meta") {
        return convert_meta_query(query);
    }

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
    let _order_by_field = params
        .get("orderBy")
        .cloned()
        .unwrap_or_else(|| "id".to_string());
    let _order_direction = params
        .get("orderDirection")
        .cloned()
        .unwrap_or_else(|| "asc".to_string());

    // Single-entity by primary key: singular entity, only 'id' param
    if !entity.ends_with('s') && params.len() == 1 && params.contains_key("id") {
        let pk_query = format!(
            "query {{\n  {}_by_pk(id: {}) {}\n}}",
            entity,
            params.get("id").unwrap(),
            selection
        );
        return Ok(pk_query);
    }

    // Convert filters to where clause
    let where_clause = convert_filters_to_where_clause(&params)?;

    // Add hardcoded where clause for Stream entity (temporary)
    let final_where_clause = if entity_cap == "Stream" {
        if where_clause.is_empty() {
            "where: {chainId: {_eq: \"1\"}}".to_string()
        } else {
            format!("where: {{chainId: {{_eq: \"1\"}}, {}}}", where_clause)
        }
    } else if !where_clause.is_empty() {
        format!("where: {{{}}}", where_clause)
    } else {
        String::new()
    };

    let where_param = if final_where_clause.is_empty() {
        String::new()
    } else {
        format!(", {}", final_where_clause)
    };

    let hyperindex_query = format!(
        "query {{\n  {}(limit: {}, offset: {}{}) {}\n}}",
        entity_cap, limit, offset, where_param, selection
    );
    Ok(hyperindex_query)
}

fn convert_meta_query(query: &str) -> Result<String, ConversionError> {
    // Check if it's a simple _meta { block { number } } query
    let simple_meta_pattern = "_meta { block { number } }";
    let complex_meta_patterns = [
        "block { hash",
        "block { parentHash",
        "block { timestamp",
        "deployment",
        "hasIndexingErrors",
    ];

    // Check for complex patterns
    for pattern in &complex_meta_patterns {
        if query.contains(pattern) {
            return Err(ConversionError::ComplexMetaQuery);
        }
    }

    // Check if it's the simple pattern
    if query.contains(simple_meta_pattern) {
        return Ok(
            "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}".to_string(),
        );
    }

    // If it's a _meta query but not the simple pattern, it's complex
    if query.contains("_meta") {
        return Err(ConversionError::ComplexMetaQuery);
    }

    // This shouldn't happen, but just in case
    Err(ConversionError::InvalidQueryFormat)
}

fn convert_filters_to_where_clause(
    params: &HashMap<String, String>,
) -> Result<String, ConversionError> {
    let mut where_conditions = Vec::new();

    for (key, value) in params {
        if key == "first" || key == "skip" || key == "orderBy" || key == "orderDirection" {
            continue; // Skip pagination and ordering parameters
        }

        let condition = convert_filter_to_hasura_condition(key, value)?;
        where_conditions.push(condition);
    }

    Ok(where_conditions.join(", "))
}

fn convert_filter_to_hasura_condition(key: &str, value: &str) -> Result<String, ConversionError> {
    // Handle different filter patterns
    if key.ends_with("_not") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_neq: {}}}", field, value));
    }

    if key.ends_with("_gt") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_gt: {}}}", field, value));
    }

    if key.ends_with("_gte") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_gte: {}}}", field, value));
    }

    if key.ends_with("_lt") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_lt: {}}}", field, value));
    }

    if key.ends_with("_lte") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_lte: {}}}", field, value));
    }

    if key.ends_with("_in") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_in: {}}}", field, value));
    }

    if key.ends_with("_not_in") {
        let field = &key[..key.len() - 7];
        return Ok(format!("{}: {{_nin: {}}}", field, value));
    }

    if key.ends_with("_contains") {
        let field = &key[..key.len() - 9];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_contains") {
        let field = &key[..key.len() - 13];
        return Ok(format!(
            "{}: {{_not: {{_ilike: \"%{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_starts_with") {
        let field = &key[..key.len() - 12];
        return Ok(format!(
            "{}: {{_ilike: \"{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_ends_with") {
        let field = &key[..key.len() - 10];
        return Ok(format!(
            "{}: {{_ilike: \"%{}\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_starts_with") {
        let field = &key[..key.len() - 16];
        return Ok(format!(
            "{}: {{_not: {{_ilike: \"{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_ends_with") {
        let field = &key[..key.len() - 14];
        return Ok(format!(
            "{}: {{_not: {{_ilike: \"%{}\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_contains_nocase") {
        let field = &key[..key.len() - 15];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_contains_nocase") {
        let field = &key[..key.len() - 19];
        return Ok(format!(
            "{}: {{_not: {{_ilike: \"%{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_starts_with_nocase") {
        let field = &key[..key.len() - 18];
        return Ok(format!(
            "{}: {{_ilike: \"{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_ends_with_nocase") {
        let field = &key[..key.len() - 16];
        return Ok(format!(
            "{}: {{_ilike: \"%{}\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_starts_with_nocase") {
        let field = &key[..key.len() - 22];
        return Ok(format!(
            "{}: {{_not: {{_ilike: \"{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_ends_with_nocase") {
        let field = &key[..key.len() - 20];
        return Ok(format!(
            "{}: {{_not: {{_ilike: \"%{}\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    // Handle unsupported filters
    if key.ends_with("_containsAny") || key.ends_with("_containsAll") {
        return Err(ConversionError::UnsupportedFilter(key.to_string()));
    }

    // Default case: treat as equality filter
    Ok(format!("{}: {{_eq: {}}}", key, value))
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
