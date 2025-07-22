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

pub fn convert_subgraph_to_hyperindex(
    payload: &Value,
    chain_id: Option<&str>,
) -> Result<Value, ConversionError> {
    // Extract the query from the payload
    let query = payload
        .get("query")
        .ok_or(ConversionError::MissingField("query".to_string()))?
        .as_str()
        .ok_or(ConversionError::InvalidQueryFormat)?;

    tracing::info!("Converting query: {}", query);

    // Parse the GraphQL query (simplified parsing for now)
    let converted_query = convert_query_structure(query, chain_id)?;

    Ok(serde_json::json!({
        "query": converted_query
    }))
}

fn convert_query_structure(query: &str, chain_id: Option<&str>) -> Result<String, ConversionError> {
    // Check for _meta query first
    if query.contains("_meta") {
        return convert_meta_query(query);
    }

    // Extract fragments and main query
    let (fragments, main_query) = extract_fragments_and_main_query(query)?;

    // Convert the main query
    let converted_main_query = convert_main_query(&main_query, chain_id)?;

    // Combine fragments with converted main query
    let mut result = String::new();
    if !fragments.is_empty() {
        result.push_str(&fragments);
        result.push('\n');
    }
    result.push_str(&converted_main_query);

    Ok(result)
}

fn extract_fragments_and_main_query(query: &str) -> Result<(String, String), ConversionError> {
    let mut fragments = String::new();
    let mut main_query = String::new();
    let mut lines = query.lines();
    let mut in_fragment = false;
    let mut brace_count = 0;

    // Extract fragments
    while let Some(line) = lines.next() {
        let trimmed = line.trim();

        if trimmed.starts_with("fragment ") {
            // Start of a fragment
            in_fragment = true;
            brace_count = 0;
            fragments.push_str(line);
            fragments.push('\n');
        } else if in_fragment {
            // Check if we've reached the query keyword (which means fragments are done)
            if trimmed.starts_with("query") {
                in_fragment = false;
                // Don't include the query line in fragments, add it to main query instead
                main_query.push_str(line);
                main_query.push('\n');
            } else {
                // We're inside a fragment
                fragments.push_str(line);
                fragments.push('\n');

                // Count braces to know when fragment ends
                for char in line.chars() {
                    if char == '{' {
                        brace_count += 1;
                    } else if char == '}' {
                        brace_count -= 1;
                        if brace_count == 0 {
                            // Fragment ended
                            in_fragment = false;
                            break;
                        }
                    }
                }
            }
        } else if trimmed.starts_with("query") {
            // Start of main query
            main_query.push_str(line);
            main_query.push('\n');
            // Don't break here, continue to collect the rest of the main query
        } else if !trimmed.is_empty() && !in_fragment {
            // Part of main query (but not inside a fragment)
            main_query.push_str(line);
            main_query.push('\n');
        }
    }

    // Add remaining lines to main query
    for line in lines {
        main_query.push_str(line);
        main_query.push('\n');
    }

    Ok((fragments.trim().to_string(), main_query.trim().to_string()))
}

fn convert_main_query(main_query: &str, chain_id: Option<&str>) -> Result<String, ConversionError> {
    // Strip the outer query { } wrapper if present
    let stripped_query = if main_query.trim().starts_with("query {") {
        let content = main_query.trim();
        let start = content.find('{').unwrap() + 1;
        let end = content.rfind('}').unwrap();
        &content[start..end]
    } else {
        main_query
    };

    // Extract multiple entities from the main query
    let entities = extract_multiple_entities(stripped_query)?;

    let mut converted_entities = Vec::new();

    for (entity, params, selection) in entities {
        let entity_cap = singularize_and_capitalize(&entity);
        let limit = params.get("first").cloned();
        let offset = params.get("skip").cloned();

        // Single-entity by primary key: singular entity, only 'id' param
        if !entity.ends_with('s') && params.len() == 1 && params.contains_key("id") {
            let pk_query = format!(
                "  {}_by_pk(id: {}) {}",
                entity,
                params.get("id").unwrap(),
                selection
            );
            converted_entities.push(pk_query);
            continue;
        }

        let mut converted_params = params.clone();

        // Add chainId to params if provided
        if let Some(chain_id) = chain_id {
            converted_params.insert("chainId".to_string(), format!("\"{}\"", chain_id));
        }

        // Convert filters to where clause (flattened)
        let where_clause = convert_filters_to_where_clause(&converted_params)?;

        let mut params_vec = Vec::new();
        if let Some(l) = limit.as_ref() {
            params_vec.push(format!("limit: {}", l));
        }
        if let Some(o) = offset.as_ref() {
            params_vec.push(format!("offset: {}", o));
        }
        if !where_clause.is_empty() {
            params_vec.push(where_clause);
        }
        let params_str = if params_vec.is_empty() {
            String::new()
        } else {
            format!("({})", params_vec.join(", "))
        };

        let converted_entity = format!("  {}{} {}", entity_cap, params_str, selection);
        converted_entities.push(converted_entity);
    }

    let converted_query = format!("query {{\n{}\n}}", converted_entities.join("\n"));
    Ok(converted_query)
}

fn extract_multiple_entities(
    query: &str,
) -> Result<Vec<(String, HashMap<String, String>, String)>, ConversionError> {
    let mut entities = Vec::new();

    // Find all entity patterns in the query using regex-like approach
    let mut current_pos = 0;
    let query_chars: Vec<char> = query.chars().collect();

    while current_pos < query_chars.len() {
        // Skip whitespace
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        if current_pos >= query_chars.len() {
            break;
        }

        // Look for entity name (word characters)
        let mut entity_start = current_pos;
        while current_pos < query_chars.len() && query_chars[current_pos].is_alphanumeric() {
            current_pos += 1;
        }

        if current_pos == entity_start {
            current_pos += 1;
            continue;
        }

        let entity_name: String = query_chars[entity_start..current_pos].iter().collect();

        // Skip whitespace
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        // Check if next character is '('
        if current_pos < query_chars.len() && query_chars[current_pos] == '(' {
            // Find the closing parenthesis
            let mut paren_count = 0;
            let mut param_start = current_pos + 1;
            let mut param_end = None;

            while current_pos < query_chars.len() {
                match query_chars[current_pos] {
                    '(' => paren_count += 1,
                    ')' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            param_end = Some(current_pos);
                            break;
                        }
                    }
                    _ => {}
                }
                current_pos += 1;
            }

            if let Some(param_end) = param_end {
                let params_str: String = query_chars[param_start..param_end].iter().collect();

                let mut params = HashMap::new();
                parse_graphql_params(&params_str, &mut params)?;

                // Find the opening brace
                while current_pos < query_chars.len() && query_chars[current_pos] != '{' {
                    current_pos += 1;
                }

                if current_pos < query_chars.len() {
                    // Extract selection set
                    let selection = extract_selection_set_chars(&query_chars, current_pos)?;

                    entities.push((entity_name, params, selection));
                }
            }
        }

        current_pos += 1;
    }

    if entities.is_empty() {
        return Err(ConversionError::InvalidQueryFormat);
    }

    Ok(entities)
}

fn extract_selection_set_chars(
    chars: &[char],
    start_pos: usize,
) -> Result<String, ConversionError> {
    let mut selection = String::new();
    let mut brace_count = 0;
    let mut pos = start_pos;

    while pos < chars.len() {
        let char = chars[pos];

        if char == '{' {
            brace_count += 1;
            selection.push(char);
        } else if char == '}' {
            brace_count -= 1;
            selection.push(char);
            if brace_count == 0 {
                return Ok(selection);
            }
        } else {
            selection.push(char);
        }

        pos += 1;
    }

    Err(ConversionError::InvalidQueryFormat)
}

fn extract_selection_set(
    lines: &[&str],
    start_line: usize,
    brace_start: usize,
) -> Result<String, ConversionError> {
    let mut selection = String::new();
    let mut brace_count = 0;
    let mut started = false;

    // Start from the opening brace
    let mut current_line = &lines[start_line][brace_start..];

    loop {
        for char in current_line.chars() {
            if char == '{' {
                brace_count += 1;
                if !started {
                    started = true;
                    selection.push(char);
                } else {
                    selection.push(char);
                }
            } else if char == '}' {
                brace_count -= 1;
                selection.push(char);
                if brace_count == 0 {
                    return Ok(selection);
                }
            } else {
                if started {
                    selection.push(char);
                }
            }
        }

        // Move to next line
        if start_line + 1 < lines.len() {
            selection.push('\n');
            current_line = lines[start_line + 1];
        } else {
            break;
        }
    }

    Err(ConversionError::InvalidQueryFormat)
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

fn flatten_where_map(mut map: HashMap<String, String>) -> HashMap<String, String> {
    let mut flat = HashMap::new();
    for (k, v) in map.drain() {
        if k == "where" {
            // Recursively parse and flatten
            if let Ok(nested) = parse_nested_where_clause(&v) {
                for (nk, nv) in flatten_where_map(nested) {
                    flat.insert(nk, nv);
                }
            }
        } else {
            flat.insert(k, v);
        }
    }
    flat
}

fn convert_filters_to_where_clause(
    params: &HashMap<String, String>,
) -> Result<String, ConversionError> {
    // Recursively flatten the entire params map
    let mut flat_filters = flatten_where_map(params.clone());

    // Remove pagination/order keys
    flat_filters.remove("first");
    flat_filters.remove("skip");
    flat_filters.remove("orderBy");
    flat_filters.remove("orderDirection");
    flat_filters.remove("where");

    // Sort keys to ensure consistent order, with chainId first
    let mut sorted_keys: Vec<_> = flat_filters.keys().collect();
    sorted_keys.sort_by(|a, b| {
        if *a == "chainId" {
            std::cmp::Ordering::Less
        } else if *b == "chainId" {
            std::cmp::Ordering::Greater
        } else {
            a.cmp(b)
        }
    });

    let mut where_conditions = Vec::new();
    for key in sorted_keys {
        let value = flat_filters.get(key).unwrap();
        let condition = if key.contains('.') {
            convert_nested_filter_to_hasura_condition(key, value)?
        } else {
            convert_basic_filter_to_hasura_condition(key, value)?
        };
        where_conditions.push(condition);
    }

    if where_conditions.is_empty() {
        return Ok(String::new());
    }

    Ok(format!("where: {{{}}}", where_conditions.join(", ")))
}

fn parse_nested_where_clause(
    where_value: &str,
) -> Result<HashMap<String, String>, ConversionError> {
    let mut nested_params = HashMap::new();

    // Remove outer braces if present
    let content = where_value
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}');

    // Parse the nested where clause using the same logic as parse_graphql_params
    parse_graphql_params(content, &mut nested_params)?;
    Ok(nested_params)
}

fn convert_basic_filter_to_hasura_condition(
    key: &str,
    value: &str,
) -> Result<String, ConversionError> {
    if key == "where" {
        // Should never emit a 'where' key at this stage
        return Ok(String::new());
    }

    // Handle different filter patterns - check longer suffixes first
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

    if key.ends_with("_contains_nocase") {
        let field = &key[..key.len() - 15];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
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

    if key.ends_with("_contains") {
        let field = &key[..key.len() - 9];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_in") {
        let field = &key[..key.len() - 7];
        return Ok(format!("{}: {{_nin: {}}}", field, value));
    }

    if key.ends_with("_gte") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_gte: {}}}", field, value));
    }

    if key.ends_with("_lte") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_lte: {}}}", field, value));
    }

    if key.ends_with("_not") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_neq: {}}}", field, value));
    }

    if key.ends_with("_gt") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_gt: {}}}", field, value));
    }

    if key.ends_with("_lt") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_lt: {}}}", field, value));
    }

    if key.ends_with("_in") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_in: {}}}", field, value));
    }

    // Handle unsupported filters
    if key.ends_with("_containsAny") || key.ends_with("_containsAll") {
        return Err(ConversionError::UnsupportedFilter(key.to_string()));
    }

    // Default case: treat as equality filter
    let result = format!("{}: {{_eq: {}}}", key, value);
    Ok(result)
}

fn convert_nested_filter_to_hasura_condition(
    key: &str,
    value: &str,
) -> Result<String, ConversionError> {
    // Split the key into parent and child parts (e.g., "user.name_starts_with" -> "user" and "name_starts_with")
    if let Some(dot_idx) = key.rfind('.') {
        let parent = &key[..dot_idx];
        let child_key = &key[dot_idx + 1..];

        // Convert the child filter to Hasura condition using the basic conversion
        let child_condition = convert_basic_filter_to_hasura_condition(child_key, value)?;

        // Create nested structure: parent: { child_condition }
        // Wrap the child condition in braces
        Ok(format!(
            "{}: {{{}}}",
            parent,
            child_condition
                .trim_start_matches('{')
                .trim_end_matches('}')
        ))
    } else {
        // Fallback to regular conversion if no dot found
        convert_basic_filter_to_hasura_condition(key, value)
    }
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
            parse_graphql_params(params_str, &mut params)?;
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

fn parse_graphql_params(
    params_str: &str,
    params: &mut HashMap<String, String>,
) -> Result<(), ConversionError> {
    let mut current_param = String::new();
    let mut brace_count = 0;
    let mut bracket_count = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for ch in params_str.chars() {
        if escape_next {
            current_param.push(ch);
            escape_next = false;
            continue;
        }

        if ch == '\\' {
            escape_next = true;
            current_param.push(ch);
            continue;
        }

        if ch == '"' {
            in_string = !in_string;
            current_param.push(ch);
            continue;
        }

        if !in_string {
            match ch {
                '{' => {
                    brace_count += 1;
                    current_param.push(ch);
                }
                '}' => {
                    brace_count -= 1;
                    current_param.push(ch);
                }
                '[' => {
                    bracket_count += 1;
                    current_param.push(ch);
                }
                ']' => {
                    bracket_count -= 1;
                    current_param.push(ch);
                }
                ',' => {
                    if brace_count == 0 && bracket_count == 0 {
                        parse_single_param(&current_param, params)?;
                        current_param.clear();
                    } else {
                        current_param.push(ch);
                    }
                }
                _ => current_param.push(ch),
            }
        } else {
            current_param.push(ch);
        }
    }

    if !current_param.trim().is_empty() {
        parse_single_param(&current_param, params)?;
    }

    Ok(())
}

fn parse_single_param(
    param_str: &str,
    params: &mut HashMap<String, String>,
) -> Result<(), ConversionError> {
    let trimmed = param_str.trim();
    if let Some(idx) = trimmed.find(':') {
        let key = trimmed[..idx].trim();
        let value = trimmed[idx + 1..].trim();

        // Special handling for 'where' clause - don't flatten it
        if key == "where" && value.starts_with('{') && value.ends_with('}') {
            // Parse the nested object but don't flatten the keys
            let nested_content = &value[1..value.len() - 1];
            let mut nested_params = HashMap::new();
            parse_graphql_params(nested_content, &mut nested_params)?;

            // Add nested params directly without flattening
            for (nested_key, nested_value) in nested_params {
                params.insert(nested_key, nested_value);
            }
        } else if value.starts_with('{') && value.ends_with('}') {
            // Parse the nested object
            let nested_content = &value[1..value.len() - 1];
            let mut nested_params = HashMap::new();
            parse_graphql_params(nested_content, &mut nested_params)?;

            // Convert nested params to flattened keys
            for (nested_key, nested_value) in nested_params {
                let flattened_key = format!("{}.{}", key, nested_key);
                params.insert(flattened_key, nested_value);
            }
        } else {
            params.insert(key.to_string(), value.to_string());
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_payload(query: &str) -> Value {
        json!({
            "query": query
        })
    }

    #[test]
    fn test_basic_collection_query() {
        let payload = create_test_payload("query { streams(first: 10, skip: 0) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 10, offset: 0, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_single_entity_query() {
        let payload = create_test_payload("query { stream(id: \"123\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  stream_by_pk(id: \"123\") {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_meta_query_simple() {
        let payload = create_test_payload("query { _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_meta_query_complex() {
        let payload = create_test_payload("query { _meta { block { hash number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    // Filter tests
    #[test]
    fn test_equality_filter() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_filter() {
        let payload = create_test_payload("query { streams(name_not: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_neq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_greater_than_filter() {
        let payload = create_test_payload("query { streams(amount_gt: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_gt: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_greater_than_or_equal_filter() {
        let payload = create_test_payload("query { streams(amount_gte: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_gte: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_less_than_filter() {
        let payload = create_test_payload("query { streams(amount_lt: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_lt: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_less_than_or_equal_filter() {
        let payload = create_test_payload("query { streams(amount_lte: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_lte: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_in_filter() {
        let payload =
            create_test_payload("query { streams(id_in: [\"1\", \"2\", \"3\"]) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, id: {_in: [\"1\", \"2\", \"3\"]}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_in_filter() {
        let payload =
            create_test_payload("query { streams(id_not_in: [\"1\", \"2\", \"3\"]) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, id: {_nin: [\"1\", \"2\", \"3\"]}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_contains_filter() {
        let payload = create_test_payload("query { streams(name_contains: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_contains_filter() {
        let payload =
            create_test_payload("query { streams(name_not_contains: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_not: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_starts_with_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_starts_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_starts_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_not: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_not: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_contains_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_contains_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name_: {_ilike: \"%test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_contains_nocase_filter() {
        let payload = create_test_payload(
            "query { streams(name_not_contains_nocase: \"test\") { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name_: {_not: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_starts_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name_: {_ilike: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_ends_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name_: {_ilike: \"%test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_starts_with_nocase_filter() {
        let payload = create_test_payload(
            "query { streams(name_not_starts_with_nocase: \"test\") { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name_: {_not: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_ends_with_nocase_filter() {
        let payload = create_test_payload(
            "query { streams(name_not_ends_with_nocase: \"test\") { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name_: {_not: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_unsupported_contains_any_filter() {
        let payload = create_test_payload(
            "query { streams(tags_containsAny: [\"tag1\", \"tag2\"]) { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::UnsupportedFilter(filter)) => {
                assert_eq!(filter, "tags_containsAny");
            }
            _ => panic!("Expected UnsupportedFilter error"),
        }
    }

    #[test]
    fn test_unsupported_contains_all_filter() {
        let payload = create_test_payload(
            "query { streams(tags_containsAll: [\"tag1\", \"tag2\"]) { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::UnsupportedFilter(filter)) => {
                assert_eq!(filter, "tags_containsAll");
            }
            _ => panic!("Expected UnsupportedFilter error"),
        }
    }

    #[test]
    fn test_multiple_filters() {
        let payload = create_test_payload(
            "query { streams(name_contains: \"test\", amount_gt: 100, status: \"active\") { id name amount status } }"
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        // Check for all filter fragments regardless of order
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(query.contains("name: {_ilike: \"%test%\"}"));
        assert!(query.contains("amount: {_gt: 100}"));
        assert!(query.contains("status: {_eq: \"active\"}"));
        // Also check the selection set
        assert!(query.contains("id name amount status"));
        // And the entity name
        assert!(query.contains("Stream"));
    }

    #[test]
    fn test_non_stream_entity() {
        let payload = create_test_payload("query { users(name_contains: \"john\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  User(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%john%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_pagination_parameters() {
        let payload = create_test_payload("query { streams(first: 5, skip: 10) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 5, offset: 10, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_order_parameters() {
        let payload = create_test_payload(
            "query { streams(orderBy: name, orderDirection: desc) { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_complex_selection_set() {
        let payload =
            create_test_payload("query { streams { id name amount status { id name } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}}) {\n    id name amount status { id name }\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_missing_query_field() {
        let payload = json!({});
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::MissingField(field)) => {
                assert_eq!(field, "query");
            }
            _ => panic!("Expected MissingField error"),
        }
    }

    #[test]
    fn test_invalid_query_format() {
        let payload = json!({
            "query": 123
        });
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::InvalidQueryFormat) => {}
            _ => panic!("Expected InvalidQueryFormat error"),
        }
    }

    #[test]
    fn test_singularize_and_capitalize() {
        assert_eq!(singularize_and_capitalize("streams"), "Stream");
        assert_eq!(singularize_and_capitalize("users"), "User");
        assert_eq!(singularize_and_capitalize("stream"), "Stream");
        assert_eq!(singularize_and_capitalize("user"), "User");
    }

    #[test]
    fn test_basic_collection_query_no_chain_id() {
        let payload = create_test_payload("query { streams(first: 10, skip: 0) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 10, offset: 0) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_single_entity_query_no_chain_id() {
        let payload = create_test_payload("query { stream(id: \"123\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  stream_by_pk(id: \"123\") {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_equality_filter_no_chain_id() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_non_stream_entity_no_chain_id() {
        let payload = create_test_payload("query { users(name_contains: \"john\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  User(where: {name: {_ilike: \"%john%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_different_chain_id() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("5")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"5\"}, name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_where_clause_with_multiple_filters() {
        let payload = create_test_payload(
            "query { streams(where: {alias_contains: \"113\", chainId: \"1\"}) { id alias } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that both filters are included
        assert!(query.contains("alias: {_ilike: \"%113%\"}"));
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(query.contains("Stream"));
    }

    #[test]
    fn test_where_clause_single_filter() {
        let payload =
            create_test_payload("query { streams(where: {alias_contains: \"113\"}) { id alias } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that the filter is included
        assert!(query.contains("alias: {_ilike: \"%113%\"}"));
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(query.contains("Stream"));
    }
}
