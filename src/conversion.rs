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
    // Handle both multi-line and single-line queries.
    // Strategy: scan the full string for 'fragment ' blocks and remove them from main.
    let mut fragments = String::new();
    let mut remaining = query.to_string();

    loop {
        if let Some(start_idx) = remaining.find("fragment ") {
            // Find the start of the fragment body '{'
            let after_start = &remaining[start_idx..];
            if let Some(open_idx_rel) = after_start.find('{') {
                let open_idx = start_idx + open_idx_rel;
                // Walk to the matching '}'
                let mut brace_count = 1;
                let mut pos = open_idx + 1;
                let chars: Vec<char> = remaining.chars().collect();
                while pos < chars.len() {
                    match chars[pos] {
                        '{' => brace_count += 1,
                        '}' => {
                            brace_count -= 1;
                            if brace_count == 0 {
                                // Capture the fragment text [start_idx..=pos]
                                let fragment_text: String = chars[start_idx..=pos].iter().collect();
                                let fragment_text = sanitize_fragment_arguments(&fragment_text);
                                if !fragments.is_empty() {
                                    fragments.push('\n');
                                }
                                fragments.push_str(fragment_text.trim());

                                // Remove it from remaining
                                let prefix: String = chars[..start_idx].iter().collect();
                                let suffix: String = chars[pos + 1..].iter().collect();
                                remaining = format!("{}{}", prefix.trim_end(), suffix);
                                break;
                            }
                        }
                        _ => {}
                    }
                    pos += 1;
                }
                // Continue loop to find next fragment in updated 'remaining'
                continue;
            } else {
                // 'fragment ' without body; stop scanning to avoid infinite loop
                break;
            }
        } else {
            break;
        }
    }

    let main_query = remaining.trim().to_string();
    Ok((fragments, main_query))
}

fn convert_main_query(main_query: &str, chain_id: Option<&str>) -> Result<String, ConversionError> {
    // Strip the outer query { } wrapper if present, including named operations like `query Name { ... }`
    let stripped_owned;
    let stripped_query = if main_query.trim().starts_with("query") {
        let content = main_query.trim();
        if let (Some(start_brace), Some(end_brace)) = (content.find('{'), content.rfind('}')) {
            stripped_owned = content[start_brace + 1..end_brace].to_string();
            &stripped_owned
        } else {
            main_query
        }
    } else if main_query.trim().starts_with('{') {
        // Already a selection body
        main_query
    } else {
        main_query
    };

    // Extract multiple entities from the main query
    let entities = extract_multiple_entities(stripped_query)?;

    let mut converted_entities = Vec::new();

    for (entity, params, selection) in entities {
        let entity_cap = singularize_and_capitalize(&entity);
        // Only include limit/offset if they are literals, not GraphQL variables (e.g., $first/$skip)
        let limit = match params.get("first").cloned() {
            Some(v) if v.trim_start().starts_with('$') => None,
            other => other,
        };
        let offset = match params.get("skip").cloned() {
            Some(v) if v.trim_start().starts_with('$') => None,
            other => other,
        };

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
        // Map orderBy/orderDirection to Hasura order_by
        if let Some(order_field) = params.get("orderBy") {
            let order_dir = params
                .get("orderDirection")
                .map(|s| s.as_str())
                .unwrap_or("asc");
            // Ignore order_by if the order field is a variable (e.g., $orderBy) to keep query valid
            if !order_field.trim_start().starts_with('$')
                && !order_dir.trim_start().starts_with('$')
            {
                params_vec.push(format!("order_by: {{{}: {}}}", order_field, order_dir));
            }
        }
        if !where_clause.is_empty() {
            println!("DEBUG: Original where_clause: '{}'", where_clause);
            // The where_clause already has the correct format, just use it directly
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
    let query_chars: Vec<char> = query.chars().collect();
    let mut current_pos = 0;

    println!("DEBUG: Parsing query: {}", query);

    // Skip opening brace if present
    while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
        current_pos += 1;
    }
    if current_pos < query_chars.len() && query_chars[current_pos] == '{' {
        println!("DEBUG: Found opening brace at position {}", current_pos);
        current_pos += 1;
    }

    while current_pos < query_chars.len() {
        // Skip whitespace and newlines
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        if current_pos >= query_chars.len() {
            break;
        }

        println!(
            "DEBUG: Looking for entity at position {}, char: '{}'",
            current_pos, query_chars[current_pos]
        );

        // Look for entity name (word characters) - only at top level
        let entity_start = current_pos;
        while current_pos < query_chars.len() && query_chars[current_pos].is_alphanumeric() {
            current_pos += 1;
        }

        if current_pos == entity_start {
            current_pos += 1;
            continue;
        }

        let entity_name = query_chars[entity_start..current_pos]
            .iter()
            .collect::<String>();
        println!("DEBUG: Found potential entity name: '{}'", entity_name);

        // Skip if this is not a valid entity name (too short or common words)
        if entity_name.len() < 2
            || [
                "id", "in", "on", "to", "of", "at", "by", "is", "it", "as", "or", "an", "if", "up",
                "do", "go", "no", "so", "we", "he", "me", "be", "my", "am", "us", "hi", "lo", "ok",
                "hi", "lo", "ok",
            ]
            .contains(&entity_name.as_str())
        {
            println!(
                "DEBUG: Skipping '{}' as it's not a valid entity name",
                entity_name
            );
            current_pos += 1;
            continue;
        }

        // Look for opening parenthesis or brace after entity name (with optional whitespace)
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        let mut params = HashMap::new();

        if current_pos < query_chars.len() && query_chars[current_pos] == '(' {
            println!("DEBUG: Found entity definition for '{}'", entity_name);

            // Found an entity definition with parameters, extract parameters
            let params_start = current_pos + 1;
            let mut paren_count = 1; // We're already inside the first parenthesis

            while current_pos < query_chars.len() {
                current_pos += 1;
                if current_pos >= query_chars.len() {
                    break;
                }

                match query_chars[current_pos] {
                    '(' => paren_count += 1,
                    ')' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if current_pos >= query_chars.len() {
                break;
            }

            let params_str = query_chars[params_start..current_pos]
                .iter()
                .collect::<String>();
            parse_graphql_params(&params_str, &mut params)?;

            // Advance past the closing parenthesis
            current_pos += 1;
        } else if current_pos < query_chars.len() && query_chars[current_pos] == '{' {
            println!(
                "DEBUG: Found entity definition for '{}' (no parameters)",
                entity_name
            );
            // Entity without parameters, continue to selection set
        } else {
            println!(
                "DEBUG: No opening parenthesis or brace after '{}', skipping",
                entity_name
            );
            // This is not an entity definition, skip
            current_pos += 1;
            continue;
        }

        // Look for opening brace for selection set
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        println!(
            "DEBUG: After params, at position {}, char: '{}'",
            current_pos,
            if current_pos < query_chars.len() {
                query_chars[current_pos]
            } else {
                '?'
            }
        );

        if current_pos >= query_chars.len() || query_chars[current_pos] != '{' {
            println!(
                "DEBUG: No opening brace for selection set after '{}', skipping",
                entity_name
            );
            // No selection set, skip this entity
            current_pos += 1;
            continue;
        }

        println!(
            "DEBUG: Found opening brace for selection set at position {}",
            current_pos
        );

        // Extract selection set
        let selection_start = current_pos + 1;
        let mut brace_count = 1; // We're already inside the first brace

        while current_pos < query_chars.len() {
            current_pos += 1;
            if current_pos >= query_chars.len() {
                break;
            }

            match query_chars[current_pos] {
                '{' => brace_count += 1,
                '}' => {
                    brace_count -= 1;
                    if brace_count == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }

        if current_pos >= query_chars.len() {
            break;
        }

        let raw_selection: String = query_chars[selection_start..current_pos]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();
        let sanitized = sanitize_selection_set(&raw_selection);
        let selection_set = format!("{{\n    {}\n  }}", sanitized);

        println!("DEBUG: Found entity: {}", entity_name);
        println!("DEBUG: Params for {}: {:?}", entity_name, params);
        println!("DEBUG: Selection for {}: {}", entity_name, selection_set);

        entities.push((entity_name, params, selection_set));
    }

    println!(
        "DEBUG: Found {} entities: {:?}",
        entities.len(),
        entities.iter().map(|(name, _, _)| name).collect::<Vec<_>>()
    );
    Ok(entities)
}

fn sanitize_selection_set(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string = false;

    while let Some(ch) = chars.next() {
        if ch == '"' {
            in_string = !in_string;
            output.push(ch);
            continue;
        }

        if !in_string && ch == '(' {
            // Remove balanced parentheses and their contents
            let mut depth: i32 = 1;
            let mut in_args_string = false;
            while let Some(nc) = chars.next() {
                if nc == '"' {
                    in_args_string = !in_args_string;
                    continue;
                }
                if !in_args_string {
                    if nc == '(' {
                        depth += 1;
                    } else if nc == ')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
            }
            // Do not push the parentheses or their content
            continue;
        }

        output.push(ch);
    }

    output
}

fn sanitize_fragment_arguments(fragment_text: &str) -> String {
    // Only sanitize the selection body after the fragment header
    // Find the first '{' and its matching '}' and strip args in between
    let mut chars: Vec<char> = fragment_text.chars().collect();
    let Some(open_idx) = chars.iter().position(|c| *c == '{') else {
        return fragment_text.to_string();
    };
    // Find matching closing brace
    let mut brace_count = 1i32;
    let mut pos = open_idx + 1;
    while pos < chars.len() {
        match chars[pos] {
            '{' => brace_count += 1,
            '}' => {
                brace_count -= 1;
                if brace_count == 0 {
                    break;
                }
            }
            _ => {}
        }
        pos += 1;
    }
    if pos >= chars.len() {
        return fragment_text.to_string();
    }
    let header: String = chars[..open_idx + 1].iter().collect();
    let body: String = chars[open_idx + 1..pos].iter().collect();
    let tail: String = chars[pos..].iter().collect();
    let sanitized_body = sanitize_selection_set(body.trim());
    format!("{}{}{}", header, sanitized_body, tail)
}

// Removed unused selection set helpers

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

    // Group filters by parent object to avoid duplicates
    let mut grouped_filters: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut basic_filters: HashMap<String, Vec<(String, String)>> = HashMap::new();

    for (key, value) in flat_filters {
        if key.contains('.') {
            // This is a nested filter (e.g., "user.name_starts_with")
            if let Some(dot_idx) = key.rfind('.') {
                let parent = &key[..dot_idx];
                let child_key = &key[dot_idx + 1..];

                grouped_filters
                    .entry(parent.to_string())
                    .or_insert_with(HashMap::new)
                    .insert(child_key.to_string(), value);
            }
        } else {
            // This is a basic filter - group by field name
            let field_name = if key.contains('_') {
                // Extract the base field name (e.g., "alias" from "alias_contains")
                if let Some(underscore_idx) = key.find('_') {
                    &key[..underscore_idx]
                } else {
                    &key
                }
            } else {
                &key
            };

            basic_filters
                .entry(field_name.to_string())
                .or_insert_with(Vec::new)
                .push((key, value));
        }
    }

    // Sort keys to ensure consistent order, with chainId first
    let mut sorted_keys: Vec<_> = basic_filters.keys().collect();
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

    // Add basic filters
    let mut and_conditions = Vec::new();
    for key in sorted_keys {
        let conditions = basic_filters.get(key).unwrap();
        if conditions.len() == 1 {
            // Single condition for this field
            let (k, v) = &conditions[0];
            let condition = convert_basic_filter_to_hasura_condition(&k, &v)?;
            where_conditions.push(condition);
        } else {
            // Multiple conditions for the same field - wrap in _and
            for (k, v) in conditions {
                let condition = convert_basic_filter_to_hasura_condition(&k, &v)?;
                and_conditions.push(format!("{{{}}}", condition));
            }
        }
    }
    if !and_conditions.is_empty() {
        where_conditions.push(format!("_and: [{}]", and_conditions.join(", ")));
    }

    // Add grouped nested filters
    for (parent, child_filters) in grouped_filters {
        let mut child_conditions = Vec::new();
        let mut child_and_conditions = Vec::new();

        // Group child filters by field name to handle duplicates
        let mut grouped_child_filters: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for (child_key, child_value) in child_filters {
            let field_name = if child_key.contains('_') {
                if let Some(underscore_idx) = child_key.find('_') {
                    &child_key[..underscore_idx]
                } else {
                    &child_key
                }
            } else {
                &child_key
            };

            grouped_child_filters
                .entry(field_name.to_string())
                .or_insert_with(Vec::new)
                .push((child_key, child_value));
        }

        for (_field_name, conditions) in grouped_child_filters {
            if conditions.len() == 1 {
                // Single condition for this field
                let (k, v) = &conditions[0];
                let condition = convert_basic_filter_to_hasura_condition(&k, &v)?;
                child_conditions.push(condition);
            } else {
                // Multiple conditions for the same field - wrap in _and
                for (k, v) in conditions {
                    let condition = convert_basic_filter_to_hasura_condition(&k, &v)?;
                    child_and_conditions.push(format!("{{{}}}", condition));
                }
            }
        }

        if !child_and_conditions.is_empty() {
            child_conditions.push(format!("_and: [{}]", child_and_conditions.join(", ")));
        }

        let nested_condition = format!("{}: {{{}}}", parent, child_conditions.join(", "));
        where_conditions.push(nested_condition);
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
        let field = &key[..key.len() - 23];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_ends_with_nocase") {
        let field = &key[..key.len() - 21];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_contains_nocase") {
        let field = &key[..key.len() - 20];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_starts_with_nocase") {
        let field = &key[..key.len() - 19];
        return Ok(format!(
            "{}: {{_ilike: \"{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_ends_with_nocase") {
        let field = &key[..key.len() - 17];
        return Ok(format!(
            "{}: {{_ilike: \"%{}\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_contains_nocase") {
        let field = &key[..key.len() - 16];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_starts_with") {
        let field = &key[..key.len() - 16];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_ends_with") {
        let field = &key[..key.len() - 14];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_contains") {
        let field = &key[..key.len() - 13];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}%\"}}}}",
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

// Removed unused nested filter helper

// Removed unused entity/params extractor

fn parse_graphql_params(
    params_str: &str,
    params: &mut HashMap<String, String>,
) -> Result<(), ConversionError> {
    let mut current_param = String::new();
    let mut brace_count = 0;
    let mut bracket_count = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for (byte_idx, ch) in params_str.char_indices() {
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
                '\n' | '\r' => {
                    // Handle newlines as parameter separators when at top level
                    if brace_count == 0 && bracket_count == 0 {
                        // Look ahead to see if next non-whitespace content is a parameter name (identifier:)
                        // Use byte_idx to slice the string correctly (char_indices gives us byte positions)
                        let next_byte_idx = byte_idx + ch.len_utf8();
                        let remaining = &params_str[next_byte_idx..];
                        let trimmed = remaining.trim_start();
                        
                        // Check if trimmed starts with identifier pattern followed by colon
                        // Pattern: [a-zA-Z_][a-zA-Z0-9_]*\s*:
                        let mut chars_iter = trimmed.chars();
                        if let Some(first) = chars_iter.next() {
                            if first.is_alphabetic() || first == '_' {
                                // Continue reading identifier
                                let mut is_param = true;
                                let mut found_colon = false;
                                for c in chars_iter {
                                    if c == ':' {
                                        found_colon = true;
                                        break;
                                    } else if c.is_alphanumeric() || c == '_' {
                                        continue;
                                    } else if c.is_whitespace() {
                                        continue;
                                    } else {
                                        is_param = false;
                                        break;
                                    }
                                }
                                
                                if is_param && found_colon {
                                    // This is a new parameter, finish current one
                                    if !current_param.trim().is_empty() {
                                        parse_single_param(&current_param, params)?;
                                        current_param.clear();
                                    }
                                    // Skip the newline, don't add it to current_param
                                    continue;
                                }
                            }
                        }
                        // Not a new parameter, preserve newline in value
                        current_param.push(ch);
                    } else {
                        // Inside braces/brackets, preserve newline
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

// Removed unused brace matching helper

fn singularize_and_capitalize(s: &str) -> String {
    // Improved singularization to cover common English plural forms used in schema entity names
    // First, handle irregulars explicitly
    let lower = s.to_lowercase();
    let irregulars: &[(&str, &str)] = &[("tranches", "tranche")];
    if let Some((_, singular_irregular)) = irregulars.iter().find(|(pl, _)| *pl == &lower) {
        let mut c = singular_irregular.chars();
        return match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        };
    }

    let singular: String = if s.ends_with("ies") && s.len() > 3 {
        // companies -> company
        format!("{}y", &s[..s.len() - 3])
    } else if s.ends_with("ches")
        || s.ends_with("shes")
        || s.ends_with("xes")
        || s.ends_with("zes")
        || s.ends_with("sses")
        || s.ends_with("oes")
        || s.ends_with("ses")
    {
        // batches -> batch, boxes -> box, addresses -> address, heroes -> hero, users -> user (via 'ses')
        s[..s.len() - 2].to_string()
    } else if s.ends_with('s') && s.len() > 1 {
        // Default: drop trailing 's'
        s[..s.len() - 1].to_string()
    } else {
        s.to_string()
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_not_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_contains_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_contains_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test%\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_starts_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_ends_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(order_by: {name: desc}, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_order_by_with_skip_and_where() {
        let payload = create_test_payload(
            "query { streams(orderBy: alias, skip: 10, where: {alias_contains: \"113\"}) { alias asset { address } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(offset: 10, order_by: {alias: asc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%113%\"}}) {\n    alias asset { address }\n  }\n}"
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

    #[test]
    fn test_named_query_with_fragments_after_operation() {
        let payload = create_test_payload(
            "query GetActions { actions { ...ActionFragment } }\nfragment ContractFragment on Contract { id address category version }\nfragment ActionFragment on Action { id chainId stream { id } category hash block timestamp from addressA addressB amountA amountB contract { ...ContractFragment } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        // Fragments should be preserved and appear in the final query
        assert!(query.contains("fragment ContractFragment on Contract"));
        assert!(query.contains("fragment ActionFragment on Action"));
        // The converted main query should target Action with chainId filter
        assert!(query.contains("Action("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        // The selection should still reference the fragment
        assert!(query.contains("...ActionFragment"));
    }

    #[test]
    fn test_single_line_query_with_fragments() {
        let payload = create_test_payload(
            "query GetActions { actions { ...ActionFragment } } fragment ContractFragment on Contract { id address category version } fragment ActionFragment on Action { id chainId stream { id } category hash block timestamp from addressA addressB amountA amountB contract { ...ContractFragment } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        assert!(query.contains("fragment ContractFragment on Contract"));
        assert!(query.contains("fragment ActionFragment on Action"));
        assert!(query.contains("Action("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        assert!(query.contains("...ActionFragment"));
    }

    #[test]
    fn test_batches_pluralization_with_fragment() {
        let payload = create_test_payload(
            "query GetBatches { batches { ...BatchFragment } } fragment BatchFragment on Batch { id label size }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        // Should singularize to Batch and include chainId where
        assert!(query.contains("fragment BatchFragment on Batch"));
        assert!(query.contains("Batch("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        assert!(query.contains("...BatchFragment"));
    }

    #[test]
    fn test_tranches_pluralization_with_fragment() {
        let payload = create_test_payload(
            "query GetTranches { tranches { ...TrancheFragment } } fragment TrancheFragment on Tranche { id position amount timestamp endTime startTime startAmount endAmount }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        // Should singularize to Tranche and include chainId where
        assert!(query.contains("fragment TrancheFragment on Tranche"));
        assert!(query.contains("Tranche("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        assert!(query.contains("...TrancheFragment"));
    }

    #[test]
    fn test_boolean_filter_in_where_clause() {
        // Test case for boolean filters in where clause (e.g., isOpen: true)
        // This should be converted to isOpen: { _eq: true } format
        let payload = create_test_payload(
            "query Trades { trades(first: 10000, where: { isOpen: true }) { id trader isOpen } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        println!("Converted query: {}", query);
        
        // Check that the boolean filter is properly converted to Hasura format
        assert!(
            query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query, got: {}",
            query
        );
        // Check that Trade entity is used (singularized from trades)
        assert!(query.contains("Trade("));
        // Check that chainId is added when provided
        assert!(query.contains("chainId: {_eq: \"1\"}"));
    }

    #[test]
    fn test_boolean_filter_multiline_query_format() {
        // Test case matching the exact failing query format with multiline structure
        // This test reproduces the bug where boolean filters in where clauses are not
        // properly converted to Hasura format when parameters are separated by newlines.
        // Expected error: "expected an object for type 'Boolean_comparison_exp', but found a boolean"
        //
        // Note: This bug specifically affects the DEFAULT case (no suffix) which should use _eq.
        // Boolean operators with explicit suffixes already work correctly:
        // - _neq (via _not suffix): isOpen_not: false → isOpen: {_neq: false} ✓ Works
        // - _in: isOpen_in: [true, false] → isOpen: {_in: [true, false]} ✓ Works  
        // - _nin: isOpen_not_in: [true] → isOpen: {_nin: [true]} ✓ Works
        // - _eq (default, no suffix): isOpen: true → isOpen: {_eq: true} ✗ BUG: Affected
        //
        // Note: Operators like _gt, _lt, _gte, _lte, _ilike, _contains don't apply to booleans
        // in Hasura (they're for numeric/string fields). For booleans, only _eq, _neq, _in, _nin are valid.
        let query = r#"query Trades {
                                        trades(
                                            first: 10000
                                            where: {
                                            isOpen: true
                                            }
                                        ) {
                                            id
                                            trader
                                            isOpen
                                        }
                                        }"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);
        
        // Check that the boolean filter is properly converted to Hasura format
        // The incorrect format "isOpen: true" would cause Hyperindex to reject the query
        assert!(
            converted_query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query.\n\
             The incorrect format 'isOpen: true' would cause Hyperindex error:\n\
             'expected an object for type Boolean_comparison_exp, but found a boolean'.\n\
             Converted query: {}",
            converted_query
        );
        // Check that Trade entity is used (singularized from trades)
        assert!(converted_query.contains("Trade("));
    }

    #[test]
    fn test_boolean_filter_not_operator_multiline() {
        // Test boolean _neq operator (via _not suffix) in multiline format
        let query = r#"query {
  trades(
    where: {
      isOpen_not: false
    }
  ) {
    id
    isOpen
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);
        
        // Check that _neq is properly formatted (this should work since it has a suffix)
        assert!(
            converted_query.contains("isOpen: {_neq: false}"),
            "Expected isOpen: {{_neq: false}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_boolean_filter_in_operator_multiline() {
        // Test boolean _in operator in multiline format
        let query = r#"query {
                                trades(
                                    where: {
                                    isOpen_in: [true, false]
                                    }
                                ) {
                                    id
                                    isOpen
                                }
                                }"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);
        
        // Check that _in is properly formatted (this should work since it has a suffix)
        assert!(
            converted_query.contains("isOpen: {_in: [true, false]}"),
            "Expected isOpen: {{_in: [true, false]}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_boolean_filter_false_in_where_clause() {
        // Test case for boolean false filters in where clause
        let payload = create_test_payload(
            "query { streams(where: { isOpen: false }) { id isOpen } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        println!("Converted query: {}", query);
        
        // Check that the boolean filter is properly converted to Hasura format
        assert!(
            query.contains("isOpen: {_eq: false}"),
            "Expected isOpen: {{_eq: false}} in converted query, got: {}",
            query
        );
    }

    #[test]
    fn test_boolean_filter_with_other_filters() {
        // Test case for boolean filter combined with other filters
        let payload = create_test_payload(
            "query { trades(where: { isOpen: true, trader: \"0x123\" }) { id trader isOpen } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result["query"].as_str().unwrap();
        println!("Converted query: {}", query);
        
        // Check that both filters are properly converted
        assert!(
            query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query"
        );
        assert!(
            query.contains("trader: {_eq: \"0x123\"}"),
            "Expected trader: {{_eq: \"0x123\"}} in converted query"
        );
    }

    #[test]
    fn test_numeric_operators_multiline_format() {
        // Test that numeric operators (_gt, _gte, _lt, _lte) work in multiline format
        // This verifies that operators with suffixes are handled correctly
        let query = r#"query {
  streams(
    where: {
      amount_gt: 100
      amount_lte: 1000
    }
  ) {
    id
    amount
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);
        
        // Check that both operators are properly converted
        assert!(
            converted_query.contains("amount: {_gt: 100}"),
            "Expected amount: {{_gt: 100}} in converted query, got: {}",
            converted_query
        );
        assert!(
            converted_query.contains("amount: {_lte: 1000}"),
            "Expected amount: {{_lte: 1000}} in converted query, got: {}",
            converted_query
        );
    }

}
