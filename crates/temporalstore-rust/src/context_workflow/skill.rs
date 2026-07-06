use super::resource::{
    default_resource_max_chunk_chars, default_resource_overlap_chars, default_resource_parser_name,
    extract_markdown_link_refs, parse_context_resource, slugify_context_resource, split_paragraphs,
};
use super::*;

pub fn parse_context_skill_markdown(
    raw_uri: impl Into<String>,
    text: impl Into<String>,
) -> ContextSkillParseReport {
    let raw_uri = raw_uri.into();
    let text = text.into();
    let front_matter = parse_skill_front_matter(&text);
    let skill_name = front_matter
        .get("name")
        .cloned()
        .unwrap_or_else(|| infer_skill_name_from_uri(&raw_uri));
    let description = front_matter
        .get("description")
        .cloned()
        .unwrap_or_else(|| first_markdown_paragraph(&text));
    let tag_refs =
        parse_skill_front_matter_list(&front_matter, &["tags", "tag", "categories", "category"]);
    let allowed_tools = parse_skill_front_matter_list(
        &front_matter,
        &["allowed_tools", "allowed_tool", "tools", "tooling"],
    );
    let triggers =
        parse_skill_front_matter_list(&front_matter, &["triggers", "trigger", "activation"]);
    let model_refs =
        parse_skill_front_matter_list(&front_matter, &["models", "model", "providers", "provider"]);
    let owner_scope = front_matter
        .get("owner_scope")
        .or_else(|| front_matter.get("owner"))
        .cloned()
        .unwrap_or_else(|| "user".to_string());
    let scope = context_scope_descriptor(&owner_scope);
    let enabled = parse_skill_enabled(&front_matter);
    let precedence = parse_skill_precedence(
        front_matter
            .get("precedence")
            .or_else(|| front_matter.get("priority"))
            .map(String::as_str),
    );
    let tool_refs = parse_skill_section_items(&text, &["tools", "tooling", "commands"], true);
    let instruction_refs = parse_skill_section_items(
        &text,
        &["instructions", "workflow", "steps", "when to use"],
        false,
    );
    let resource_refs = parse_skill_section_items(&text, &["resources", "references"], true);
    let example_refs = parse_skill_section_items(&text, &["examples"], false);
    let resource = parse_context_resource(ContextResourceParseRequest {
        raw_uri: raw_uri.clone(),
        resource_type: Some("skill".to_string()),
        text,
        max_chunk_chars: default_resource_max_chunk_chars(),
        overlap_chars: default_resource_overlap_chars(),
        chunk_hash_base: None,
        owner_scope: owner_scope.clone(),
        version: front_matter
            .get("version")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        watch_interval_minutes: 0,
        parser_name: default_resource_parser_name(),
    });
    let capability_refs = resource
        .chunks
        .iter()
        .filter_map(|chunk| chunk.metadata.get("heading_slug"))
        .filter(|slug| {
            matches!(
                slug.as_str(),
                "when-to-use"
                    | "tools"
                    | "instructions"
                    | "resources"
                    | "references"
                    | "examples"
                    | "capabilities"
            )
        })
        .cloned()
        .collect();
    ContextSkillParseReport {
        status: Status::ok(),
        skill_name,
        description,
        source_ref: raw_uri,
        version: front_matter
            .get("version")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        owner_scope,
        scope,
        enabled,
        precedence,
        front_matter,
        tag_refs,
        capability_refs,
        allowed_tools,
        triggers,
        model_refs,
        tool_refs,
        instruction_refs,
        resource_refs,
        example_refs,
        parser_warnings: resource.parser_warnings.clone(),
        resource,
    }
}

pub fn context_skill_registry_from_parsed(
    skills: &[ContextSkillParseReport],
    updated_at_ms: u64,
) -> ContextSkillRegistryReport {
    let entries = skills
        .iter()
        .map(|skill| ContextSkillRegistryEntry {
            skill_name: skill.skill_name.clone(),
            source_ref: skill.source_ref.clone(),
            description: skill.description.clone(),
            version: skill.version.clone(),
            owner_scope: skill.owner_scope.clone(),
            scope: skill.scope.clone(),
            enabled: skill.enabled,
            precedence: skill.precedence,
            triggers: normalized_unique_strings(skill.triggers.clone()),
            allowed_tools: normalized_unique_strings(skill.allowed_tools.clone()),
            tag_refs: normalized_unique_strings(skill.tag_refs.clone()),
            model_refs: normalized_unique_strings(skill.model_refs.clone()),
            updated_at_ms,
        })
        .collect::<Vec<_>>();
    context_skill_registry_report(entries, Vec::new())
}

pub fn update_context_skill_registry(
    mut entries: Vec<ContextSkillRegistryEntry>,
    updates: Vec<ContextSkillRegistryUpdate>,
) -> ContextSkillRegistryReport {
    let mut version_updates = Vec::new();
    for update in updates {
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.skill_name == update.skill_name)
        {
            if let Some(enabled) = update.enabled {
                entry.enabled = enabled;
            }
            if let Some(precedence) = update.precedence {
                entry.precedence = precedence;
            }
            if let Some(owner_scope) = update.owner_scope {
                entry.owner_scope = owner_scope;
                entry.scope = context_scope_descriptor(&entry.owner_scope);
            }
            if let Some(triggers) = update.triggers {
                entry.triggers = normalized_unique_strings(triggers);
            }
            if let Some(allowed_tools) = update.allowed_tools {
                entry.allowed_tools = normalized_unique_strings(allowed_tools);
            }
            if let Some(version) = update.version {
                if entry.version != version {
                    version_updates.push(format!(
                        "{}:{}->{}",
                        entry.skill_name, entry.version, version
                    ));
                    entry.version = version;
                }
            }
            if update.updated_at_ms > 0 {
                entry.updated_at_ms = update.updated_at_ms;
            }
        }
    }
    context_skill_registry_report(entries, version_updates)
}

pub fn select_context_skills_for_retrieval(
    request: ContextSkillSelectionRequest,
) -> ContextSkillSelectionReport {
    let query_terms = context_query_terms(&request.query);
    let requested_scope = context_scope_descriptor(&request.owner_scope);
    let owner_scope_filter_enabled = !request.owner_scope.trim().is_empty();
    let tool_name = request.tool_name.trim();
    let mut selected = Vec::new();
    let mut skipped_disabled = Vec::new();
    let mut skipped_owner_scope = Vec::new();
    let mut skipped_tool = Vec::new();
    let mut scope_resolution_order = Vec::new();
    let mut agent_producers = Vec::new();
    let mut shared_graph_scopes = BTreeSet::new();

    for entry in request.registry {
        if !entry.enabled && !request.include_disabled {
            skipped_disabled.push(entry.skill_name);
            continue;
        }
        let entry_scope = context_skill_entry_scope(&entry);
        if !request.allowed_scope_layers.is_empty()
            && !request
                .allowed_scope_layers
                .iter()
                .any(|layer| *layer == entry_scope.layer)
        {
            skipped_owner_scope.push(entry.skill_name);
            continue;
        }
        if owner_scope_filter_enabled && !context_scope_matches(&requested_scope, &entry_scope) {
            skipped_owner_scope.push(entry.skill_name);
            continue;
        }
        let allowed_tool_match = tool_name.is_empty()
            || entry.allowed_tools.is_empty()
            || entry
                .allowed_tools
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(tool_name));
        if !allowed_tool_match {
            skipped_tool.push(entry.skill_name);
            continue;
        }
        let matched_triggers = entry
            .triggers
            .iter()
            .filter(|trigger| {
                query_terms
                    .iter()
                    .any(|term| trigger.to_ascii_lowercase().contains(term))
                    || request
                        .query
                        .to_ascii_lowercase()
                        .contains(trigger.to_ascii_lowercase().as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let lexical_matches = query_terms
            .iter()
            .filter(|term| {
                entry
                    .skill_name
                    .to_ascii_lowercase()
                    .contains(term.as_str())
                    || entry
                        .description
                        .to_ascii_lowercase()
                        .contains(term.as_str())
                    || entry
                        .tag_refs
                        .iter()
                        .any(|tag| tag.to_ascii_lowercase().contains(term.as_str()))
            })
            .count() as i64;
        let scope_bonus = 60_i64.saturating_sub(i64::from(entry_scope.precedence_rank));
        let score = context_skill_precedence_weight(entry.precedence)
            + scope_bonus
            + (matched_triggers.len() as i64 * 25)
            + (lexical_matches * 5)
            + i64::from(allowed_tool_match);
        push_unique_string(
            &mut scope_resolution_order,
            context_scope_layer_name(entry_scope.layer).to_string(),
        );
        if !entry_scope.producer_agent_id.is_empty() {
            push_unique_string(&mut agent_producers, entry_scope.producer_agent_id.clone());
        }
        shared_graph_scopes.insert(entry_scope.shared_graph_scope.clone());
        selected.push(ContextSkillSelectionCandidate {
            skill_name: entry.skill_name,
            version: entry.version,
            owner_scope: entry.owner_scope,
            scope: entry_scope,
            precedence: entry.precedence,
            score,
            matched_triggers,
            allowed_tool_match,
            enabled: entry.enabled,
        });
    }
    selected.sort_by_key(|candidate| {
        (
            Reverse(candidate.score),
            Reverse(context_skill_precedence_weight(candidate.precedence)),
            candidate.scope.precedence_rank,
            candidate.skill_name.clone(),
        )
    });
    selected.truncate(request.limit.max(1));
    let status = if selected.is_empty() {
        Status::error(
            "context_skill_selection_empty",
            "no registry skills matched retrieval request",
        )
    } else {
        Status::ok()
    };
    ContextSkillSelectionReport {
        status,
        query: request.query,
        selected,
        skipped_disabled,
        skipped_owner_scope,
        skipped_tool,
        scope_resolution_order,
        agent_producers,
        shared_graph_scope_count: shared_graph_scopes.len(),
    }
}

fn parse_skill_front_matter(text: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    let mut lines = text.lines();
    if lines.next().map(str::trim) != Some("---") {
        return metadata;
    }
    let mut active_list_key: Option<String> = None;
    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            break;
        }
        if let Some(key) = active_list_key.clone() {
            if let Some(item) = trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
            {
                let value = item
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim_matches('`');
                if !value.is_empty() {
                    metadata
                        .entry(key)
                        .and_modify(|existing| {
                            if !existing.is_empty() {
                                existing.push(',');
                            }
                            existing.push_str(value);
                        })
                        .or_insert_with(|| value.to_string());
                    continue;
                }
            }
            if !line.starts_with(' ') && !line.starts_with('\t') {
                active_list_key = None;
            }
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if value.is_empty() {
                active_list_key = Some(key.clone());
                metadata.entry(key).or_default();
            } else {
                metadata.insert(key, value.to_string());
                active_list_key = None;
            }
        }
    }
    metadata
}

fn parse_skill_enabled(metadata: &BTreeMap<String, String>) -> bool {
    let raw = metadata
        .get("enabled")
        .or_else(|| metadata.get("disabled"))
        .or_else(|| metadata.get("status"))
        .map(|value| value.trim().to_ascii_lowercase());
    match raw.as_deref() {
        Some("false") | Some("0") | Some("no") | Some("disabled") | Some("disable") => false,
        Some("true") | Some("1") | Some("yes") | Some("enabled") | Some("active") => true,
        Some(value) if metadata.contains_key("disabled") => !matches!(value, "true" | "1" | "yes"),
        _ => true,
    }
}

fn parse_skill_precedence(raw: Option<&str>) -> ContextSkillPrecedence {
    match raw.unwrap_or_default().trim().to_ascii_lowercase().as_str() {
        "low" | "0" => ContextSkillPrecedence::Low,
        "high" | "2" => ContextSkillPrecedence::High,
        "critical" | "highest" | "3" => ContextSkillPrecedence::Critical,
        _ => ContextSkillPrecedence::Normal,
    }
}

fn parse_skill_front_matter_list(
    metadata: &BTreeMap<String, String>,
    keys: &[&str],
) -> Vec<String> {
    let mut values = Vec::new();
    for key in keys {
        let Some(raw) = metadata.get(*key) else {
            continue;
        };
        for item in raw
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
        {
            let value = item
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('`')
                .trim();
            if !value.is_empty() {
                values.push(value.to_string());
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

fn parse_skill_section_items(
    text: &str,
    section_slugs: &[&str],
    first_token_only: bool,
) -> Vec<String> {
    let wanted = section_slugs
        .iter()
        .map(|slug| slugify_context_resource(slug))
        .collect::<Vec<_>>();
    let mut active = false;
    let mut refs = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        let hash_count = trimmed.chars().take_while(|ch| *ch == '#').count();
        let is_heading = (1..=6).contains(&hash_count)
            && trimmed.as_bytes().get(hash_count) == Some(&b' ')
            && trimmed.len() > hash_count + 1;
        if is_heading {
            let heading = trimmed[hash_count..].trim();
            active = wanted
                .iter()
                .any(|wanted_slug| slugify_context_resource(heading) == *wanted_slug);
            continue;
        }
        if !active {
            continue;
        }
        if let Some(item) = parse_markdown_list_item(trimmed, first_token_only) {
            refs.push(item);
        }
    }
    refs.sort();
    refs.dedup();
    refs
}

fn parse_markdown_list_item(trimmed: &str, first_token_only: bool) -> Option<String> {
    let item = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| trimmed.strip_prefix("+ "))
        .or_else(|| {
            let (prefix, rest) = trimmed.split_once(". ")?;
            prefix.chars().all(|ch| ch.is_ascii_digit()).then_some(rest)
        })?
        .trim();
    (!item.is_empty()).then(|| {
        let linked_refs = extract_markdown_link_refs(item);
        let value = if first_token_only {
            linked_refs
                .first()
                .map(String::as_str)
                .unwrap_or_else(|| item.split_whitespace().next().unwrap_or(item))
        } else {
            item
        };
        value
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .trim_matches(|ch: char| matches!(ch, ',' | ';' | ':' | '.'))
            .to_string()
    })
}

fn infer_skill_name_from_uri(raw_uri: &str) -> String {
    raw_uri
        .rsplit('/')
        .find(|part| !part.is_empty() && *part != "SKILL.md")
        .unwrap_or("skill")
        .trim_end_matches(".md")
        .to_string()
}

fn normalized_unique_strings(mut values: Vec<String>) -> Vec<String> {
    values.iter_mut().for_each(|value| {
        *value = value.trim().to_string();
    });
    values.retain(|value| !value.is_empty());
    values.sort_by_key(|value| value.to_ascii_lowercase());
    values.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    values
}

fn context_skill_precedence_weight(precedence: ContextSkillPrecedence) -> i64 {
    match precedence {
        ContextSkillPrecedence::Low => 0,
        ContextSkillPrecedence::Normal => 100,
        ContextSkillPrecedence::High => 200,
        ContextSkillPrecedence::Critical => 300,
    }
}

fn context_skill_registry_report(
    mut entries: Vec<ContextSkillRegistryEntry>,
    version_updates: Vec<String>,
) -> ContextSkillRegistryReport {
    entries.sort_by_key(|entry| {
        (
            Reverse(context_skill_precedence_weight(entry.precedence)),
            entry.skill_name.clone(),
        )
    });
    let enabled_count = entries.iter().filter(|entry| entry.enabled).count();
    let disabled_count = entries.len().saturating_sub(enabled_count);
    let highest_precedence = entries
        .iter()
        .map(|entry| entry.precedence)
        .max()
        .unwrap_or_default();
    let mut scope_layers = BTreeMap::new();
    let mut shared_graph_scopes = BTreeSet::new();
    let mut producer_agents = BTreeSet::new();
    for entry in &entries {
        let scope = context_skill_entry_scope(entry);
        *scope_layers
            .entry(context_scope_layer_name(scope.layer).to_string())
            .or_insert(0) += 1;
        shared_graph_scopes.insert(scope.shared_graph_scope);
        if !scope.producer_agent_id.is_empty() {
            producer_agents.insert(scope.producer_agent_id);
        }
    }
    ContextSkillRegistryReport {
        status: Status::ok(),
        entries,
        enabled_count,
        disabled_count,
        highest_precedence,
        version_updates,
        scope_layers,
        shared_graph_scope_count: shared_graph_scopes.len(),
        producer_agent_count: producer_agents.len(),
    }
}

fn context_skill_entry_scope(entry: &ContextSkillRegistryEntry) -> ContextScopeDescriptor {
    if entry.owner_scope.trim().is_empty()
        || entry
            .scope
            .raw_scope
            .eq_ignore_ascii_case(entry.owner_scope.trim())
    {
        entry.scope.clone()
    } else {
        context_scope_descriptor(&entry.owner_scope)
    }
}

fn push_unique_string(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn first_markdown_paragraph(text: &str) -> String {
    split_paragraphs(
        &text
            .lines()
            .filter(|line| !line.trim().starts_with("---"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .into_iter()
    .find(|paragraph| !paragraph.trim_start().starts_with('#'))
    .unwrap_or_default()
}
