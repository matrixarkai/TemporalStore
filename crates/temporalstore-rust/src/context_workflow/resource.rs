// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub fn parse_context_resource(request: ContextResourceParseRequest) -> ContextResourceParseReport {
    let resource_type =
        infer_context_resource_type(&request.raw_uri, request.resource_type.as_deref());
    let import_kind = context_resource_import_kind(&request.raw_uri, &resource_type);
    let owner_scope = if request.owner_scope.trim().is_empty() {
        "user".to_string()
    } else {
        request.owner_scope.trim().to_string()
    };
    let scope = context_scope_descriptor(&owner_scope);
    let parser_name = if request.parser_name.trim().is_empty() {
        default_resource_parser_name()
    } else {
        request.parser_name.trim().to_string()
    };
    let max_chunk_chars = request.max_chunk_chars.max(1);
    let overlap_chars = request.overlap_chars.min(max_chunk_chars.saturating_sub(1));
    let payload_size_bytes = request.text.as_bytes().len();
    let max_inline_bytes = default_context_resource_max_inline_bytes();
    let inline_payload = payload_size_bytes <= max_inline_bytes;
    let external_object_uri = if inline_payload {
        String::new()
    } else {
        context_resource_object_store_uri(&request.raw_uri, payload_size_bytes)
    };
    let units = context_resource_units(&request.text, &resource_type, &request.raw_uri);
    let mut chunks = Vec::new();
    let mut parser_warnings = Vec::new();

    for (unit_index, mut unit) in units.into_iter().enumerate() {
        let text = unit.remove("text").unwrap_or_default();
        let heading_path = unit
            .get("heading_path")
            .map(|path| {
                path.split('/')
                    .filter(|part| !part.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let parent_source_ref = unit
            .get("parent_heading_slug")
            .filter(|slug| !slug.is_empty())
            .map(|slug| format!("{}#heading={slug}", request.raw_uri));
        for (split_index, piece) in
            split_context_resource_text(&text, max_chunk_chars, overlap_chars)
                .into_iter()
                .enumerate()
        {
            if piece.trim().is_empty() {
                continue;
            }
            let chunk_index = chunks.len();
            unit.insert("resource_type".to_string(), resource_type.clone());
            unit.insert(
                "import_kind".to_string(),
                context_resource_import_kind_name(import_kind).to_string(),
            );
            unit.insert("parser_name".to_string(), parser_name.clone());
            unit.insert(
                "parser_version".to_string(),
                default_resource_parser_version(),
            );
            unit.insert("owner_scope".to_string(), owner_scope.clone());
            unit.insert(
                "scope_layer".to_string(),
                context_scope_layer_name(&scope.layer).to_string(),
            );
            unit.insert("scope_owner_id".to_string(), scope.owner_id.clone());
            unit.insert(
                "shared_graph_scope".to_string(),
                scope.shared_graph_scope.clone(),
            );
            if !scope.producer_agent_id.is_empty() {
                unit.insert(
                    "producer_agent_id".to_string(),
                    scope.producer_agent_id.clone(),
                );
            }
            unit.insert("chunk_index".to_string(), chunk_index.to_string());
            unit.insert("unit_index".to_string(), unit_index.to_string());
            unit.insert("split_index".to_string(), split_index.to_string());
            unit.insert("raw_uri".to_string(), request.raw_uri.clone());
            unit.insert(
                "uri_scheme".to_string(),
                context_resource_uri_scheme(&request.raw_uri),
            );
            unit.insert(
                "resource_title".to_string(),
                context_resource_title(&request.raw_uri),
            );
            if let Some(extension) = context_resource_extension(&request.raw_uri) {
                unit.insert("source_extension".to_string(), extension);
            }
            unit.insert(
                "payload_size_bytes".to_string(),
                payload_size_bytes.to_string(),
            );
            unit.insert("max_inline_bytes".to_string(), max_inline_bytes.to_string());
            unit.insert("inline_payload".to_string(), inline_payload.to_string());
            if !external_object_uri.is_empty() {
                unit.insert(
                    "external_object_uri".to_string(),
                    external_object_uri.clone(),
                );
                unit.insert("storage_backend".to_string(), "objectstore".to_string());
                unit.insert(
                    "storage_value_mode".to_string(),
                    "object_ref_json".to_string(),
                );
            } else {
                unit.insert("storage_backend".to_string(), "temporalstore".to_string());
                unit.insert(
                    "storage_value_mode".to_string(),
                    "raw_body_utf8".to_string(),
                );
            }
            let source_ref = context_resource_source_ref(&request.raw_uri, &unit);
            unit.insert("source_ref".to_string(), source_ref.clone());
            let content_hash = stable_hash64(&format!("resource_content:{source_ref}:{piece}"));
            unit.insert("content_hash".to_string(), content_hash.to_string());
            let linked_refs = extract_markdown_link_refs(&piece);
            if !linked_refs.is_empty() {
                unit.insert("linked_refs".to_string(), linked_refs.join(","));
            }
            let chunk_kind = unit
                .get("code_language")
                .map(|_| "code")
                .unwrap_or("text")
                .to_string();
            unit.insert("chunk_kind".to_string(), chunk_kind);
            if piece.len() > max_chunk_chars {
                parser_warnings.push(format!(
                    "chunk {} exceeded max_chunk_chars after boundary adjustment",
                    chunk_index
                ));
            }
            let chunk_hash = request
                .chunk_hash_base
                .map(|base| base.saturating_add(chunk_index as u64))
                .unwrap_or_else(|| stable_hash64(&format!("resource_chunk:{source_ref}:{piece}")));
            let embedding_ref_hash = stable_hash64(&format!(
                "resource_chunk_embedding:{}:{chunk_hash}:{}",
                request.raw_uri, "mock-embedding-v1"
            ));
            chunks.push(ContextParsedResourceChunk {
                chunk_hash,
                content_hash,
                embedding_ref_hash,
                source_ref,
                parent_source_ref: parent_source_ref.clone(),
                heading_path: heading_path.clone(),
                token_estimate: estimate_tokens(&piece),
                text: piece,
                metadata: unit.clone(),
            });
        }
    }

    let total_tokens = chunks.iter().fold(0_u32, |total, chunk| {
        total.saturating_add(chunk.token_estimate)
    });
    let content_hash = stable_hash64(&format!(
        "resource_lifecycle:{}:{}:{}",
        request.raw_uri, request.version, request.text
    ));
    let watched = request.watch_interval_minutes > 0
        || matches!(import_kind, ContextResourceImportKind::WatchedResource);
    let version = if request.version.trim().is_empty() {
        format!("{content_hash:x}")
    } else {
        request.version.trim().to_string()
    };
    let lifecycle = ContextResourceLifecycleRecord {
        raw_uri: request.raw_uri.clone(),
        target_uri: context_resource_target_uri(&request.raw_uri),
        owner_scope,
        scope,
        parser_name,
        parser_version: default_resource_parser_version(),
        resource_type: resource_type.clone(),
        import_kind,
        action: if watched {
            ContextResourceLifecycleAction::Watch
        } else {
            ContextResourceLifecycleAction::Add
        },
        version,
        content_hash,
        stale: false,
        invalidates_version: String::new(),
        watched,
        watch_interval_minutes: request.watch_interval_minutes,
        next_refresh_after_ms: request.watch_interval_minutes.saturating_mul(60_000),
        deleted: false,
        chunk_count: chunks.len(),
        payload_size_bytes,
        max_inline_bytes,
        inline_payload,
        external_object_uri: external_object_uri.clone(),
    };
    ContextResourceParseReport {
        status: Status::ok(),
        raw_uri: request.raw_uri.clone(),
        resource_type,
        resource_hash: stable_hash64(&format!("resource:{}", request.raw_uri)),
        uri_scheme: context_resource_uri_scheme(&request.raw_uri),
        resource_title: context_resource_title(&request.raw_uri),
        embedding_model: "mock-embedding-v1".to_string(),
        lifecycle,
        source_refs: chunks
            .iter()
            .map(|chunk| chunk.source_ref.clone())
            .collect(),
        chunks,
        total_tokens,
        payload_size_bytes,
        max_inline_bytes,
        inline_payload,
        external_object_uri,
        parser_warnings,
    }
}

pub fn update_context_resource_lifecycle(
    mut resources: Vec<ContextResourceLifecycleRecord>,
    updates: Vec<ContextResourceLifecycleUpdate>,
) -> ContextResourceLifecycleReport {
    let mut by_uri = resources
        .iter()
        .enumerate()
        .map(|(index, resource)| (resource.raw_uri.clone(), index))
        .collect::<BTreeMap<_, _>>();
    for update in updates {
        let observed_at_ms = update.observed_at_ms;
        if let Some(index) = by_uri.get(&update.raw_uri).copied() {
            let resource = &mut resources[index];
            resource.action = update.action;
            if !update.owner_scope.trim().is_empty() {
                resource.owner_scope = update.owner_scope.trim().to_string();
                resource.scope = context_scope_descriptor(&resource.owner_scope);
            }
            if !update.version.trim().is_empty() && update.version != resource.version {
                resource.stale = true;
                resource.invalidates_version = resource.version.clone();
                resource.version = update.version.trim().to_string();
            }
            if update.watch_interval_minutes > 0 {
                resource.watched = true;
                resource.watch_interval_minutes = update.watch_interval_minutes;
            }
            if resource.watched {
                resource.next_refresh_after_ms = observed_at_ms
                    .saturating_add(resource.watch_interval_minutes.saturating_mul(60_000));
            }
            resource.deleted = update.action == ContextResourceLifecycleAction::Delete;
            if update.action == ContextResourceLifecycleAction::Refresh {
                resource.stale = false;
            }
        } else {
            let mut resource = ContextResourceLifecycleRecord {
                raw_uri: update.raw_uri.clone(),
                target_uri: context_resource_target_uri(&update.raw_uri),
                owner_scope: if update.owner_scope.trim().is_empty() {
                    "user".to_string()
                } else {
                    update.owner_scope.trim().to_string()
                },
                scope: context_scope_descriptor(&update.owner_scope),
                resource_type: infer_context_resource_type(&update.raw_uri, None),
                action: update.action,
                version: update.version,
                watched: update.watch_interval_minutes > 0,
                watch_interval_minutes: update.watch_interval_minutes,
                next_refresh_after_ms: observed_at_ms
                    .saturating_add(update.watch_interval_minutes.saturating_mul(60_000)),
                deleted: update.action == ContextResourceLifecycleAction::Delete,
                ..ContextResourceLifecycleRecord::default()
            };
            resource.import_kind =
                context_resource_import_kind(&resource.raw_uri, &resource.resource_type);
            by_uri.insert(resource.raw_uri.clone(), resources.len());
            resources.push(resource);
        }
    }
    context_resource_lifecycle_report(resources, 0)
}

pub(super) fn default_resource_max_chunk_chars() -> usize {
    1_400
}

pub(super) fn default_resource_overlap_chars() -> usize {
    120
}

fn context_resource_object_store_uri(raw_uri: &str, payload_size_bytes: usize) -> String {
    let resource_hash = stable_hash64(&format!("resource_object:{raw_uri}:{payload_size_bytes}"));
    format!("objectstore://matrixark/resources/{resource_hash:016x}.bin")
}

pub(super) fn default_resource_parser_name() -> String {
    "rust-context-resource-parser".to_string()
}

pub(super) fn default_resource_parser_version() -> String {
    "reference-compatible-v1".to_string()
}

fn infer_context_resource_type(raw_uri: &str, resource_type: Option<&str>) -> String {
    if let Some(kind) = resource_type {
        let kind = kind.trim().trim_start_matches('.').to_ascii_lowercase();
        if !kind.is_empty() {
            return kind;
        }
    }
    raw_uri
        .rsplit_once('.')
        .map(|(_, suffix)| suffix.trim().to_ascii_lowercase())
        .filter(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric()))
        .unwrap_or_else(|| "txt".to_string())
}

fn context_resource_uri_scheme(raw_uri: &str) -> String {
    raw_uri
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .unwrap_or_else(|| "file".to_string())
}

fn context_resource_target_uri(raw_uri: &str) -> String {
    if raw_uri.starts_with("baseline://resources/") {
        raw_uri.to_string()
    } else {
        format!("baseline://resources/{}", stable_hash64(raw_uri))
    }
}

fn context_resource_import_kind(raw_uri: &str, resource_type: &str) -> ContextResourceImportKind {
    let scheme = context_resource_uri_scheme(raw_uri);
    let lower_uri = raw_uri.to_ascii_lowercase();
    let resource_type = resource_type.to_ascii_lowercase();
    if lower_uri.contains("wiki") || scheme == "wiki" {
        ContextResourceImportKind::WikiDoc
    } else if matches!(scheme.as_str(), "git" | "ssh")
        || lower_uri.ends_with(".git")
        || lower_uri.starts_with("git@")
    {
        ContextResourceImportKind::GitRepo
    } else if matches!(
        resource_type.as_str(),
        "rs" | "py" | "native" | "cc" | "c" | "h" | "hpp" | "js" | "ts" | "go" | "java"
    ) || lower_uri.contains("/src/")
    {
        ContextResourceImportKind::CodeRepo
    } else if matches!(resource_type.as_str(), "pdf") {
        ContextResourceImportKind::Pdf
    } else if matches!(
        resource_type.as_str(),
        "doc" | "docx" | "ppt" | "pptx" | "xls" | "xlsx"
    ) {
        ContextResourceImportKind::Document
    } else if matches!(resource_type.as_str(), "skill") {
        ContextResourceImportKind::Skill
    } else if matches!(resource_type.as_str(), "md" | "markdown") {
        ContextResourceImportKind::Markdown
    } else if matches!(scheme.as_str(), "http" | "https") {
        ContextResourceImportKind::Url
    } else if lower_uri.contains("watch=") || lower_uri.contains("/watched/") {
        ContextResourceImportKind::WatchedResource
    } else {
        ContextResourceImportKind::Text
    }
}

fn context_resource_import_kind_name(kind: ContextResourceImportKind) -> &'static str {
    match kind {
        ContextResourceImportKind::Text => "text",
        ContextResourceImportKind::Markdown => "markdown",
        ContextResourceImportKind::Skill => "skill",
        ContextResourceImportKind::Url => "url",
        ContextResourceImportKind::GitRepo => "git_repo",
        ContextResourceImportKind::CodeRepo => "code_repo",
        ContextResourceImportKind::Pdf => "pdf",
        ContextResourceImportKind::Document => "document",
        ContextResourceImportKind::WikiDoc => "wiki_doc",
        ContextResourceImportKind::WatchedResource => "watched_resource",
    }
}

pub(super) fn context_resource_lifecycle_report(
    mut resources: Vec<ContextResourceLifecycleRecord>,
    now_ms: u64,
) -> ContextResourceLifecycleReport {
    resources.sort_by_key(|resource| resource.target_uri.clone());
    let watched_count = resources.iter().filter(|resource| resource.watched).count();
    let stale_count = resources.iter().filter(|resource| resource.stale).count();
    let deleted_count = resources.iter().filter(|resource| resource.deleted).count();
    let refresh_due_count = resources
        .iter()
        .filter(|resource| {
            resource.watched
                && resource.next_refresh_after_ms > 0
                && now_ms >= resource.next_refresh_after_ms
        })
        .count();
    let mut import_kinds = BTreeMap::new();
    for resource in &resources {
        *import_kinds
            .entry(context_resource_import_kind_name(resource.import_kind).to_string())
            .or_insert(0) += 1;
    }
    ContextResourceLifecycleReport {
        status: Status::ok(),
        resources,
        watched_count,
        stale_count,
        deleted_count,
        refresh_due_count,
        import_kinds,
    }
}

fn context_resource_title(raw_uri: &str) -> String {
    raw_uri
        .rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or(raw_uri)
        .split('#')
        .next()
        .unwrap_or(raw_uri)
        .to_string()
}

fn context_resource_extension(raw_uri: &str) -> Option<String> {
    context_resource_title(raw_uri)
        .rsplit_once('.')
        .map(|(_, suffix)| suffix.trim().to_ascii_lowercase())
        .filter(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric()))
}

fn context_resource_units(
    text: &str,
    resource_type: &str,
    raw_uri: &str,
) -> Vec<BTreeMap<String, String>> {
    if matches!(resource_type, "md" | "markdown" | "skill") {
        markdown_resource_units(text)
    } else {
        paragraph_resource_units(text, raw_uri)
    }
}

fn markdown_resource_units(text: &str) -> Vec<BTreeMap<String, String>> {
    let mut units = Vec::new();
    let mut current_heading = "document".to_string();
    let mut current_level = 0_usize;
    let mut current_path = vec!["document".to_string()];
    let mut current_heading_slug = "document".to_string();
    let mut parent_heading_slug = String::new();
    let mut buffer = Vec::new();
    let mut start_line = 1_usize;
    let mut heading_stack: Vec<(usize, String, String)> = Vec::new();
    let mut active_code_language: Option<String> = None;
    let mut section_code_language: Option<String> = None;

    fn flush(
        units: &mut Vec<BTreeMap<String, String>>,
        buffer: &mut Vec<String>,
        heading: &str,
        level: usize,
        path: &[String],
        heading_slug: &str,
        parent_heading_slug: &str,
        start_line: usize,
        end_line: usize,
        code_language: Option<&str>,
    ) {
        let content = buffer.join("\n").trim().to_string();
        if content.is_empty() {
            buffer.clear();
            return;
        }
        let mut unit = BTreeMap::new();
        unit.insert("text".to_string(), content);
        unit.insert("heading".to_string(), heading.to_string());
        unit.insert("heading_slug".to_string(), heading_slug.to_string());
        unit.insert("heading_level".to_string(), level.to_string());
        unit.insert("heading_path".to_string(), path.join("/"));
        unit.insert("line_start".to_string(), start_line.to_string());
        unit.insert("line_end".to_string(), end_line.max(start_line).to_string());
        if !parent_heading_slug.is_empty() {
            unit.insert(
                "parent_heading_slug".to_string(),
                parent_heading_slug.to_string(),
            );
        }
        if let Some(language) = code_language.filter(|language| !language.is_empty()) {
            unit.insert("code_language".to_string(), language.to_string());
        }
        units.push(unit);
        buffer.clear();
    }

    for (line_index, line) in text.lines().enumerate() {
        let line_number = line_index + 1;
        let trimmed = line.trim();
        if let Some(fence) = trimmed.strip_prefix("```") {
            let language = fence.trim();
            if active_code_language.is_some() {
                active_code_language = None;
            } else if !language.is_empty() {
                active_code_language = Some(language.to_string());
            } else {
                active_code_language = Some("plain".to_string());
            }
        }
        let hash_count = trimmed.chars().take_while(|ch| *ch == '#').count();
        let is_heading = (1..=6).contains(&hash_count)
            && trimmed.as_bytes().get(hash_count) == Some(&b' ')
            && trimmed.len() > hash_count + 1;
        if is_heading {
            flush(
                &mut units,
                &mut buffer,
                &current_heading,
                current_level,
                &current_path,
                &current_heading_slug,
                &parent_heading_slug,
                start_line,
                line_number.saturating_sub(1),
                section_code_language.as_deref(),
            );
            current_level = hash_count;
            current_heading = trimmed[hash_count..].trim().to_string();
            let current_slug = slugify_context_resource(&current_heading);
            while heading_stack
                .last()
                .map(|(level, _, _)| *level >= current_level)
                .unwrap_or(false)
            {
                heading_stack.pop();
            }
            parent_heading_slug = heading_stack
                .last()
                .map(|(_, _, slug)| slug.clone())
                .unwrap_or_default();
            heading_stack.push((current_level, current_heading.clone(), current_slug.clone()));
            current_path = heading_stack
                .iter()
                .map(|(_, heading, _)| slugify_context_resource(heading))
                .collect();
            current_heading_slug = current_slug;
            start_line = line_number;
            active_code_language = None;
            section_code_language = None;
            buffer.push(trimmed.to_string());
        } else {
            if let Some(language) = active_code_language.as_deref() {
                section_code_language.get_or_insert_with(|| language.to_string());
            }
            buffer.push(line.trim_end().to_string());
        }
    }
    let end_line = text.lines().count().max(1);
    flush(
        &mut units,
        &mut buffer,
        &current_heading,
        current_level,
        &current_path,
        &current_heading_slug,
        &parent_heading_slug,
        start_line,
        end_line,
        section_code_language.as_deref(),
    );
    if units.is_empty() && !text.trim().is_empty() {
        let mut unit = BTreeMap::new();
        unit.insert("text".to_string(), text.trim().to_string());
        unit.insert("heading".to_string(), "document".to_string());
        unit.insert("heading_slug".to_string(), "document".to_string());
        unit.insert("heading_level".to_string(), "0".to_string());
        unit.insert("heading_path".to_string(), "document".to_string());
        unit.insert("line_start".to_string(), "1".to_string());
        unit.insert("line_end".to_string(), end_line.to_string());
        units.push(unit);
    }
    units
}

fn paragraph_resource_units(text: &str, raw_uri: &str) -> Vec<BTreeMap<String, String>> {
    let mut units = Vec::new();
    for (index, paragraph) in split_paragraphs(text).into_iter().enumerate() {
        let mut unit = BTreeMap::new();
        let line_count = paragraph.lines().count().max(1);
        unit.insert("text".to_string(), paragraph);
        unit.insert("paragraph_index".to_string(), index.to_string());
        unit.insert(
            "section".to_string(),
            raw_uri.rsplit('/').next().unwrap_or("document").to_string(),
        );
        unit.insert("line_start".to_string(), "1".to_string());
        unit.insert("line_end".to_string(), line_count.to_string());
        units.push(unit);
    }
    if units.is_empty() && !text.trim().is_empty() {
        let mut unit = BTreeMap::new();
        unit.insert("text".to_string(), text.trim().to_string());
        unit.insert("paragraph_index".to_string(), "0".to_string());
        unit.insert(
            "section".to_string(),
            raw_uri.rsplit('/').next().unwrap_or("document").to_string(),
        );
        unit.insert("line_start".to_string(), "1".to_string());
        unit.insert(
            "line_end".to_string(),
            text.trim().lines().count().max(1).to_string(),
        );
        units.push(unit);
    }
    units
}

pub(super) fn split_paragraphs(text: &str) -> Vec<String> {
    let mut paragraphs = Vec::new();
    let mut current = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                paragraphs.push(current.join("\n").trim().to_string());
                current.clear();
            }
        } else {
            current.push(line.trim_end().to_string());
        }
    }
    if !current.is_empty() {
        paragraphs.push(current.join("\n").trim().to_string());
    }
    paragraphs
}

pub(super) fn extract_markdown_link_refs(text: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut remainder = text;
    while let Some(open_label) = remainder.find('[') {
        remainder = &remainder[open_label + 1..];
        let Some(close_label) = remainder.find("](") else {
            continue;
        };
        remainder = &remainder[close_label + 2..];
        let Some(close_url) = remainder.find(')') else {
            break;
        };
        let target = remainder[..close_url].trim();
        if !target.is_empty() {
            refs.push(
                target
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim_matches('`')
                    .to_string(),
            );
        }
        remainder = &remainder[close_url + 1..];
    }
    refs.sort();
    refs.dedup();
    refs
}

fn split_context_resource_text(
    text: &str,
    max_chunk_chars: usize,
    overlap_chars: usize,
) -> Vec<String> {
    let mut normalized = text.trim().to_string();
    while normalized.contains("\n\n\n") {
        normalized = normalized.replace("\n\n\n", "\n\n");
    }
    if normalized.len() <= max_chunk_chars {
        return vec![normalized];
    }
    let mut pieces = Vec::new();
    let mut start = 0;
    while start < normalized.len() {
        let mut end =
            floor_char_boundary(&normalized, (start + max_chunk_chars).min(normalized.len()));
        if end < normalized.len() {
            let window = &normalized[start..end];
            let paragraph_boundary = window.rfind("\n\n").map(|index| start + index);
            let sentence_boundary = window.rfind(". ").map(|index| start + index + 1);
            let boundary = paragraph_boundary
                .into_iter()
                .chain(sentence_boundary)
                .max();
            if let Some(boundary) = boundary {
                if boundary > start + (max_chunk_chars / 2) {
                    end = boundary;
                }
            }
        }
        let piece = normalized[start..end].trim().to_string();
        if !piece.is_empty() {
            pieces.push(piece);
        }
        if end >= normalized.len() {
            break;
        }
        start = ceil_char_boundary(
            &normalized,
            end.saturating_sub(overlap_chars).max(start + 1),
        );
    }
    pieces
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index < value.len() && !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn context_resource_source_ref(raw_uri: &str, metadata: &BTreeMap<String, String>) -> String {
    let mut suffix = if let Some(page) = metadata.get("page") {
        format!("page={page}")
    } else if let Some(heading_slug) = metadata.get("heading_slug") {
        format!("heading={heading_slug}")
    } else if let Some(paragraph_index) = metadata.get("paragraph_index") {
        format!("paragraph={paragraph_index}")
    } else {
        format!(
            "chunk={}",
            metadata
                .get("chunk_index")
                .map(String::as_str)
                .unwrap_or("0")
        )
    };
    if metadata
        .get("split_index")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
        > 0
    {
        suffix.push_str("&part=");
        suffix.push_str(
            metadata
                .get("split_index")
                .map(String::as_str)
                .unwrap_or("0"),
        );
    }
    format!("{raw_uri}#{suffix}")
}

pub(super) fn slugify_context_resource(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string().if_empty("section")
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}
