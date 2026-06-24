# MatrixArk Resource And Skill Backend Parity

Run ID: 20260623-parser-production
All OK: True
Comparison: passed

## What Was Tested

- Markdown, text, and PDF resource ingestion
- SKILL.md parsing into manifest and sections
- ResourceManifest, ResourceChunk, SkillManifest, SkillSection writes
- L0 summaries and embeddings for resources and skills
- ContextIndex entries for resource and skill filtering
- ResourceRegistry and SkillRegistry list APIs
- ContextPack retrieval for resource chunks and selected skill instructions
- Skill disable/update behavior
- Cross-user scope isolation

## cpp

- OK: True
- Storage prefix: matrixark:resource-skill-parity:cpp:20260623-parser-production
- Resource count: 3
- Skill count: 1
- Resource selected refs: 9
- Skill selected refs: 9
- Disabled skill selected refs: 6
- Cross-user selected refs: 0
- Record counts: {"context_child_ref": 6, "context_embedding": 21, "context_event": 4, "context_index": 13, "context_node": 8, "context_pack_audit": 2, "context_summary": 8, "context_summary_dirty": 11, "matrixark_audit_log": 8, "resource_chunk": 8, "resource_manifest": 3, "session_buffer_event": 4, "skill_manifest": 1, "skill_section": 2}
- Embedding types: event_text, resource_chunk, resource_l0, session_l0, skill_l0, skill_summary
- Index names: resource_type:md, resource_type:pdf, resource_type:skill, resource_type:txt, skill_name:context-debugger, skill_tool:matrixark_audit, skill_tool:matrixark_replay, skill_trigger:inspect_selected_refs, skill_trigger:replay_evidence, source_type:resource, source_type:skill

## rust

- OK: True
- Storage prefix: matrixark:resource-skill-parity:rust:20260623-parser-production
- Resource count: 3
- Skill count: 1
- Resource selected refs: 9
- Skill selected refs: 9
- Disabled skill selected refs: 6
- Cross-user selected refs: 0
- Record counts: {"context_child_ref": 6, "context_embedding": 21, "context_event": 4, "context_index": 13, "context_node": 8, "context_pack_audit": 2, "context_summary": 8, "context_summary_dirty": 11, "matrixark_audit_log": 8, "resource_chunk": 8, "resource_manifest": 3, "session_buffer_event": 4, "skill_manifest": 1, "skill_section": 2}
- Embedding types: event_text, resource_chunk, resource_l0, session_l0, skill_l0, skill_summary
- Index names: resource_type:md, resource_type:pdf, resource_type:skill, resource_type:txt, skill_name:context-debugger, skill_tool:matrixark_audit, skill_tool:matrixark_replay, skill_trigger:inspect_selected_refs, skill_trigger:replay_evidence, source_type:resource, source_type:skill

## C++ Vs Rust Comparison

{
  "checks": {
    "embedding_types_equal": true,
    "resource_ref_types_equal": true,
    "resource_registry_count_equal": true,
    "skill_ref_types_equal": true,
    "skill_registry_count_equal": true
  },
  "status": "passed"
}
