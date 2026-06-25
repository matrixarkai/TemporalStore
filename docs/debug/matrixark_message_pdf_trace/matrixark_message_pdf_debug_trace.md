# MatrixArk Message + PDF Debug Trace

This debug run ingests conversation messages and several PDF resources, then retrieves one ContextPack. It is meant for inspecting exactly what MatrixArk writes and reads during ingestion, extraction, chunking, summary generation, embedding storage, tree traversal, secondary-index filtering, packing, audit, and replay.

## Re-run

```bash
python3 tools/run_matrixark_message_pdf_debug_trace.py
```

## Configuration

- Event log: `/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/matrixark_message_pdf_debug_trace.jsonl`
- Embedding model: `matrixark-local-token-hash-v1`
- Embedding execution mode: `deterministic-token-hash`
- Query: `What is the current Project Aurora GPU approval, owner, budget cap, deadline, and runbook blocker?`
- Summary refresh: background interval `1000` ms, limit `64` dirty nodes per tick
- Node L1 policy: generate when child summaries, >=3 source events, or >=180 estimated source tokens

## Record Counts

|record_type|count|
|---|---|
|context_batch_commit|1|
|context_child_ref|6|
|context_embedding|81|
|context_entity|20|
|context_entity_update_audit|5|
|context_event|26|
|context_extraction_audit|1|
|context_index|329|
|context_node|7|
|context_pack_audit|1|
|context_segment|3|
|context_summary|29|
|context_summary_dirty|66|
|context_summary_refresh_audit|7|
|matrixark_audit_log|16|
|matrixark_metric|3|
|resource_chunk|3|
|resource_import_task|9|
|resource_manifest|3|
|resource_registry|3|
|session_buffer_event|11|

## Input Messages

|role|content|
|---|---|
|user|Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.|
|assistant|Recorded: Project Aurora GPU purchase is approved by Alice in finance, pending procurement owner assignment.|
|user|Bob will own procurement, and the budget cap is 42000 dollars for the initial GPU batch.|
|assistant|I will track Bob as procurement owner and the 42000 dollar cap for the initial batch.|
|user|The deadline is July 15, 2026, and the runbook says finance approval must be attached before vendor selection.|
|assistant|The active deadline is July 15, 2026. Vendor selection requires the finance approval attachment.|
|user|Correction: Alice raised the cap to 45000 dollars after the backup GPU quote came in.|
|assistant|Updated: the current Project Aurora GPU budget cap is 45000 dollars.|

## PDF Resources

|raw_uri|title|line_count|
|---|---|---|
|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf|Project Aurora GPU Approval Packet|5|
|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf|GPU Procurement Runbook|4|
|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf|Budget Update Memo|4|

## Resource Import Tasks

|status|raw_uri|resource_type|chunk_count|resource_fact_count|resource_entity_count|metrics|
|---|---|---|---|---|---|---|
|queued|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf|pdf|||||
|running|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf|pdf|||||
|completed|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf|pdf|1|7|7|{"chunk_count": 1, "cloud_bucket": "", "cloud_key": "", "dedupe_count": 0, "duration_ms": 205.382, "embedding_count":...|
|queued|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf|pdf|||||
|running|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf|pdf|||||
|completed|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf|pdf|1|3|3|{"chunk_count": 1, "cloud_bucket": "", "cloud_key": "", "dedupe_count": 0, "duration_ms": 19.877, "embedding_count": ...|
|queued|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf|pdf|||||
|running|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf|pdf|||||
|completed|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf|pdf|1|5|5|{"chunk_count": 1, "cloud_bucket": "", "cloud_key": "", "dedupe_count": 0, "duration_ms": 18.248, "embedding_count": ...|

## Resource Chunks

|chunk_hash|raw_uri|source_ref|token_estimate|metadata.unit_kind|metadata.content_hash|text|
|---|---|---|---|---|---|---|
|8736436273504687932|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|51|pdf_page|49199ad5bd94964c|Project Aurora GPU Approval Packet Decision: Alice approved the Project Aurora GPU purchase after finance review. Own...|
|6034139221235933872|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|43|pdf_page|7aaae94b56b51807|GPU Procurement Runbook Procedure: Attach finance approval before vendor selection. Procedure: Compare primary and ba...|
|4893379877725482254|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|48|pdf_page|87731a0bb7829d5c|Budget Update Memo Update: The backup GPU quote increased the cap from 42000 dollars to 45000 dollars. Current state:...|

## Extracted Events

|event_id_hash|node_path|internal_extraction.event_type|internal_extraction.entity_type|summary_text|source_ref|
|---|---|---|---|---|---|
|524936940655528425|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.||
|3110922328373977738|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||assistant: Recorded: Project Aurora GPU purchase is approved by Alice in finance, pending procurement owner assignment.||
|5587845906929271104|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||user: Bob will own procurement, and the budget cap is 42000 dollars for the initial GPU batch.||
|4010347062634094153|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||assistant: I will track Bob as procurement owner and the 42000 dollar cap for the initial batch.||
|420109978585177584|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||user: The deadline is July 15, 2026, and the runbook says finance approval must be attached before vendor selection.||
|6460926268341786696|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||assistant: The active deadline is July 15, 2026. Vendor selection requires the finance approval attachment.||
|3050751851654260645|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||user: Correction: Alice raised the cap to 45000 dollars after the backup GPU quote came in.||
|6665319736342895071|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|||assistant: Updated: the current Project Aurora GPU budget cap is 45000 dollars.||
|571365746382456544|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_decision|resource_decision|resource_decision: Alice approved the Project Aurora GPU purchase after finance review|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|2900456491093257987|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_owner|resource_owner|resource_owner: Bob owns procurement and vendor coordination|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|1596920217437410578|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_cost|resource_cost|resource_cost: Current approved cap is 45000 dollars|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|9047961491740927299|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_deadline|resource_deadline|resource_deadline: Purchase order must be ready by July 15, 2026|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|929939956861191542|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_policy|resource_policy|resource_policy: be ready by July 15, 2026|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|274150047248606686|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_approval|resource_approval|resource_approval: Packet|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|4265994536714805107|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_risk|resource_risk|resource_risk: Vendor selection is blocked if finance approval is not attached|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|8513196518652600321|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|||tool: Import PDF resource for MatrixArk parsing: Project Aurora GPU Approval Packet||
|2159848791115076643|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_troubleshooting_step|resource_troubleshooting|resource_troubleshooting_step: Procedure: Attach finance approval before vendor selection|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|
|4657147395257529645|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_approval|resource_approval|resource_approval: before vendor selection|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|
|5464066946068943028|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_procedure|resource_procedure|resource_procedure: Attach finance approval before vendor selection|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|
|443443440602181842|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|||tool: Import PDF resource for MatrixArk parsing: GPU Procurement Runbook||
|6698509590300807928|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_cost|resource_cost|resource_cost: Update Memo|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|5430002385288940542|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_policy|resource_policy|resource_policy: not be used for current-state answers|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|6999014925757708944|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_approval|resource_approval|resource_approval: r: Alice confirmed the updated cap|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|2824274976164423253|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_risk|resource_risk|resource_risk: 42000 dollars is historical and should not be used for current-state answers|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|4155085225937358975|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_procedure|resource_procedure|resource_procedure: ed the updated cap|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|1585344323533811142|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|||tool: Import PDF resource for MatrixArk parsing: Budget Update Memo||

## Extracted Entities

|entity_hash|node_path|entity_type|entity_name|operator|state|source_ref|
|---|---|---|---|---|---|---|
|1488030737650625042|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|current_plan|current_plan|LLM_MERGE|track Bob as procurement owner and the 42000 dollar cap for the initial batch||
|5205088207995267081|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|approval_state|the GPU purchase request for Project Aurora after reviewing the Q3 budget|LLM_MERGE|the GPU purchase request for Project Aurora after reviewing the Q3 budget||
|5708414255151575681|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|approval_state|by Alice in finance, pending procurement owner assignment|LLM_MERGE|by Alice in finance, pending procurement owner assignment||
|8967060400784335657|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|approval_state|must be attached before vendor selection|LLM_MERGE|must be attached before vendor selection||
|1722827731307680407|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|approval_state|attachment|LLM_MERGE|attachment||
|6763603293199773729|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_decision|decision:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Alice approved the Project ...|LATEST|resource_decision: Alice approved the Project Aurora GPU purchase after finance review. Source: Project Aurora GPU Ap...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|8832111546635263332|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_owner|owner:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Bob owns procurement and vendo...|LATEST|resource_owner: Bob owns procurement and vendor coordination. Source: Project Aurora GPU Approval Packet Decision: Al...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|203192128854999035|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_cost|cost:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Current approved cap is 45000 d...|LATEST|resource_cost: Current approved cap is 45000 dollars. Source: Project Aurora GPU Approval Packet Decision: Alice appr...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|1846258924606901354|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_deadline|deadline:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Purchase order must be read...|LATEST|resource_deadline: Purchase order must be ready by July 15, 2026. Source: Project Aurora GPU Approval Packet Decision...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|957438680062716470|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_policy|policy:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:be ready by July 15, 2026|LATEST|resource_policy: be ready by July 15, 2026. Source: Project Aurora GPU Approval Packet Decision: Alice approved the P...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|3959598143726660477|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_approval|approval:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Packet|LATEST|resource_approval: Packet. Source: Project Aurora GPU Approval Packet Decision: Alice approved the Project Aurora GPU...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|7448309444804846956|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_risk|risk:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Vendor selection is blocked if ...|LATEST|resource_risk: Vendor selection is blocked if finance approval is not attached. Source: Project Aurora GPU Approval P...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pd...|
|482208152304334466|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_troubleshooting|troubleshooting:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Procedure: Attach fi...|LATEST|resource_troubleshooting_step: Procedure: Attach finance approval before vendor selection. Source: GPU Procurement Ru...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|
|1138079515152565102|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_approval|approval:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:before vendor selection|LATEST|resource_approval: before vendor selection. Source: GPU Procurement Runbook Procedure: Attach finance approval before...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|
|2904433185475945916|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_procedure|procedure:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Attach finance approval be...|LATEST|resource_procedure: Attach finance approval before vendor selection. Source: GPU Procurement Runbook Procedure: Attac...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1|
|8287587184611689973|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_cost|cost:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:Update Memo|LATEST|resource_cost: Update Memo. Source: Budget Update Memo Update: The backup GPU quote increased the cap from 42000 doll...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|6918872119779279271|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_policy|policy:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:not be used for current-state...|LATEST|resource_policy: not be used for current-state answers. Source: Budget Update Memo Update: The backup GPU quote incre...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|62418527498755741|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_approval|approval:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:r: Alice confirmed the upda...|LATEST|resource_approval: r: Alice confirmed the updated cap. Source: Budget Update Memo Update: The backup GPU quote increa...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|9128041085176869255|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_risk|risk:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:42000 dollars is historical and...|LATEST|resource_risk: 42000 dollars is historical and should not be used for current-state answers. Source: Budget Update Me...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|
|2222864349714042728|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|resource_procedure|procedure:/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/f:ed the updated cap|LATEST|resource_procedure: ed the updated cap. Source: Budget Update Memo Update: The backup GPU quote increased the cap fro...|/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1|

## Summaries

|summary_type|summary_hash|node_path|summary_generation_policy.reason|summary_text|source_chunk_hashes|
|---|---|---|---|---|---|
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|batch_l0|3690166991097399202|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. assistan...||
|resource_l0|1065960116080248254|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]||resource: /root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval...|[8736436273504687932]|
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|resource_l0|3105537185769273475|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]||resource: /root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook....|[6034139221235933872]|
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|resource_l0|5896945476345582083|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]||resource: /root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_updat...|[4893379877725482254]|
|session_l0|8695652974415713980|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]||user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. user: Al...||
|node_l0||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session"]|has_child_summaries|tenant:tenant_codex / user:deeproute / session:debug-message-pdf-session :: user: Alice from finance approved the GPU...||
|node_l1||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session"]|has_child_summaries|Context node tenant:tenant_codex / user:deeproute / session:debug-message-pdf-session. Rich overview: user: Alice fro...||
|node_l0||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|event_count_threshold|tenant:tenant_codex / user:deeproute / session:debug-message-pdf-session / conversation:project_aurora :: user: Alice...||
|node_l1||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|event_count_threshold|Context node tenant:tenant_codex / user:deeproute / session:debug-message-pdf-session / conversation:project_aurora. ...||
|node_l0||["tenant:tenant_codex"]|has_child_summaries|tenant:tenant_codex :: user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing ...||
|node_l1||["tenant:tenant_codex"]|has_child_summaries|Context node tenant:tenant_codex. Rich overview: user: Alice from finance approved the GPU purchase request for Proje...||
|node_l0||["tenant:tenant_codex", "user:deeproute"]|has_child_summaries|tenant:tenant_codex / user:deeproute :: user: Alice from finance approved the GPU purchase request for Project Aurora...||
|node_l1||["tenant:tenant_codex", "user:deeproute"]|has_child_summaries|Context node tenant:tenant_codex / user:deeproute. Rich overview: user: Alice from finance approved the GPU purchase ...||
|node_l0||["tenant:tenant_codex", "user:deeproute", "resources"]|has_child_summaries|tenant:tenant_codex / user:deeproute / resources :: resource: /root/src/github-services/TemporalStore/docs/debug/matr...||
|node_l1||["tenant:tenant_codex", "user:deeproute", "resources"]|has_child_summaries|Context node tenant:tenant_codex / user:deeproute / resources. Rich overview: resource: /root/src/github-services/Tem...||
|node_l0||["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora"]|has_child_summaries|tenant:tenant_codex / user:deeproute / resources / project_aurora :: resource: /root/src/github-services/TemporalStor...||
|node_l1||["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora"]|has_child_summaries|Context node tenant:tenant_codex / user:deeproute / resources / project_aurora. Rich overview: resource: /root/src/gi...||
|node_l0||["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|event_count_threshold|tenant:tenant_codex / user:deeproute / resources / project_aurora / gpu_procurement :: GPU Procurement Runbook Proced...||
|node_l1||["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|event_count_threshold|Context node tenant:tenant_codex / user:deeproute / resources / project_aurora / gpu_procurement. Rich overview: GPU ...||

## Node L0/L1 Generation Policy

|node_path|generated_summary_types|l1_policy.generate_l1|l1_policy.reason|l1_policy.token_estimate|source_event_count|source_summary_count|
|---|---|---|---|---|---|---|
|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session"]|["node_l0", "node_l1"]|True|has_child_summaries|508|8|2|
|["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|["node_l0", "node_l1"]|True|event_count_threshold|205|8|0|
|["tenant:tenant_codex"]|["node_l0", "node_l1"]|True|has_child_summaries|1034|8|4|
|["tenant:tenant_codex", "user:deeproute"]|["node_l0", "node_l1"]|True|has_child_summaries|1034|8|4|
|["tenant:tenant_codex", "user:deeproute", "resources"]|["node_l0", "node_l1"]|True|has_child_summaries|731|8|2|
|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora"]|["node_l0", "node_l1"]|True|has_child_summaries|731|8|2|
|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|["node_l0", "node_l1"]|True|event_count_threshold|492|8|0|

## Embeddings

|embedding_type|ref_type|ref_hash|model|dim|preview|
|---|---|---|---|---|---|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.2, 0.4, 0.0, -0.2, 0.0]|
|event_text|event|524936940655528425|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.2, 0.4, 0.0, -0.2, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.05903, -0.17708, 0.4132, 0.0, -0.23611, 0.05903]|
|event_text|event|3110922328373977738|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.26726, 0.0, 0.26726, 0.0, -0.26726, 0.26726]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|5587845906929271104|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.2582, -0.2582, 0.2582, 0.2582, 0.0, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|4010347062634094153|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.22942, 0.22942, 0.0, 0.22942, 0.0, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|420109978585177584|matrixark-local-token-hash-v1|32|[0.0, -0.22942, -0.22942, -0.22942, 0.22942, 0.0, -0.45883, -0.22942]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|6460926268341786696|matrixark-local-token-hash-v1|32|[0.24254, 0.0, 0.0, 0.0, 0.0, 0.0, -0.24254, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|3050751851654260645|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.44721, 0.22361, 0.22361, 0.22361, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|6665319736342895071|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, 0.0, 0.31623, 0.31623, 0.0, -0.31623]|
|entity_state|entity|1488030737650625042|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.24254, 0.0, 0.0, 0.24254, 0.0, 0.0]|
|entity_state|entity|5205088207995267081|matrixark-local-token-hash-v1|32|[0.0, 0.24254, 0.0, -0.24254, 0.24254, 0.0, 0.0, 0.0]|
|entity_state|entity|5708414255151575681|matrixark-local-token-hash-v1|32|[0.0, 0.33333, 0.33333, 0.0, 0.0, 0.0, 0.0, 0.33333]|
|entity_state|entity|8967060400784335657|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.44721, 0.0, 0.44721, -0.44721, 0.0]|
|entity_state|entity|1722827731307680407|matrixark-local-token-hash-v1|32|[0.70711, 0.70711, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]|
|segment_text|segment|8549485618968706578|matrixark-local-token-hash-v1|32|[0.0, -0.07538, -0.07538, -0.30151, 0.15076, 0.0, -0.30151, 0.0]|
|segment_text|segment|1085561979845616970|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.3849, 0.0, 0.19245, 0.19245, 0.19245]|
|segment_text|segment|8132170898046088330|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.22361, 0.0, 0.22361, 0.22361, 0.0, 0.0]|
|batch_l0|summary|3690166991097399202|matrixark-local-token-hash-v1|32|[0.05083, -0.05083, -0.10167, -0.1525, 0.35583, 0.20333, -0.20333, 0.0]|
|resource_l0|summary|1065960116080248254|matrixark-local-token-hash-v1|32|[0.08839, -0.08839, -0.08839, -0.53033, 0.26516, 0.17678, -0.08839, 0.08839]|
|resource_chunk|resource_chunk|8736436273504687932|matrixark-local-token-hash-v1|32|[0.07474, -0.07474, -0.07474, -0.52321, 0.22423, 0.07474, -0.07474, 0.14949]|
|event_text|event|571365746382456544|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.08704, -0.43519, 0.26112, 0.0, -0.34815, 0.08704]|
|entity_state|entity|6763603293199773729|matrixark-local-token-hash-v1|32|[0.07538, -0.07538, -0.07538, -0.37689, 0.30151, 0.0, -0.15076, 0.07538]|
|event_text|event|2900456491093257987|matrixark-local-token-hash-v1|32|[0.0, 0.1, -0.2, -0.6, 0.2, 0.0, -0.3, 0.1]|
|entity_state|entity|8832111546635263332|matrixark-local-token-hash-v1|32|[0.0822, 0.0822, -0.2466, -0.6576, 0.1644, 0.0, -0.1644, 0.0822]|
|event_text|event|1596920217437410578|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.1, -0.4, 0.2, 0.1, -0.4, 0.1]|
|entity_state|entity|203192128854999035|matrixark-local-token-hash-v1|32|[0.08248, -0.08248, -0.08248, -0.32991, 0.16496, 0.24744, -0.32991, 0.08248]|
|event_text|event|9047961491740927299|matrixark-local-token-hash-v1|32|[0.0, -0.09492, -0.09492, -0.37966, 0.18983, -0.09492, -0.47458, 0.18983]|
|entity_state|entity|1846258924606901354|matrixark-local-token-hash-v1|32|[0.07559, -0.22678, -0.07559, -0.30237, 0.15119, -0.15119, -0.45356, 0.22678]|
|event_text|event|929939956861191542|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.19803, -0.39606, 0.19803, -0.09902, -0.49507, 0.19803]|
|entity_state|entity|957438680062716470|matrixark-local-token-hash-v1|32|[0.08639, -0.08639, -0.17277, -0.34555, 0.17277, -0.08639, -0.51832, 0.25916]|
|event_text|event|274150047248606686|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.10976, -0.43906, 0.21953, 0.0, -0.32929, 0.10976]|
|entity_state|entity|3959598143726660477|matrixark-local-token-hash-v1|32|[0.09759, -0.09759, -0.09759, -0.39036, 0.19518, 0.09759, -0.19518, 0.09759]|
|event_text|event|4265994536714805107|matrixark-local-token-hash-v1|32|[0.0, 0.0, -0.09206, -0.46029, 0.18412, 0.0, -0.27617, 0.09206]|
|entity_state|entity|7448309444804846956|matrixark-local-token-hash-v1|32|[0.08737, -0.08737, -0.08737, -0.43685, 0.17474, 0.0, -0.17474, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|8513196518652600321|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.5, 0.25, 0.0, -0.25, 0.0]|
|resource_l0|summary|3105537185769273475|matrixark-local-token-hash-v1|32|[0.16784, -0.08392, -0.25175, -0.58743, -0.16784, 0.41959, 0.08392, 0.0]|
|resource_chunk|resource_chunk|6034139221235933872|matrixark-local-token-hash-v1|32|[0.14037, -0.07019, -0.21056, -0.63168, -0.28075, 0.35093, 0.07019, 0.07019]|
|event_text|event|2159848791115076643|matrixark-local-token-hash-v1|32|[0.09091, 0.0, -0.27273, -0.54546, -0.36364, 0.36364, -0.09091, 0.0]|
|entity_state|entity|482208152304334466|matrixark-local-token-hash-v1|32|[0.15385, -0.07692, -0.15385, -0.46154, -0.30769, 0.30769, 0.07692, 0.0]|
|event_text|event|4657147395257529645|matrixark-local-token-hash-v1|32|[0.09667, 0.0, -0.29002, -0.58004, -0.29002, 0.3867, -0.09667, 0.0]|
|entity_state|entity|1138079515152565102|matrixark-local-token-hash-v1|32|[0.15962, -0.07981, -0.23943, -0.55866, -0.23943, 0.39904, 0.07981, 0.0]|
|event_text|event|5464066946068943028|matrixark-local-token-hash-v1|32|[0.09366, 0.0, -0.28098, -0.56195, -0.28098, 0.37463, -0.09366, 0.0]|
|entity_state|entity|2904433185475945916|matrixark-local-token-hash-v1|32|[0.24495, -0.08165, -0.24495, -0.4899, -0.3266, 0.3266, 0.08165, 0.0]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|443443440602181842|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.57735, 0.0, 0.0, -0.28868, 0.0]|
|resource_l0|summary|5896945476345582083|matrixark-local-token-hash-v1|32|[0.0, -0.26414, -0.08804, -0.35218, 0.08804, 0.26414, 0.17609, -0.17609]|
|resource_chunk|resource_chunk|4893379877725482254|matrixark-local-token-hash-v1|32|[0.0, -0.20702, -0.06901, -0.34503, 0.0, 0.20702, 0.13801, -0.06901]|
|event_text|event|6698509590300807928|matrixark-local-token-hash-v1|32|[-0.09853, -0.19707, -0.09853, -0.19707, 0.0, 0.19707, 0.0, -0.19707]|
|entity_state|entity|8287587184611689973|matrixark-local-token-hash-v1|32|[0.0, -0.25265, -0.08421, -0.16843, 0.0, 0.16843, 0.08421, -0.16843]|
|event_text|event|5430002385288940542|matrixark-local-token-hash-v1|32|[-0.09285, -0.18569, -0.18569, -0.18569, 0.0, 0.09285, -0.09285, -0.27854]|
|entity_state|entity|6918872119779279271|matrixark-local-token-hash-v1|32|[0.0, -0.23867, -0.15911, -0.15911, 0.0, -0.07956, -0.07956, -0.23867]|
|event_text|event|6999014925757708944|matrixark-local-token-hash-v1|32|[-0.08874, -0.17747, -0.08874, -0.17747, 0.0, 0.17747, 0.0, -0.26621]|
|entity_state|entity|62418527498755741|matrixark-local-token-hash-v1|32|[0.0, -0.21764, -0.07255, -0.1451, 0.0, 0.07255, 0.07255, -0.29019]|
|event_text|event|2824274976164423253|matrixark-local-token-hash-v1|32|[-0.08392, -0.25175, -0.16784, -0.16784, 0.0, 0.08392, -0.08392, -0.25175]|
|entity_state|entity|9128041085176869255|matrixark-local-token-hash-v1|32|[0.0, -0.37165, -0.22299, -0.14866, 0.0, -0.07433, -0.07433, -0.14866]|
|event_text|event|4155085225937358975|matrixark-local-token-hash-v1|32|[-0.09245, -0.1849, -0.09245, -0.1849, 0.0, 0.27735, 0.0, -0.27735]|
|entity_state|entity|2222864349714042728|matrixark-local-token-hash-v1|32|[0.0, -0.23643, -0.07881, -0.15762, -0.07881, 0.31524, 0.07881, -0.31524]|
|session_l0|summary|8695652974415713980|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.04994, -0.19975, 0.3995, 0.0, -0.24969, 0.04994]|
|event_text|event|1585344323533811142|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.35355, 0.0, 0.0, -0.35355, 0.0]|
|node_l0|node|3084181658660614334|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.25198, 0.50395, 0.0, -0.12599, 0.0]|
|node_l1|node|3084181658660614334|matrixark-local-token-hash-v1|32|[0.0, -0.02757, -0.02757, -0.16539, 0.38592, 0.02757, -0.27566, 0.02757]|
|node_l0|node|2100209595829882121|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.28571, 0.42857, 0.14286, -0.14286, 0.0]|
|node_l1|node|2100209595829882121|matrixark-local-token-hash-v1|32|[0.0, -0.03975, -0.11924, -0.23848, 0.35772, 0.23848, -0.15899, -0.07949]|
|node_l0|node|3263141514618168867|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.21953, 0.43906, -0.10976, -0.21953, 0.0]|
|node_l1|node|3263141514618168867|matrixark-local-token-hash-v1|32|[0.0, -0.02704, -0.02704, -0.16222, 0.35148, 0.05407, -0.27037, 0.02704]|
|node_l0|node|623184698193930698|matrixark-local-token-hash-v1|32|[0.0, 0.0, 0.0, -0.2325, 0.46499, 0.0, -0.2325, 0.0]|
|node_l1|node|623184698193930698|matrixark-local-token-hash-v1|32|[0.0, -0.02736, -0.02736, -0.16415, 0.38302, 0.02736, -0.27359, 0.02736]|
|node_l0|node|1257764480205296887|matrixark-local-token-hash-v1|32|[0.20851, -0.20851, 0.0, -0.41703, 0.20851, -0.20851, 0.20851, 0.0]|
|node_l1|node|1257764480205296887|matrixark-local-token-hash-v1|32|[0.0, -0.10432, -0.03477, -0.38251, 0.24341, 0.10432, -0.13909, -0.03477]|
|node_l0|node|5984959491336829337|matrixark-local-token-hash-v1|32|[0.20851, -0.20851, 0.0, -0.41703, 0.20851, 0.20851, 0.20851, 0.0]|
|node_l1|node|5984959491336829337|matrixark-local-token-hash-v1|32|[0.0, -0.10401, -0.03467, -0.38136, 0.24268, 0.13867, -0.13867, -0.03467]|
|node_l0|node|1737304210274426578|matrixark-local-token-hash-v1|32|[0.0, -0.19612, -0.19612, -0.58835, -0.19612, 0.39223, 0.19612, 0.0]|
|node_l1|node|1737304210274426578|matrixark-local-token-hash-v1|32|[-0.03505, -0.2103, -0.17525, -0.45565, -0.0701, 0.31545, -0.03505, -0.1402]|

## Secondary Indexes

|index_name|ref_type|ref_hash|chunk_hash|node_path|
|---|---|---|---|---|
|event_type:correction||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|classification:batch_memory||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|status:observed||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|source_type:message||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|entity_type:current_plan||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|entity_type:approval_state||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|segment_topic:approval_budget||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|segment_topic:correction||||["tenant:tenant_codex", "user:deeproute", "session:debug-message-pdf-session", "conversation:project_aurora"]|
|source_type:resource||||["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf||||["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|unit_kind:pdf_page|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:project|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:aurora|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:gpu|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approval|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:packet|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:decision|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:alice|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approved|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:purchase|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:finance|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:review|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:owner|resource_chunk|8736436273504687932|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource_fact|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|event_type:resource_decision|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_decision|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_fact|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|unit_kind:pdf_page|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:project|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:aurora|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:gpu|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approval|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:packet|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:decision|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:alice|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approved|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:purchase|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:finance|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:review|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:owner|resource_fact|571365746382456544|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource_fact|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|event_type:resource_owner|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_owner|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_fact|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|unit_kind:pdf_page|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:project|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:aurora|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:gpu|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approval|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:packet|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:decision|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:alice|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approved|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:purchase|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:finance|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:review|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:owner|resource_fact|2900456491093257987|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource_fact|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|event_type:resource_cost|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_cost|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_fact|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|unit_kind:pdf_page|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:project|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:aurora|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:gpu|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approval|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:packet|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:decision|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:alice|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approved|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:purchase|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:finance|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:review|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:owner|resource_fact|1596920217437410578|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource_fact|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|event_type:resource_deadline|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_deadline|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_fact|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|unit_kind:pdf_page|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:project|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:aurora|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:gpu|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approval|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:packet|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:decision|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:alice|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approved|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:purchase|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:finance|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:review|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:owner|resource_fact|9047961491740927299|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource_fact|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|event_type:resource_policy|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_policy|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_fact|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|unit_kind:pdf_page|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:project|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:aurora|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:gpu|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approval|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:packet|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:decision|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:alice|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:approved|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:purchase|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:finance|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:review|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|keyword:owner|resource_fact|929939956861191542|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|source_type:resource_fact|resource_fact|274150047248606686|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|event_type:resource_approval|resource_fact|274150047248606686|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_approval|resource_fact|274150047248606686|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|entity_type:resource_fact|resource_fact|274150047248606686|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|
|resource_type:pdf|resource_fact|274150047248606686|8736436273504687932|["tenant:tenant_codex", "user:deeproute", "resources", "project_aurora", "gpu_procurement"]|

## Retrieval Scan

```json
{
  "context_pack_id": "3047053284104680203",
  "dropped_refs": {
    "duplicate": 12,
    "estimated_tokens": {
      "duplicate": 584,
      "low_score": 0,
      "over_budget": 0,
      "raw_l2": 0,
      "stale": 0,
      "summary": 0
    },
    "low_score": 0,
    "over_budget": 0,
    "raw_l2": 0,
    "reason_descriptions": {
      "duplicate": "candidate duplicated local context or an already selected ref",
      "low_score": "candidate score was below the minimum packing threshold",
      "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
      "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
      "stale": "candidate was stale or superseded for the query policy",
      "summary": "summary text was dropped in favor of denser raw/evidence refs"
    },
    "refs": [
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_decision",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.809227,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 571365746382456544,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.80692,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_cost",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.804421,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 1596920217437410578,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.803316,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_owner",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.789166,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 2900456491093257987,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.791875,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_risk",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.784072,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 4265994536714805107,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.788054,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_policy",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.780112,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 929939956861191542,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.785084,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_deadline",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.774172,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 9047961491740927299,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.780629,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:correction",
          "event_type:resource_procedure",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.656496,
        "packing_score": 0.992372,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 4155085225937358975,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.692372,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 48,
        "token_estimate": 48,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:correction",
          "event_type:resource_policy",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.64993,
        "packing_score": 0.987448,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 5430002385288940542,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.687448,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 48,
        "token_estimate": 48,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:correction",
          "event_type:resource_risk",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.645433,
        "packing_score": 0.984075,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 2824274976164423253,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.684075,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 48,
        "token_estimate": 48,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:correction",
          "event_type:resource_cost",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.641837,
        "packing_score": 0.981378,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 6698509590300807928,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.681378,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 48,
        "token_estimate": 48,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:dialogue_batch",
          "event_type:resource_procedure",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.608063,
        "packing_score": 0.956047,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 5464066946068943028,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.656047,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 43,
        "token_estimate": 43,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:dialogue_batch",
          "event_type:resource_troubleshooting_step",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.597564,
        "packing_score": 0.948173,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 2159848791115076643,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.648173,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 43,
        "token_estimate": 43,
        "version_state": "current"
      }
    ],
    "stale": 0,
    "summary": 0
  },
  "quality_warnings": [],
  "query": "What is the current Project Aurora GPU approval, owner, budget cap, deadline, and runbook blocker?",
  "recall_policy": {
    "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
    "auxiliary_quota": 6,
    "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
    "secondary_index_filter": {
      "applied_before_embedding_scoring": true,
      "dropped_candidate_count": 18,
      "effective_mode": "all_groups",
      "enabled": true,
      "matched_candidate_count": 31,
      "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
      "required_groups": [
        [
          "classification:confirmation",
          "classification:resource_fact",
          "entity_type:approval_state",
          "entity_type:confirmation",
          "entity_type:resource_fact",
          "event_type:confirmation",
          "event_type:resource_approval_fact",
          "segment_topic:approval_budget",
          "source_type:resource",
          "source_type:resource_fact"
        ]
      ]
    },
    "time_decay": {
      "freshness_tolerance_ms": 86400000,
      "half_life_ms": 604800000
    },
    "tree_traversal": {
      "enabled": true,
      "fallback_reason": "",
      "fallback_to_flat": false,
      "max_children_scored_per_parent": 10000,
      "selected_leaf_count": 2,
      "selected_node_count": 7,
      "selected_path_count": 7,
      "summary_embeddings": [
        "node_l0",
        "node_l1"
      ],
      "top_k_per_layer": 8
    },
    "weights": {
      "business": 0.1,
      "time": 0.15
    }
  },
  "selected_refs": [
    {
      "business_score": 0.95,
      "context_class": "entity",
      "embedding_score": 0.470871,
      "entity_name": "the GPU purchase request for Project Aurora after reviewing the Q3 budget",
      "entity_type": "approval_state",
      "final_score": 0.801195,
      "keyword_score": 5,
      "matched_index_terms": [
        "entity_type:approval_state"
      ],
      "metadata": {},
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.741594,
      "packing_policy": "current_state",
      "packing_score": 1.0,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 5205088207995267081,
      "ref_type": "entity",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.801195,
      "selection_reason": "selected by tree path, secondary indexes, and entity state score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.35714285714285715,
      "text": "approval_state: the GPU purchase request for Project Aurora after reviewing the Q3 budget = the GPU purchase request for Project Aurora after reviewing the Q3 budget",
      "time_score": 1.0,
      "token_estimate": 25,
      "updated_at_ms": 1782415058953
    },
    {
      "business_score": 0.9,
      "context_class": "resource_fact",
      "embedding_score": 0.639306,
      "event_type": "resource_approval",
      "final_score": 0.849325,
      "keyword_score": 11,
      "matched_index_terms": [
        "classification:resource_fact",
        "event_type:confirmation",
        "event_type:resource_approval",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "source_type:resource",
        "source_type:resource_fact",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "resource_title": "Project Aurora GPU Approval Packet",
        "source": "debug_trace"
      },
      "node_hash": 1737304210274426578,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "resources",
        "project_aurora",
        "gpu_procurement"
      ],
      "node_score": 0.732478,
      "origin_score": 0.812433,
      "packing_policy": "current_state",
      "packing_score": 1.0,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 274150047248606686,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.849325,
      "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
      "source_chunk_hash": 8736436273504687932,
      "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
      "sparse_score": 0.7857142857142857,
      "text": "Project Aurora GPU Approval Packet\nDecision: Alice approved the Project Aurora GPU purchase after finance review.\nOwner: Bob owns procurement and vendor coordination.\nBudget: Current approved cap is 45000 dollars.\nDeadline: Purchase order must be ready by July 15, 2026.\nRisk: Vendor selection is blocked if finance approval is not attached.",
      "time_score": 1.0,
      "token_estimate": 51,
      "updated_at_ms": 1782415059110
    },
    {
      "business_score": 0.95,
      "context_class": "entity",
      "embedding_score": 0.24807,
      "entity_name": "must be attached before vendor selection",
      "entity_type": "approval_state",
      "final_score": 0.661492,
      "keyword_score": 0,
      "matched_index_terms": [
        "entity_type:approval_state"
      ],
      "metadata": {},
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.555323,
      "packing_policy": "current_state",
      "packing_score": 0.961492,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 8967060400784335657,
      "ref_type": "entity",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.661492,
      "selection_reason": "selected by tree path, secondary indexes, and entity state score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.0,
      "text": "approval_state: must be attached before vendor selection = must be attached before vendor selection",
      "time_score": 1.0,
      "token_estimate": 13,
      "updated_at_ms": 1782415058953
    },
    {
      "business_score": 0.95,
      "context_class": "entity",
      "embedding_score": 0.0,
      "entity_name": "by Alice in finance, pending procurement owner assignment",
      "entity_type": "approval_state",
      "final_score": 0.629078,
      "keyword_score": 1,
      "matched_index_terms": [
        "entity_type:approval_state"
      ],
      "metadata": {},
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.512104,
      "packing_policy": "current_state",
      "packing_score": 0.929078,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 5708414255151575681,
      "ref_type": "entity",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.629078,
      "selection_reason": "selected by tree path, secondary indexes, and entity state score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.07142857142857142,
      "text": "approval_state: by Alice in finance, pending procurement owner assignment = by Alice in finance, pending procurement owner assignment",
      "time_score": 1.0,
      "token_estimate": 17,
      "updated_at_ms": 1782415058953
    },
    {
      "business_score": 0.95,
      "context_class": "entity",
      "embedding_score": 0.0,
      "entity_name": "attachment",
      "entity_type": "approval_state",
      "final_score": 0.610328,
      "keyword_score": 0,
      "matched_index_terms": [
        "entity_type:approval_state"
      ],
      "metadata": {},
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.487104,
      "packing_policy": "current_state",
      "packing_score": 0.910328,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 1722827731307680407,
      "ref_type": "entity",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.610328,
      "selection_reason": "selected by tree path, secondary indexes, and entity state score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.0,
      "text": "approval_state: attachment = attachment",
      "time_score": 1.0,
      "token_estimate": 3,
      "updated_at_ms": 1782415058953
    },
    {
      "business_score": 0.9,
      "context_class": "resource_fact",
      "embedding_score": 0.418384,
      "event_type": "resource_approval",
      "final_score": 0.728759,
      "keyword_score": 7,
      "matched_index_terms": [
        "classification:resource_fact",
        "event_type:correction",
        "event_type:resource_approval",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "source_type:resource",
        "source_type:resource_fact",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "resource_title": "Budget Update Memo",
        "source": "debug_trace"
      },
      "node_hash": 1737304210274426578,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "resources",
        "project_aurora",
        "gpu_procurement"
      ],
      "node_score": 0.732478,
      "origin_score": 0.651679,
      "packing_policy": "current_state",
      "packing_score": 0.908759,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 6999014925757708944,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.728759,
      "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
      "source_chunk_hash": 4893379877725482254,
      "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
      "sparse_score": 0.5,
      "text": "Budget Update Memo\nUpdate: The backup GPU quote increased the cap from 42000 dollars to 45000 dollars.\nCurrent state: 45000 dollars is the valid active budget cap.\nStale blocker: 42000 dollars is historical and should not be used for current-state answers.\nApprover: Alice confirmed the updated cap.",
      "time_score": 1.0,
      "token_estimate": 48,
      "updated_at_ms": 1782415059406
    },
    {
      "business_score": 0.9,
      "context_class": "resource_fact",
      "embedding_score": 0.455812,
      "event_type": "resource_approval",
      "final_score": 0.698979,
      "keyword_score": 5,
      "matched_index_terms": [
        "classification:resource_fact",
        "event_type:dialogue_batch",
        "event_type:resource_approval",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "source_type:resource",
        "source_type:resource_fact",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "resource_title": "GPU Procurement Runbook",
        "source": "debug_trace"
      },
      "node_hash": 1737304210274426578,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "resources",
        "project_aurora",
        "gpu_procurement"
      ],
      "node_score": 0.732478,
      "origin_score": 0.611972,
      "packing_policy": "current_state",
      "packing_score": 0.878979,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 4657147395257529645,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.698979,
      "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
      "source_chunk_hash": 6034139221235933872,
      "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_runbook.pdf#page=1",
      "sparse_score": 0.35714285714285715,
      "text": "GPU Procurement Runbook\nProcedure: Attach finance approval before vendor selection.\nProcedure: Compare primary and backup GPU quotes before purchase order creation.\nTroubleshooting: If approval attachment is missing, notify Alice and stop vendor selection.\nAudit: Store final vendor selection evidence with the purchase order.",
      "time_score": 1.0,
      "token_estimate": 43,
      "updated_at_ms": 1782415059343
    },
    {
      "business_score": 0.95,
      "coordinate_tuples": [
        [
          0,
          2
        ],
        [
          4,
          5
        ],
        [
          7,
          7
        ]
      ],
      "embedding_score": 0.480839,
      "final_score": 0.864313,
      "keyword_score": 11,
      "matched_index_terms": [
        "segment_topic:approval_budget"
      ],
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "non_contiguous": true,
      "origin_score": 0.8257513000000001,
      "packing_policy": "current_state",
      "packing_score": 0.864313,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 8549485618968706578,
      "ref_type": "segment",
      "saliency_score": 0.966667,
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.864313,
      "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
      "sparse_score": 0.7857142857142857,
      "text": "0: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget. 1: Recorded: Project Aurora GPU purchase is approved by Alice in finance, pending procurement owner assignment. 2: Bob will own procurement, and the budget cap is 42000 dollars for the initial GPU batch. 4: The deadline is July 15, 2026, and the runbook says finance approval must be attached before vendor sele...",
      "time_score": 1.0,
      "token_estimate": 69,
      "topic": "approval_budget",
      "updated_at_ms": 1782415058953
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.613941,
      "event_type": "NEW_EVENT",
      "final_score": 0.733204,
      "keyword_score": 7,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:correction",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 8,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.710938,
      "packing_policy": "current_state",
      "packing_score": 0.733204,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 6665319736342895071,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.733204,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.5,
      "text": "assistant: Updated: the current Project Aurora GPU budget cap is 45000 dollars.",
      "time_score": 1.0,
      "token_estimate": 12,
      "updated_at_ms": 1782415058943
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.572656,
      "event_type": "NEW_EVENT",
      "final_score": 0.687188,
      "keyword_score": 5,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:correction",
        "event_type:dialogue_batch",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 5,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.649584,
      "packing_policy": "current_state",
      "packing_score": 0.687188,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 420109978585177584,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.687188,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.35714285714285715,
      "text": "user: The deadline is July 15, 2026, and the runbook says finance approval must be attached before vendor selection.",
      "time_score": 1.0,
      "token_estimate": 19,
      "updated_at_ms": 1782415058917
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.50128,
      "event_type": "NEW_EVENT",
      "final_score": 0.672467,
      "keyword_score": 5,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:correction",
        "event_type:plan_update",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 3,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.629956,
      "packing_policy": "current_state",
      "packing_score": 0.672467,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 5587845906929271104,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.672467,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.35714285714285715,
      "text": "user: Bob will own procurement, and the budget cap is 42000 dollars for the initial GPU batch.",
      "time_score": 1.0,
      "token_estimate": 17,
      "updated_at_ms": 1782415058906
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.49923,
      "event_type": "NEW_EVENT",
      "final_score": 0.672044,
      "keyword_score": 5,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:confirmation",
        "event_type:correction",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 1,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.629392,
      "packing_policy": "current_state",
      "packing_score": 0.672044,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 524936940655528425,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.672044,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.35714285714285715,
      "text": "user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.",
      "time_score": 1.0,
      "token_estimate": 17,
      "updated_at_ms": 1782415058897
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.496139,
      "event_type": "NEW_EVENT",
      "final_score": 0.633907,
      "keyword_score": 3,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:correction",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 7,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.578542,
      "packing_policy": "current_state",
      "packing_score": 0.633907,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 3050751851654260645,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.633907,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.21428571428571427,
      "text": "user: Correction: Alice raised the cap to 45000 dollars after the backup GPU quote came in.",
      "time_score": 1.0,
      "token_estimate": 16,
      "updated_at_ms": 1782415058932
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.416025,
      "event_type": "NEW_EVENT",
      "final_score": 0.632023,
      "keyword_score": 4,
      "matched_index_terms": [
        "classification:new_event",
        "event_type:dialogue_batch",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "resource_title": "Project Aurora GPU Approval Packet",
        "source": "debug_trace"
      },
      "node_hash": 1737304210274426578,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "resources",
        "project_aurora",
        "gpu_procurement"
      ],
      "node_score": 0.732478,
      "origin_score": 0.576031,
      "packing_policy": "current_state",
      "packing_score": 0.632023,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 8513196518652600321,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.632023,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.2857142857142857,
      "text": "tool: Import PDF resource for MatrixArk parsing: Project Aurora GPU Approval Packet",
      "time_score": 1.0,
      "token_estimate": 12,
      "updated_at_ms": 1782415059110
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.403604,
      "event_type": "NEW_EVENT",
      "final_score": 0.614821,
      "keyword_score": 3,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:correction",
        "event_type:dialogue_batch",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 6,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.553095,
      "packing_policy": "current_state",
      "packing_score": 0.614821,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 6460926268341786696,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.614821,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.21428571428571427,
      "text": "assistant: The active deadline is July 15, 2026. Vendor selection requires the finance approval attachment.",
      "time_score": 1.0,
      "token_estimate": 15,
      "updated_at_ms": 1782415058925
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.074125,
      "event_type": "NEW_EVENT",
      "final_score": 0.565616,
      "keyword_score": 4,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:confirmation",
        "event_type:correction",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 2,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.487488,
      "packing_policy": "current_state",
      "packing_score": 0.565616,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 3110922328373977738,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.565616,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.2857142857142857,
      "text": "assistant: Recorded: Project Aurora GPU purchase is approved by Alice in finance, pending procurement owner assignment.",
      "time_score": 1.0,
      "token_estimate": 16,
      "updated_at_ms": 1782415058901
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.063629,
      "event_type": "NEW_EVENT",
      "final_score": 0.563451,
      "keyword_score": 4,
      "matched_index_terms": [
        "classification:batch_memory",
        "classification:new_event",
        "entity_type:approval_state",
        "entity_type:current_plan",
        "event_type:correction",
        "event_type:plan_update",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "segment_topic:approval_budget",
        "segment_topic:correction",
        "source_type:message",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "message_index": 4,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "session:debug-message-pdf-session",
          "conversation:project_aurora"
        ],
        "source": "debug_trace"
      },
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "node_score": 0.84208,
      "origin_score": 0.484602,
      "packing_policy": "current_state",
      "packing_score": 0.563451,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 4010347062634094153,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.563451,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.2857142857142857,
      "text": "assistant: I will track Bob as procurement owner and the 42000 dollar cap for the initial batch.",
      "time_score": 1.0,
      "token_estimate": 17,
      "updated_at_ms": 1782415058911
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.160128,
      "event_type": "NEW_EVENT",
      "final_score": 0.541744,
      "keyword_score": 2,
      "matched_index_terms": [
        "classification:new_event",
        "event_type:dialogue_batch",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "resource_title": "GPU Procurement Runbook",
        "source": "debug_trace"
      },
      "node_hash": 1737304210274426578,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "resources",
        "project_aurora",
        "gpu_procurement"
      ],
      "node_score": 0.732478,
      "origin_score": 0.455659,
      "packing_policy": "current_state",
      "packing_score": 0.541744,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 443443440602181842,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.541744,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.14285714285714285,
      "text": "tool: Import PDF resource for MatrixArk parsing: GPU Procurement Runbook",
      "time_score": 1.0,
      "token_estimate": 10,
      "updated_at_ms": 1782415059343
    },
    {
      "business_score": 0.5,
      "context_class": "event",
      "embedding_score": 0.098058,
      "event_type": "NEW_EVENT",
      "final_score": 0.510193,
      "keyword_score": 1,
      "matched_index_terms": [
        "classification:new_event",
        "event_type:dialogue_batch",
        "keyword:alice",
        "keyword:approval",
        "keyword:approved",
        "keyword:attach",
        "keyword:aurora",
        "keyword:backup",
        "keyword:budget",
        "keyword:cap",
        "keyword:compare",
        "keyword:current",
        "keyword:decision",
        "keyword:dollars",
        "keyword:finance",
        "keyword:gpu",
        "keyword:increased",
        "keyword:memo",
        "keyword:owner",
        "keyword:packet",
        "keyword:primary",
        "keyword:procedure",
        "keyword:procurement",
        "keyword:project",
        "keyword:purchase",
        "keyword:quote",
        "keyword:review",
        "keyword:runbook",
        "keyword:selection",
        "keyword:state",
        "keyword:update",
        "keyword:valid",
        "keyword:vendor",
        "resource_type:pdf",
        "source_type:resource",
        "status:observed",
        "unit_kind:pdf_page"
      ],
      "metadata": {
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "resource_title": "Budget Update Memo",
        "source": "debug_trace"
      },
      "node_hash": 1737304210274426578,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "resources",
        "project_aurora",
        "gpu_procurement"
      ],
      "node_score": 0.732478,
      "origin_score": 0.41359,
      "packing_policy": "current_state",
      "packing_score": 0.510193,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_hybrid",
      "ref_hash": 1585344323533811142,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "score": 0.510193,
      "selection_reason": "selected by tree path, secondary indexes, and event hybrid score",
      "source_chunk_hash": null,
      "source_ref": "",
      "sparse_score": 0.07142857142857142,
      "text": "tool: Import PDF resource for MatrixArk parsing: Budget Update Memo",
      "time_score": 1.0,
      "token_estimate": 10,
      "updated_at_ms": 1782415059406
    }
  ],
  "used_context_tokens": 430
}
```

## ContextPack

```json
{
  "access": {
    "account_id": "acct_local",
    "agent_name": "codex",
    "api_key_id": "dev",
    "mode": "dev",
    "role": "dev_admin",
    "session_id": "debug-message-pdf-session",
    "tenant_id": "tenant_codex",
    "user_id": "deeproute"
  },
  "auxiliary_candidate_count": 29,
  "context_assembly_policy": {
    "access_scope_before_scoring": true,
    "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
    "skill_selection": "skill_section_only"
  },
  "context_pack_id": "3047053284104680203",
  "dropped_refs": {
    "duplicate": 12,
    "estimated_tokens": {
      "duplicate": 584,
      "low_score": 0,
      "over_budget": 0,
      "raw_l2": 0,
      "stale": 0,
      "summary": 0
    },
    "low_score": 0,
    "over_budget": 0,
    "raw_l2": 0,
    "reason_descriptions": {
      "duplicate": "candidate duplicated local context or an already selected ref",
      "low_score": "candidate score was below the minimum packing threshold",
      "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
      "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
      "stale": "candidate was stale or superseded for the query policy",
      "summary": "summary text was dropped in favor of denser raw/evidence refs"
    },
    "refs": [
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_decision",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.809227,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 571365746382456544,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.80692,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_cost",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.804421,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 1596920217437410578,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.803316,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_owner",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.789166,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 2900456491093257987,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.791875,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_risk",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.784072,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 4265994536714805107,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.788054,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_policy",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.780112,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 929939956861191542,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.785084,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:confirmation",
          "event_type:resource_deadline",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.774172,
        "packing_score": 1.0,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 9047961491740927299,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.780629,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_gpu_approval_packet.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 51,
        "token_estimate": 51,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:correction",
          "event_type:resource_procedure",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.656496,
        "packing_score": 0.992372,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 4155085225937358975,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
        "score": 0.692372,
        "selection_reason": "selected by tree path, secondary indexes, and resource fact/event hybrid score",
        "source_ref": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "stale_or_superseded": false,
        "token_cost": 48,
        "token_estimate": 48,
        "version_state": "current"
      },
      {
        "access_decision": "allowed_by_scope",
        "citation": "/root/src/github-services/TemporalStore/docs/debug/matrixark_message_pdf_trace/fixtures/aurora_budget_update.pdf#page=1",
        "context_class": "resource_fact",
        "drop_reason": "duplicate",
        "matched_index_terms": [
          "classification:resource_fact",
          "event_type:correction",
          "event_type:resource_policy",
          "keyword:alice",
          "keyword:approval",
          "keyword:approved",
          "keyword:attach",
          "keyword:aurora",
          "keyword:backup",
          "keyword:budget",
          "keyword:cap",
          "keyword:compare",
          "keyword:current",
          "keyword:decision",
          "keyword:dollars",
          "keyword:finance",
          "keyword:gpu",
          "keyword:increased",
          "keyword:memo",
          "keyword:owner",
          "keyword:packet",
          "keyword:primary",
          "keyword:procedure",
          "keyword:procurement",
          "keyword:project",
          "keyword:purchase",
          "keyword:quote",
          "keyword:review",
          "keyword:runbook",
          "keyword:selection",
          "keyword:state",
          "keyword:update",
          "keyword:valid",
          "keyword:vendor",
          "resource_type:pdf",
          "source_type:resource",
          "source_type:resource_fact",
          "status:observed",
          "unit_kind:pdf_page"
        ],
        "node_hash": 1737304210274426578,
        "node_path": [
          "tenant:tenant_codex",
          "user:deeproute",
          "resources",
          "project_aurora",
          "gpu_procurement"
        ],
        "origin_score": 0.64993,
        "packing_score": 0.987448,
        "raw_uri": "",
        "reason": "duplicate",
        "ref_hash": 5430002385288940542,
        "ref_type": "event",
        "resource_type": "",
        "resource_version": "",
 
```

## Replay

```json
{
  "access": {
    "account_id": "acct_local",
    "agent_name": "codex",
    "api_key_id": "dev",
    "mode": "dev",
    "role": "dev_admin",
    "session_id": "debug-message-pdf-session",
    "tenant_id": "tenant_codex",
    "user_id": "deeproute"
  },
  "context_pack_id": "3047053284104680203",
  "events": [
    {
      "account_id": "acct_local",
      "action": "backend.ready",
      "api_key_id": "dev",
      "audit_id_hash": 8496801801613178684,
      "created_at_ms": 1782415058897,
      "details": {
        "attempts": null,
        "backend": "local"
      },
      "record_type": "matrixark_audit_log",
      "role": "dev_admin",
      "status": "ok",
      "tenant_id": "tenant_codex"
    },
    {
      "created_at_ms": 1782415058897,
      "depth": 1,
      "node_hash": 3263141514618168867,
      "node_name": "tenant:tenant_codex",
      "node_path": [
        "tenant:tenant_codex"
      ],
      "parent_hash": 0,
      "record_type": "context_node",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "created_at_ms": 1782415058897,
      "depth": 2,
      "node_hash": 623184698193930698,
      "node_name": "user:deeproute",
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute"
      ],
      "parent_hash": 3263141514618168867,
      "record_type": "context_node",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "child_hash": 623184698193930698,
      "child_name": "user:deeproute",
      "child_path": [
        "tenant:tenant_codex",
        "user:deeproute"
      ],
      "child_ref_hash": 30283733866140312,
      "created_at_ms": 1782415058897,
      "depth": 2,
      "parent_hash": 3263141514618168867,
      "parent_path": [
        "tenant:tenant_codex"
      ],
      "record_type": "context_child_ref",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "created_at_ms": 1782415058897,
      "depth": 3,
      "node_hash": 3084181658660614334,
      "node_name": "session:debug-message-pdf-session",
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session"
      ],
      "parent_hash": 623184698193930698,
      "record_type": "context_node",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "child_hash": 3084181658660614334,
      "child_name": "session:debug-message-pdf-session",
      "child_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session"
      ],
      "child_ref_hash": 3331542308452180010,
      "created_at_ms": 1782415058897,
      "depth": 3,
      "parent_hash": 623184698193930698,
      "parent_path": [
        "tenant:tenant_codex",
        "user:deeproute"
      ],
      "record_type": "context_child_ref",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "created_at_ms": 1782415058897,
      "depth": 4,
      "node_hash": 2100209595829882121,
      "node_name": "conversation:project_aurora",
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "parent_hash": 3084181658660614334,
      "record_type": "context_node",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "child_hash": 2100209595829882121,
      "child_name": "conversation:project_aurora",
      "child_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "child_ref_hash": 472388797698908023,
      "created_at_ms": 1782415058897,
      "depth": 4,
      "parent_hash": 3084181658660614334,
      "parent_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session"
      ],
      "record_type": "context_child_ref",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "status": "active",
      "updated_at_ms": 1782415058897
    },
    {
      "context_node_key": [
        "deeproute",
        "debug-message-pdf-session",
        "",
        ""
      ],
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "record_type": "context_summary",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "source_event_hash": 524936940655528425,
      "summary_hash": 8695652974415713980,
      "summary_text": "user: Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.",
      "summary_type": "session_l0",
      "updated_at_ms": 1782415058897
    },
    {
      "dim": 32,
      "embedding_type": "session_l0",
      "model": "matrixark-local-token-hash-v1",
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "record_type": "context_embedding",
      "ref_hash": 8695652974415713980,
      "ref_type": "summary",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "updated_at_ms": 1782415058897,
      "vector": [
        0.0,
        0.0,
        0.0,
        -0.2,
        0.4,
        0.0,
        -0.2,
        0.0,
        0.0,
        0.0,
        -0.4,
        0.2,
        0.0,
        0.2,
        0.0,
        0.0,
        0.4,
        0.0,
        0.0,
        -0.2,
        0.0,
        -0.2,
        0.0,
        0.0,
        0.2,
        -0.4,
        0.0,
        -0.2,
        0.0,
        0.0,
        0.0,
        0.2
      ]
    },
    {
      "dim": 32,
      "embedding_type": "event_text",
      "model": "matrixark-local-token-hash-v1",
      "node_hash": 2100209595829882121,
      "node_path": [
        "tenant:tenant_codex",
        "user:deeproute",
        "session:debug-message-pdf-session",
        "conversation:project_aurora"
      ],
      "record_type": "context_embedding",
      "ref_hash": 524936940655528425,
      "ref_type": "event",
      "scope": {
        "_explicit_scope_keys": [
          "account_id",
          "agent_name",
          "session_id",
          "tenant_id",
          "user_id"
        ],
        "account_id": "acct_local",
        "agent_name": "codex",
        "session_hash": 7498925135890267938,
        "session_id": "debug-message-pdf-session",
        "tenant_hash": 2466697514329931826,
        "tenant_id": "tenant_codex",
        "user_hash": 7836037686236352053,
        "user_id": "deeproute"
      },
      "updated_at_ms": 1782415058897,
      "vector": [
        0.0,
        0.0,
        0.0,
        -0.2,
        0.4,
        0.0,
        -0.2,
        0.0,
        0.0,
        0.0,
        -0.4,
        0.2,
        0.0,
        0.2,
        0.0,
        0.0,
        0.4,
        0.0,
        0.0,
        -0.2,
        0.0,
        -0.2,
        0.0,
        0.0,
        0.2,
        -0.4,
        0.0,
        -0.2,
        0.0,
        0.0,
        0.0,
        0.2
      ]
    },
    {
      "agent_hook": null,
      "envelope": {
        "ingestion_time_ms": 1782415058897,
        "kind": "message",
        "messages": [
          {
            "content": "Alice from finance approved the GPU purchase request for Project Aurora after reviewing the Q3 budget.",
            "role": "user"
          }
        ],
        "metadata": {
          "message_index": 1,
          "node_
```
