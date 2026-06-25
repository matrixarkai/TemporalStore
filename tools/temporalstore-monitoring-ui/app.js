const healthIds = ["metaserver", "proxy", "exporter", "data_nodes", "efs", "blockcache"];
const refreshTimeoutMs = Number(globalThis.TEMPORALSTORE_REFRESH_TIMEOUT_MS) || 5000;
const healthCacheKey = "temporalstore.monitoring.lastGoodHealth.v1";
const healthCacheMaxAgeMs = Number(globalThis.TEMPORALSTORE_HEALTH_CACHE_MAX_AGE_MS) || 6 * 60 * 60 * 1000;
const refreshIntervalMs = Number(globalThis.TEMPORALSTORE_REFRESH_INTERVAL_MS) || 15000;

const fallbackHealth = {
  cluster: {
    name: "aws-scale",
    status: "pending",
    environment: "AWS test cluster",
    metaservers: 1,
    data_nodes: 2,
  },
  health: {
    metaserver: { status: "pending", detail: "waiting for live health.json" },
    proxy: { status: "pending", detail: "waiting for live health.json" },
    exporter: { status: "pending", detail: "waiting for live health.json" },
    data_nodes: { status: "pending", detail: "primary and secondary expected" },
    efs: { status: "pending", detail: "shared file store" },
    blockcache: { status: "pending", detail: "DRAM + SSD cache" },
  },
  runtime_config: {
    profile: "low-latency EFS replay",
    storage_zone_size: "256 MB",
    stream_max_blob_size: "256 MB",
    storage_oplog_delay_dump_length: "0",
    replicator_loop_interval_us: "1000",
    replicator_max_oplog_per_loop: "20000",
    replicator_update_remote_interval_ms: "20",
    blockcache_dram_capacity: "64 MB",
    blockcache_ssd_capacity: "2 GB",
    modes: [
      { name: "low-latency", oplog_batch: "1 ms / 256 KB", replay_loop: "1 ms", use: "secondary visibility" },
      { name: "default", oplog_batch: "2 ms / 512 KB", replay_loop: "1-5 ms", use: "balanced serving" },
      { name: "throughput", oplog_batch: "5 ms / 1 MB", replay_loop: "5 ms", use: "bulk ingest" },
    ],
  },
  nodes: [
    {
      name: "meta-01",
      role: "metaserver + proxy + UI",
      status: "pending",
      endpoint: "10.70.1.88:17000",
      cpu: "-",
      memory: "-",
      storage: "EFS mounted",
      replay: "n/a",
    },
    {
      name: "data-01",
      role: "primary",
      status: "pending",
      endpoint: "10.70.1.139:17001",
      cpu: "-",
      memory: "-",
      storage: "EFS + blockcache",
      replay: "writer",
    },
    {
      name: "data-02",
      role: "secondary",
      status: "pending",
      endpoint: "10.70.1.195:17001",
      cpu: "-",
      memory: "-",
      storage: "EFS + blockcache",
      replay: "waiting",
    },
  ],
  replication: {
    mode: "shared file store + primary-pull fallback",
    secondary_lag_ms: "-",
    replay_source: "EFS or primary stream",
    visibility: "pending",
    lag_matrix: [],
  },
  scale_tests: [
    {
      name: "TemporalAggregate 10k features",
      status: "pending",
      write_qps: "-",
      read_p50_ms: "-",
      read_p99_ms: "-",
      secondary_lag_ms: "-",
      workload: "feature x bucket increments with window query",
    },
    {
      name: "STRING primary",
      status: "pending",
      write_qps: "-",
      read_p50_ms: "-",
      read_p99_ms: "-",
      secondary_lag_ms: "-",
      workload: "plain SET/GET baseline",
    },
    {
      name: "Sequence feature",
      status: "pending",
      write_qps: "-",
      read_p50_ms: "-",
      read_p99_ms: "-",
      secondary_lag_ms: "-",
      workload: "long behavior sequence window scan",
    },
  ],
  scale_matrix: [],
  module_tests: [
    {
      module: "TemporalAggregate",
      test: "high-cardinality window aggregate",
      status: "pending",
      write_path: "direct SDK",
      read_path: "primary and replica-eligible",
      latency: "-",
      notes: "count/sum over event buckets",
    },
    {
      module: "Feature",
      test: "module ingest/query",
      status: "pending",
      write_path: "direct SDK",
      read_path: "primary",
      latency: "-",
      notes: "profile-like feature object",
    },
    {
      module: "IPS",
      test: "risk/frequency cap sample",
      status: "pending",
      write_path: "direct SDK",
      read_path: "primary",
      latency: "-",
      notes: "slot/table/action dimensions",
    },
    {
      module: "STRING",
      test: "SET/GET baseline",
      status: "pending",
      write_path: "direct SDK and proxy",
      read_path: "primary and replica-eligible",
      latency: "-",
      notes: "plain KV baseline",
    },
  ],
  data_models: [
    {
      name: "TemporalAggregate",
      status: "passed",
      use_case: "high-cardinality counters, sums, and windowed feature serving",
      write_shape: "INCR(key, metric, dimensions, timestamp, bucket_width, value)",
      query_shape: "QUERY(key, metric, dimensions, start, end, bucket_width, op)",
      storage_shape: "bucketed temporal state inside an object",
      consistency: "primary write, secondary replay",
      test_status: "10k features x 12 buckets passed",
    },
    {
      name: "Sequence Feature",
      status: "passed",
      use_case: "long behavior history and filtered window scans",
      write_shape: "append rows with timestamp and fields",
      query_shape: "window query with optional filters",
      storage_shape: "time-ordered rows per entity",
      consistency: "primary write, secondary replay",
      test_status: "100k rows smoke and latency test passed",
    },
    {
      name: "STRING",
      status: "passed",
      use_case: "plain KV baseline and Redis-compatible simple values",
      write_shape: "SET(key, value)",
      query_shape: "GET(key)",
      storage_shape: "single value object",
      consistency: "primary write, replica-eligible read",
      test_status: "smoke passed; scale rerun pending",
    },
  ],
  diagnostics: {
    last_result_dir: "-",
    release_build: "pending",
    proxy_sdk: "pending",
    direct_sdk: "pending",
  },
  products: [],
  product_services: [],
  bytekv_scale: {},
  abase_api: {},
  context_ops: {
    status: "ready",
    kpis: [
      { label: "Unified cases", value: "32", note: "context, SDK, cache, storage, raft" },
      { label: "Context latency budget", value: "50 ms", note: "tree traversal target" },
      { label: "Prompt budget", value: "48 tokens", note: "exact event + chunk parity case" },
      { label: "Serving store", value: "TemporalStore", note: "events, nodes, indexes, embeddings" },
      { label: "OSS models", value: "local first", note: "embedding, rerank, extraction, VLM" },
    ],
    flow: [
      { name: "Raw input", status: "ready", detail: "query, event, feedback, .md/.txt/.pdf resource" },
      { name: "Extraction", status: "ready", detail: "intent, time window, filters, event type, status" },
      { name: "Ingestion", status: "ready", detail: "node chain, child refs, event, index, embedding, dirty marker" },
      { name: "Retrieval", status: "ready", detail: "bounded tree traversal, filters, chunks, context pack" },
      { name: "Replay", status: "ready", detail: "pack audit and post-answer feedback become future context" },
    ],
    data_plane: [
      {
        label: "Context Nodes",
        value: "7 visible",
        detail: "tenant/team/project/collection/resource/chunk topology with hashes and parent refs",
        evidence: "ContextNode + ChildRef",
        status: "ready",
      },
      {
        label: "Events",
        value: "4 writes",
        detail: "timestamped event leaves, feedback events, incident facts, and approval facts",
        evidence: "ContextEvent",
        status: "passed",
      },
      {
        label: "Extractions",
        value: "2 lanes",
        detail: "raw query/event/resource text into event type, status, filters, and time hints",
        evidence: "rules + optional LLM",
        status: "passed",
      },
      {
        label: "Ingestions",
        value: "6 paths",
        detail: "raw event, resource, feedback, API, stream, and batch ingestion paths",
        evidence: "idempotency + offsets",
        status: "passed",
      },
      {
        label: "Resources",
        value: "3 lanes",
        detail: "markdown/text/pdf chunks, raw_uri refs, resource filters, and chunk embeddings",
        evidence: "ResourceChunk + embedding",
        status: "passed",
      },
      {
        label: "Feedback",
        value: "1 path",
        detail: "final answer confirmation becomes future retrievable memory",
        evidence: "feedback ContextEvent",
        status: "passed",
      },
      {
        label: "Summaries",
        value: "1 compression lane",
        detail: "dirty summary markers, non-destructive time compression, source event audit",
        evidence: "ContextCompressionEvent",
        status: "passed",
      },
      {
        label: "Context Packs",
        value: "audited",
        detail: "token-budgeted events, chunks, filters, replay metadata, and pack audit",
        evidence: "ContextPackAudit",
        status: "passed",
      },
    ],
    tests: [
      {
        name: "Raw event extraction",
        status: "passed",
        workload: "Dana approved security review 15555 for project_1",
        write_qps: "1 event",
        read_p50_ms: "intent parsed",
        read_p99_ms: "index written",
        secondary_lag_ms: "dirty L0 marker",
        threads: "tenant 1001",
        result_dir: "context_raw_extraction_query_pipeline",
      },
      {
        name: "Resource chunk recall",
        status: "passed",
        workload: "Markdown runbook chunks with source refs and embeddings",
        write_qps: "2 chunks",
        read_p50_ms: "topK=1",
        read_p99_ms: "citation returned",
        secondary_lag_ms: "TemporalStore embedding",
        threads: "resource markdown",
        result_dir: "context_resource_ingestion_chunk_query",
      },
      {
        name: "Feedback replay",
        status: "passed",
        workload: "Final LLM answer confirmation is stored as future memory",
        write_qps: "1 feedback event",
        read_p50_ms: "queryable",
        read_p99_ms: "pack-audited",
        secondary_lag_ms: "replayable",
        threads: "query 77015555",
        result_dir: "context_resource_event_feedback_loop",
      },
      {
        name: "Budget parity",
        status: "passed",
        workload: "Event-first then resource-chunk packing at 47/48 token boundary",
        write_qps: "0 writes",
        read_p50_ms: "24 tokens",
        read_p99_ms: "48 tokens",
        secondary_lag_ms: "C++ + Rust",
        threads: "query 77027771",
        result_dir: "context_pack_token_budget_parity",
      },
      {
        name: "Layered resource parsing",
        status: "passed",
        workload: "PDF-style L0/L1/L2 chunks with source refs, type filters, extracted fact, and pack retrieval",
        write_qps: "3 chunks",
        read_p50_ms: "L0 abstract",
        read_p99_ms: "60 tokens",
        secondary_lag_ms: "C++ + Rust",
        threads: "resource pdf",
        result_dir: "context_layered_resource_parsing_pipeline",
      },
    ],
    pipeline: [
      {
        step: "ingest_raw_event",
        input: "raw_text + tenant/team/project hints",
        writes: "ContextNode, ChildRef, ContextEvent, IndexRef, Embedding, DirtyMarker",
        query: "status=approved AND project=project_1",
        output: "event 992003",
        status: "passed",
      },
      {
        step: "ingest_resource",
        input: "raw_uri + markdown chunks",
        writes: "resource node, chunk refs, chunk embeddings",
        query: "vector + raw_uri filter",
        output: "chunk 10016661",
        status: "passed",
      },
      {
        step: "retrieve_with_resources",
        input: "raw query + vector + token budget",
        writes: "ContextPackAudit",
        query: "tree traversal + event filters + chunk recall",
        output: "events 992003/992010 + chunk 10016661",
        status: "passed",
      },
      {
        step: "ingest_feedback",
        input: "final answer feedback + query id",
        writes: "feedback ContextEvent",
        query: "future memory lookup",
        output: "event 992004",
        status: "passed",
      },
      {
        step: "budgeted_pack",
        input: "raw query + time/filter hints + max_prompt_tokens",
        writes: "optional ContextPackAudit",
        query: "event first, then chunk if tokens fit",
        output: "47 tokens => event only; 48 tokens => event + chunk",
        status: "passed",
      },
      {
        step: "parse_layered_resource",
        input: "pdf raw_uri + L0/L1/L2 page chunks",
        writes: "resource node, chunk embeddings, raw_uri ref, extracted incident event",
        query: "resource_type=pdf AND raw_uri=incident_77.pdf",
        output: "chunk 10029901 + event 994020",
        status: "passed",
      },
      {
        step: "batch_ingest_x8",
        input: "8 raw events + per-event hints",
        writes: "8 ContextEvents, 8 leaf embeddings, status indexes",
        query: "approval, incident, cost, rejected security filters",
        output: "8 extracted events + 3 retrieval packs",
        status: "passed",
      },
      {
        step: "api_stream_batch_ingest",
        input: "API idempotency key + stream offsets + batch items",
        writes: "shared extraction path into ContextEvent and indexes",
        query: "API approval, stream incident, batch cost, compressed window",
        output: "7 accepted events + duplicate API key and stream offsets skipped",
        status: "passed",
      },
      {
        step: "time_compression",
        input: "older event ids + source time window",
        writes: "ContextCompressionEvent summary",
        query: "time range returns compression id 77001",
        output: "fresh raw events stay queryable; older window has summary",
        status: "passed",
      },
      {
        step: "parity_gates_x8",
        input: "API, stream, batch, resource, retrieve, token budget, compression",
        writes: "one TemporalStore-only resource chunk for tenant 3003",
        query: "context_assert_parity_gates over existing corpus state",
        output: "8 parity gates passed with duplicate stream event absent",
        status: "passed",
      },
      {
        step: "parity_gates_x9",
        input: "API retry, stream replay, batch, resource, retrieve, compression sources",
        writes: "no duplicate API/stream events; compression source ids retained",
        query: "context_nine_ingestion_compression_parity_gates",
        output: "9 parity gates passed with source events 996001, 996004, 996007",
        status: "passed",
      },
    ],
    e2e_parity_runs: [
      {
        run: "Raw query to pack",
        covers: "query extraction, traversal, filters, token budget",
        evidence: "context_resource_feedback_second_query_pipeline",
        output: "events + resource chunk returned under prompt budget",
        status: "passed",
      },
      {
        run: "API idempotency",
        covers: "idempotency key, event extraction, duplicate retry skip",
        evidence: "context_stream_batch_api_ingestion_compression",
        output: "API event 996001 accepted; duplicate 996088 absent",
        status: "passed",
      },
      {
        run: "Stream replay checkpoint",
        covers: "partition offset checkpoint, replay skip, next offset accept",
        evidence: "context_stream_batch_api_ingestion_compression",
        output: "offset 12 skipped on replay; offset 13 produced 996007",
        status: "passed",
      },
      {
        run: "Batch ingest x8",
        covers: "approval, incident, cost, rejected security filters",
        evidence: "context_batch_extraction_query_ingestion_x8",
        output: "8 extracted events and 3 retrieval packs",
        status: "passed",
      },
      {
        run: "Resource parsing",
        covers: "markdown/text/pdf chunks, source refs, embeddings",
        evidence: "context_layered_resource_parsing_pipeline",
        output: "PDF chunk 10029901 and event 994020",
        status: "passed",
      },
      {
        run: "Feedback memory",
        covers: "final answer confirmation and future retrieval",
        evidence: "context_resource_event_feedback_loop",
        output: "feedback event 994011 is replayable",
        status: "passed",
      },
      {
        run: "Temporal compression",
        covers: "older window summary without hiding fresh events",
        evidence: "context_stream_batch_api_ingestion_compression",
        output: "compression id 77001 returned",
        status: "passed",
      },
      {
        run: "Compression source audit",
        covers: "source event ids survive compression for replay and governance",
        evidence: "context_nine_ingestion_compression_parity_gates",
        output: "source events 996001, 996004, 996007 retained",
        status: "passed",
      },
      {
        run: "C++ module parity",
        covers: "registered context models, child refs, embeddings, summaries",
        evidence: "context_module_test --gtest_brief=1",
        output: "7 context module tests passed",
        status: "passed",
      },
    ],
    request_builder: [
      {
        name: "Ingest",
        method: "POST",
        path: "/v1/context/ingest_raw_event",
        body: "raw_text, tenant_hash, hints {team, project, leaf_node_hash, event_time_ms}",
      },
      {
        name: "Batch",
        method: "POST",
        path: "/v1/context/batch_ingest",
        body: "tenant_hash, events[] {raw_text, hints}",
      },
      {
        name: "Stream",
        method: "POST",
        path: "/v1/context/stream_ingest",
        body: "stream_name, partition, offset, raw_text, hints",
      },
      {
        name: "Retrieve",
        method: "POST",
        path: "/v1/context/retrieve_with_resources",
        body: "raw_query, query_vector, root_node_hash, time window, max_prompt_tokens",
      },
      {
        name: "Feedback",
        method: "POST",
        path: "/v1/context/ingest_feedback",
        body: "query_id_hash, final answer feedback, node_hash, confidence, importance",
      },
      {
        name: "Query intent",
        method: "POST",
        path: "/v1/context/extract_query",
        body: "raw_query, hints {team, project, time_window}, optional agent intent",
      },
      {
        name: "Resource",
        method: "POST",
        path: "/v1/context/ingest_resource",
        body: "raw_uri, resource_type, chunks {text, vector, layer}",
      },
    ],
    query_workbench: {
      raw_query: "What confirmed context says incident INC-77 rollback is stable and what runbook supports it?",
      query_id: "77027771",
      route: "tenant 1001 / infra_team / project_1",
      intent: [
        { label: "event_type", value: "incident_update" },
        { label: "type", value: "confirmation" },
        { label: "time_window", value: "latest" },
        { label: "resource", value: "incident_77.txt" },
      ],
      controls: [
        { label: "root_node_hash", value: "1001300" },
        { label: "top_k_per_depth", value: "1" },
        { label: "max_depth", value: "2" },
        { label: "max_candidate_nodes", value: "3" },
        { label: "max_prompt_tokens", value: "48" },
      ],
      filters: [
        "team=infra_team",
        "project=project_1",
        "type=confirmation",
        "min_confidence>=95",
        "min_importance>=80",
        "time=1781509150000..1781509300000",
      ],
      result: {
        events: ["994011 user confirmation", "994020 PDF abstract fact"],
        chunks: ["10029901 incident_77.pdf#page=1:L0"],
        tokens: "60 / 70",
      },
    },
    config: [
      {
        group: "Extraction",
        items: [
          { label: "Default parser", value: "rules first" },
          { label: "OSS embedding", value: "all-MiniLM-L6-v2 / bge-small" },
          { label: "Optional LLM", value: "Qwen/Llama/Mistral provider hook" },
          { label: "Accepted hints", value: "team, project, time_window, token budget" },
        ],
      },
      {
        group: "Traversal",
        items: [
          { label: "max_depth", value: "6 default / 2 in parity case" },
          { label: "top_k_per_depth", value: "1-5" },
          { label: "max_children_scored", value: "128 per parent" },
          { label: "candidate cap", value: "24 nodes" },
        ],
      },
      {
        group: "Resources",
        items: [
          { label: "types", value: ".md, .txt, .pdf" },
          { label: "raw bytes", value: "raw_uri reference" },
          { label: "stored data", value: "L0/L1/L2 chunks, embeddings" },
          { label: "filters", value: "raw_uri, resource_type" },
        ],
      },
      {
        group: "Summaries",
        items: [
          { label: "L0", value: "required for traversal" },
          { label: "L1", value: "optional overview" },
          { label: "refresh", value: "async dirty marker" },
          { label: "compression", value: "non-destructive time-window summary" },
        ],
      },
    ],
    model_registry: [
      {
        role: "Query embedding",
        default_model: "sentence-transformers/all-MiniLM-L6-v2",
        alternatives: "BAAI/bge-small-en-v1.5, intfloat/e5-small-v2",
        runtime: "CPU default",
        io: "raw query -> normalized vector",
        use: "tree traversal and chunk recall",
      },
      {
        role: "Resource embedding",
        default_model: "sentence-transformers/all-MiniLM-L6-v2",
        alternatives: "BAAI/bge-base-en-v1.5, mixedbread-ai/mxbai-embed-large-v1",
        runtime: "CPU or small GPU",
        io: "L0/L1/L2 chunks -> vectors",
        use: "source-ref chunk recall",
      },
      {
        role: "Reranker",
        default_model: "BAAI/bge-reranker-base",
        alternatives: "cross-encoder/ms-marco-MiniLM-L-6-v2",
        runtime: "optional CPU/GPU",
        io: "query + candidate text -> relevance score",
        use: "final pack ordering after TemporalStore filters",
      },
      {
        role: "Extraction LLM",
        default_model: "Qwen2.5-7B-Instruct",
        alternatives: "Llama-3.1-8B-Instruct, Mistral-7B-Instruct",
        runtime: "GPU recommended, rules fallback",
        io: "raw event/query -> event_type, status, time, filters",
        use: "schema-light customer input",
      },
      {
        role: "Summary LLM",
        default_model: "Qwen2.5-7B-Instruct",
        alternatives: "Phi-3.5-mini-instruct, Llama-3.2-3B-Instruct",
        runtime: "async worker",
        io: "events/chunks -> L0/L1 summaries",
        use: "refresh dirty summaries without blocking writes",
      },
      {
        role: "PDF/VLM parser",
        default_model: "Qwen2-VL-7B-Instruct",
        alternatives: "InternVL2.5, LLaVA-Next",
        runtime: "optional GPU",
        io: "page image/table/chart -> text facts + source refs",
        use: "multimodal documents beyond text extraction",
      },
    ],
    tree: [
      { depth: 0, label: "company_a", meta: "tenant root" },
      { depth: 1, label: "infra_team", meta: "scope hash" },
      { depth: 2, label: "project_1", meta: "project filter" },
      { depth: 3, label: "approvals", meta: "collection" },
      { depth: 4, label: "security_review_15555", meta: "leaf event/entity" },
      { depth: 4, label: "security_review_runbook", meta: "resource node" },
      { depth: 4, label: "incident_inc_77_resolved", meta: "confirmed incident leaf" },
      { depth: 4, label: "incident_77_runbook", meta: "resource chunks" },
      { depth: 4, label: "incident_77_postmortem", meta: "pdf L0/L1/L2 chunks" },
    ],
    topology: {
      summary: "TemporalStore node graph with idempotent child refs and compact parent metadata",
      selected_path: ["1001000", "1001100", "1001200", "1001300", "10029900"],
      nodes: [
        {
          id: "1001000",
          parent: "",
          label: "company_a",
          type: "tenant root",
          depth: 0,
          child_count: 1,
          updated_at_ms: "1781500000100",
          status: "scope",
          score: "-",
          metadata: {
            object_key: "ctx:node:1001:1001000",
            model: "ContextNode",
            tenant_hash: "1001",
            scope: "tenant",
            child_ref_key: "ctx:child:1001:1001000",
            summary: "L0 tenant routing root",
          },
          records: {
            summaries: [{ level: "L0", text: "Company A context root with infra team memory." }],
            indexes: [{ name: "tenant_hash", value: "1001" }],
          },
        },
        {
          id: "1001100",
          parent: "1001000",
          label: "infra_team",
          type: "team",
          depth: 1,
          child_count: 1,
          updated_at_ms: "1781500000200",
          status: "scope",
          score: "-",
          metadata: {
            object_key: "ctx:node:1001:1001100",
            model: "ContextNode",
            parent_ref: "ctx:child:1001:1001000 -> 1001100",
            scope_hash: "infra_team",
            child_ref_key: "ctx:child:1001:1001100",
            serving_filter: "team=infra_team",
          },
          records: {
            summaries: [{ level: "L0", text: "Infra team project approvals, incidents, runbooks, and cost context." }],
            indexes: [{ name: "team", value: "infra_team" }],
          },
        },
        {
          id: "1001200",
          parent: "1001100",
          label: "project_1",
          type: "project",
          depth: 2,
          child_count: 1,
          updated_at_ms: "1781500000300",
          status: "scope",
          score: "-",
          metadata: {
            object_key: "ctx:node:1001:1001200",
            model: "ContextNode",
            parent_ref: "ctx:child:1001:1001100 -> 1001200",
            scope_hash: "project_1",
            child_ref_key: "ctx:child:1001:1001200",
            serving_filter: "project=project_1",
          },
          records: {
            summaries: [{ level: "L0", text: "Project 1 active approvals, incidents, budget changes, and resources." }],
            indexes: [{ name: "project", value: "project_1" }],
          },
        },
        {
          id: "1001300",
          parent: "1001200",
          label: "approvals",
          type: "collection",
          depth: 3,
          child_count: 5,
          updated_at_ms: "1781500000500",
          status: "parent updated",
          score: "0.96",
          metadata: {
            object_key: "ctx:node:1001:1001300",
            model: "ContextNode",
            child_ref_key: "ctx:child:1001:1001300",
            embedding_ref: "ctx:emb:1001:1001300",
            query_role: "collection selected by L0 score before leaf timeline query",
          },
          records: {
            summaries: [
              { level: "L0", text: "Approval and incident evidence for project_1." },
              { level: "L1", text: "Contains GPU purchase approvals, security review decisions, rollback confirmations, and supporting runbooks." },
            ],
            embeddings: [{ ref: "ctx:emb:1001:1001300", model: "all-MiniLM-L6-v2", dim: 384, score: "0.96" }],
            indexes: [
              { name: "event_kind", value: "approval" },
              { name: "project", value: "project_1" },
            ],
          },
        },
        {
          id: "10018891",
          parent: "1001300",
          label: "gpu_purchase_request_8891",
          type: "event leaf",
          depth: 4,
          child_count: 0,
          updated_at_ms: "1781500000500",
          status: "deduped child ref",
          score: "0.92",
          metadata: {
            object_key: "ctx:node:1001:10018891",
            model: "ContextNode + ContextEvent",
            parent_ref: "ctx:child:1001:1001300 -> 10018891",
            event_key: "ctx:event:1001:10018891",
            idempotency: "same child_hash updates latest ref without duplicate edge",
            summary_dirty: "ctx:dirty:1001:10018891",
          },
          records: {
            events: [
              {
                id: "991001",
                type: "approval_confirmation",
                text: "Alice approved GPU purchase request 8891 for project_1.",
                ingestion_time_ms: "1781500000500",
                event_time_ms: "1781499900000",
                confidence: "0.96",
              },
              {
                id: "991002",
                type: "cost_update",
                text: "Finance attached 42000 USD budget for GPU purchase request 8891.",
                ingestion_time_ms: "1781500000600",
                event_time_ms: "1781499950000",
                confidence: "0.91",
              },
            ],
            entities: [
              { name: "GPU purchase request 8891", type: "approval", value: "approved", confidence: "0.96" },
              { name: "budget", type: "cost", value: "42000 USD", confidence: "0.91" },
            ],
            summaries: [{ level: "L0", text: "GPU request 8891 is approved with 42000 USD budget evidence." }],
            embeddings: [{ ref: "ctx:emb:1001:10018891", model: "all-MiniLM-L6-v2", dim: 384, score: "0.92" }],
            indexes: [
              { name: "event_kind", value: "approval_confirmation" },
              { name: "entity", value: "gpu_purchase_request_8891" },
              { name: "status", value: "approved" },
              { name: "event_time_bucket", value: "1781499600000" },
            ],
          },
        },
        {
          id: "10029900",
          parent: "1001300",
          label: "incident_77_postmortem",
          type: "pdf resource",
          depth: 4,
          child_count: 3,
          updated_at_ms: "1781509300000",
          status: "selected",
          score: "0.98",
          metadata: {
            object_key: "ctx:node:1001:10029900",
            model: "ContextNode + resource chunks",
            raw_uri: "incident_77.pdf",
            resource_type: "pdf",
            child_ref_key: "ctx:child:1001:10029900",
            embedding_ref: "ctx:emb:1001:10029900",
          },
          records: {
            events: [
              {
                id: "994020",
                type: "resource_fact",
                text: "Incident INC-77 rollback was stable after stream proxy restart.",
                ingestion_time_ms: "1781509300000",
                event_time_ms: "1781509200000",
                confidence: "0.95",
              },
            ],
            summaries: [
              { level: "L0", text: "INC-77 postmortem confirms rollback stability and source runbook evidence." },
              { level: "L1", text: "PDF resource with page-level chunks covering rollback sequence, confirmation, and postmortem notes." },
            ],
            embeddings: [{ ref: "ctx:emb:1001:10029900", model: "all-MiniLM-L6-v2", dim: 384, score: "0.98" }],
            resources: [{ raw_uri: "incident_77.pdf", type: "pdf", chunks: "3", parser: "pdf + VLM fallback" }],
            indexes: [
              { name: "resource_type", value: "pdf" },
              { name: "event_kind", value: "incident_update" },
              { name: "status", value: "stable" },
            ],
            compression: [
              {
                id: "77001",
                window: "1781500000000..1781509000000",
                summary: "Older INC-77 rollback context compressed; source event ids retained.",
              },
            ],
          },
        },
        {
          id: "10029901",
          parent: "10029900",
          label: "incident_77.pdf#page=1:L0",
          type: "chunk",
          depth: 5,
          child_count: 0,
          updated_at_ms: "1781509300000",
          status: "packed",
          score: "0.94",
          metadata: {
            object_key: "ctx:resource:1001:10029901",
            model: "ResourceChunk + ContextEmbedding",
            embedding_ref: "ctx:emb:1001:10029901",
            pack_role: "selected citation chunk",
          },
          records: {
            resources: [
              {
                raw_uri: "incident_77.pdf",
                source_ref: "page=1:L0",
                text: "INC-77 rollback stable after stream proxy restart; monitor queue drain before close.",
              },
            ],
            embeddings: [{ ref: "ctx:emb:1001:10029901", model: "all-MiniLM-L6-v2", dim: 384, score: "0.94" }],
            indexes: [
              { name: "resource_type", value: "pdf" },
              { name: "raw_uri", value: "incident_77.pdf" },
            ],
          },
        },
      ],
      mock_ingestions: [
        {
          source: "agent_hook.before_llm",
          input: "Alice approved GPU purchase request 8891.",
          writes: "node path + event + entity + indexes + embedding + dirty summary",
          node_hash: "10018891",
          status: "accepted",
        },
        {
          source: "agent_hook.resource_added",
          input: "incident_77.pdf",
          writes: "resource node + chunk nodes + chunk embeddings + resource_fact event",
          node_hash: "10029900",
          status: "accepted",
        },
        {
          source: "agent_hook.after_llm",
          input: "User confirmed INC-77 rollback answer.",
          writes: "feedback ContextEvent + accepted refs + ContextPackAudit",
          node_hash: "10029900",
          status: "confirmed",
        },
      ],
    },
    filesystem: [
      {
        path: "/company_a",
        node_hash: "1001000",
        model: "ContextNode",
        storage: "ctx:node + ctx:child",
        children: "infra_team",
        events: "0",
        summary: "tenant L0 fresh",
        status: "ready",
      },
      {
        path: "/company_a/infra_team/project_1/approvals",
        node_hash: "1001300",
        model: "ContextNode + ContextSummary",
        storage: "ctx:node, ctx:child, ctx:emb",
        children: "5",
        events: "approval refs",
        summary: "collection L0 score 0.96",
        status: "selected",
      },
      {
        path: "/company_a/infra_team/project_1/approvals/gpu_purchase_request_8891",
        node_hash: "10018891",
        model: "ContextNode + ContextEvent + ContextEntity",
        storage: "ctx:event, ctx:entity, ctxidx",
        children: "0",
        events: "991001",
        summary: "dirty marker queued",
        status: "deduped child ref",
      },
      {
        path: "/company_a/infra_team/project_1/approvals/incident_77_postmortem",
        node_hash: "10029900",
        model: "ContextNode + ResourceChunk",
        storage: "ctx:resource, ctx:emb",
        children: "3 chunks",
        events: "994020 extracted",
        summary: "L0/L1 resource ready",
        status: "packed",
      },
    ],
    observations: [
      {
        label: "Node traversal",
        value: "7 nodes",
        detail: "selected path /company_a/infra_team/project_1/approvals/incident_77_postmortem",
        status: "ready",
      },
      {
        label: "Event freshness",
        value: "fresh + compressed",
        detail: "raw confirmations remain queryable; old windows retain source event ids",
        status: "passed",
      },
      {
        label: "Index health",
        value: "AND filters",
        detail: "status + event_type + project refs intersect before timeline reads",
        status: "passed",
      },
      {
        label: "Summary lag",
        value: "watch",
        detail: "dirty markers are visible; L0 refresh runs asynchronously",
        status: "watch",
      },
      {
        label: "Token pressure",
        value: "60 / 70",
        detail: "event and resource chunk fit the context pack budget",
        status: "passed",
      },
      {
        label: "Replay audit",
        value: "query 77027771",
        detail: "selected events, chunks, filters, and pack refs are replayable",
        status: "ready",
      },
    ],
    pack: {
      query: "What confirmed PDF context supports incident INC-77 stable rollback?",
      selected_tokens: "60 / 70",
      events: ["994011 user-confirmed final answer", "994020 PDF abstract fact"],
      chunks: ["10029901 incident_77.pdf#page=1:L0"],
      filters: ["team=infra_team", "project=project_1", "type=confirmation", "resource_type=pdf"],
    },
    alerts: [
      { label: "Traversal deadline", value: "ok", detail: "top_k_per_depth=1, max_candidate_nodes=3" },
      { label: "Dirty summaries", value: "watch", detail: "async L0 refresh pending after ingest" },
      { label: "Token budget", value: "ok", detail: "event + chunk pack below budget" },
      { label: "Embedding model", value: "local", detail: "all-MiniLM-L6-v2 compatible test vectors" },
      { label: "Reranker", value: "optional", detail: "bge-reranker-base after TemporalStore candidate filtering" },
    ],
    audit: [
      { label: "Last query id", value: "77027771" },
      { label: "Selected events", value: "994011, 994020" },
      { label: "Selected chunks", value: "10029901" },
      { label: "Feedback memory", value: "994011" },
      { label: "Parity gate", value: "C++ + Rust unified" },
    ],
    operators: [
      {
        name: "Extract Query",
        status: "ready",
        command: "context_extract_query",
        detail: "derive intent, status, time window, and filters from raw query and hints",
      },
      {
        name: "Traverse Tree",
        status: "ready",
        command: "context_retrieve",
        detail: "score child summaries layer by layer before querying leaf timelines",
      },
      {
        name: "Ingest Resource",
        status: "ready",
        command: "context_ingest_resource",
        detail: "parse markdown, text, and PDF pages into L0/L1/L2-style chunks with source refs",
      },
      {
        name: "Pack Replay",
        status: "ready",
        command: "context_retrieve_with_resources",
        detail: "assemble events and chunks under prompt budget and write audit metadata",
      },
      {
        name: "Feedback Hook",
        status: "ready",
        command: "context_ingest_feedback",
        detail: "store final answer confirmation as future retrievable memory",
      },
      {
        name: "Summary Worker",
        status: "watch",
        command: "context_query_dirty",
        detail: "refresh L0/L1 summaries asynchronously after lightweight writes",
      },
    ],
    safeguards: [
      { label: "Customer input", value: "raw query + hints", detail: "schema complexity stays service-side" },
      { label: "Tenant boundary", value: "tenant_hash", detail: "all tree, event, chunk, and audit records scoped" },
      { label: "Serving threshold", value: "bounded", detail: "depth, children, candidates, and token budget capped" },
      { label: "Historical data", value: "queryable", detail: "old events can remain raw or compressed by time window" },
      { label: "Model choice", value: "pluggable", detail: "OSS local default, provider hook for hosted models" },
      { label: "Model isolation", value: "config driven", detail: "tenant can pin OSS model family and runtime class" },
    ],
    readiness_gates: [
      {
        label: "Contract parity",
        value: "pass",
        severity: "blocker",
        owner: "runtime",
        evidence: "context_nine_ingestion_compression_parity_gates",
        detail: "C++ and Rust unified corpus agree on API idempotency, stream replay, batch, resource, token, compression, and source-audit behavior",
      },
      {
        label: "Tenant isolation",
        value: "pass",
        severity: "blocker",
        owner: "storage",
        evidence: "context object keys include tenant_hash",
        detail: "tenant_hash scopes nodes, child refs, embeddings, events, resources, and audits",
      },
      {
        label: "Latency budgets",
        value: "pass",
        severity: "blocker",
        owner: "serving",
        evidence: "Traversal controls cap depth, fanout, candidates, and deadline",
        detail: "max_depth, top_k_per_depth, max_children_scored, candidates, and deadline are bounded",
      },
      {
        label: "Token budgets",
        value: "pass",
        severity: "high",
        owner: "packing",
        evidence: "context_pack_token_budget_parity",
        detail: "context pack excludes chunks that would exceed max_prompt_tokens",
      },
      {
        label: "Idempotent writes",
        value: "pass",
        severity: "high",
        owner: "ingestion",
        evidence: "retry child-ref corpus checks",
        detail: "retrying child refs preserves one unique edge without a duplicate timeline write",
      },
      {
        label: "Async summaries",
        value: "pass",
        severity: "high",
        owner: "workers",
        evidence: "dirty marker without synchronous parent summary writes",
        detail: "event writes stay lightweight and summary refresh remains dirty-marker driven",
      },
      {
        label: "Replay audit",
        value: "pass",
        severity: "medium",
        owner: "governance",
        evidence: "ContextPackAudit selected refs and query id",
        detail: "selected events, chunks, blocked refs, and prompt budget are replayable by query id",
      },
      {
        label: "Local model fallback",
        value: "pass",
        severity: "medium",
        owner: "model-runtime",
        evidence: "OSS embedding registry and rules-first extraction",
        detail: "OSS embeddings and rule extraction keep Docker/local validation independent of hosted models",
      },
    ],
    ui_readiness_gates: [
      {
        label: "Accessible controls",
        value: "pass",
        severity: "blocker",
        owner: "frontend",
        evidence: "native button, table, details, and summary controls",
        detail: "refresh, topology metadata, tables, and disclosure controls use browser-native accessible elements",
      },
      {
        label: "Responsive layout",
        value: "pass",
        severity: "blocker",
        owner: "frontend",
        evidence: "@media breakpoints collapse grids below tablet width",
        detail: "context panels, query workbench, topology, pack columns, model cards, and operators reflow on narrow screens",
      },
      {
        label: "Overflow guard",
        value: "pass",
        severity: "blocker",
        owner: "frontend",
        evidence: "browser check confirms scrollWidth <= clientWidth",
        detail: "long hashes, model names, evidence commands, and source refs wrap inside their containers",
      },
      {
        label: "Empty state safety",
        value: "pass",
        severity: "high",
        owner: "frontend",
        evidence: "renderers use fallback arrays and text placeholders",
        detail: "missing health fields render as empty sections or '-' instead of throwing",
      },
      {
        label: "Refresh resilience",
        value: "pass",
        severity: "high",
        owner: "frontend",
        evidence: "health fetch has fallbackHealth and status pill updates",
        detail: "dashboard stays usable when health.json cannot be fetched or is partially absent",
      },
      {
        label: "Evidence visibility",
        value: "pass",
        severity: "high",
        owner: "ops",
        evidence: "owner/severity/evidence rendered under every readiness gate",
        detail: "operators can see who owns each gate and which command or signal proves it",
      },
      {
        label: "Deterministic fixture",
        value: "pass",
        severity: "medium",
        owner: "qa",
        evidence: "health.json is validated by unit tests and json.tool",
        detail: "local UI, browser check, and CI-style tests render the same fixture payload",
      },
      {
        label: "Actionable runbook",
        value: "pass",
        severity: "medium",
        owner: "ops",
        evidence: "Runbook panel links commands for corpus, C++ contract, Rust unified, and local serve",
        detail: "the UI exposes the exact commands operators need to reproduce readiness signals",
      },
      {
        label: "Nine-lane parity",
        value: "pass",
        severity: "medium",
        owner: "qa",
        evidence: "context_nine_ingestion_compression_parity_gates",
        detail: "end-to-end parity table renders API, stream, batch, resource, feedback, compression, audit, and C++ lanes",
      },
    ],
    runbook: [
      { label: "Run unified tests", value: "bash tools/run_rust_unified_tests.sh" },
      { label: "Validate corpus", value: "python3 tools/run_temporalstore_unified_tests.py --validate-only" },
      { label: "Run C++ contract", value: "tools/run_cpp_unified_context_contract.sh third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json" },
      { label: "Refresh health", value: "python3 tools/temporalstore-monitoring-ui/render_health_from_results.py" },
      { label: "Serve locally", value: "python3 -m http.server 8080 -d tools/temporalstore-monitoring-ui" },
    ],
  },
};

function byId(id) {
  return document.getElementById(id);
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function text(value, fallback = "-") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function statusClass(status) {
  const normalized = text(status, "unknown").toLowerCase();
  if (["ok", "pass", "passed", "healthy", "ready", "running"].includes(normalized)) {
    return "ok";
  }
  if (["fail", "failed", "bad", "error", "unhealthy", "down"].includes(normalized)) {
    return "bad";
  }
  return "warn";
}

function setText(id, value) {
  const el = byId(id);
  if (el) {
    el.textContent = text(value);
  }
}

function badge(status) {
  const cls = statusClass(status);
  return `<span class="badge ${cls}">${escapeHtml(text(status, "unknown"))}</span>`;
}

function escapeHtml(value) {
  return text(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

const contextTreeUiState = {
  selectedNodeId: "",
  selectedEdgeId: "",
  collapsedNodeIds: new Set(),
};

function renderMetricList(id, rows) {
  const el = byId(id);
  if (!el) {
    return;
  }
  el.innerHTML = rows
    .map(
      (row) => `
        <div class="metric-row">
          <span>${escapeHtml(row.label)}</span>
          <strong>${row.html || escapeHtml(row.value)}</strong>
        </div>
      `,
    )
    .join("");
}

function renderTableRows(id, rows, columns) {
  const el = byId(id);
  if (!el) {
    return;
  }
  el.innerHTML = asArray(rows)
    .map(
      (row) => `
        <tr>
          ${columns.map((col) => `<td>${escapeHtml(row[col] ?? "-")}</td>`).join("")}
        </tr>
      `,
    )
    .join("");
}

function renderNodes(nodes) {
  byId("nodes-body").innerHTML = asArray(nodes)
    .map(
      (node) => `
        <tr>
          <td><strong>${escapeHtml(node.name)}</strong></td>
          <td>${escapeHtml(node.role)}</td>
          <td>${badge(node.status)}</td>
          <td>${escapeHtml(node.endpoint)}</td>
          <td>${escapeHtml(node.cpu)}</td>
          <td>${escapeHtml(node.memory)}</td>
          <td>${escapeHtml(node.storage)}</td>
          <td>${escapeHtml(node.replay)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderScaleTests(tests) {
  byId("scale-tests").innerHTML = asArray(tests)
    .map(
      (test) => `
        <article class="test-card">
          <div class="test-title">
            <strong>${escapeHtml(test.name)}</strong>
            ${badge(test.status)}
          </div>
          <p>${escapeHtml(test.workload)}</p>
          <div class="mini-grid">
            <span>Write QPS<strong>${escapeHtml(test.write_qps)}</strong></span>
            <span>Read p50<strong>${escapeHtml(test.read_p50_ms)}</strong></span>
            <span>Read p99<strong>${escapeHtml(test.read_p99_ms)}</strong></span>
            <span>Replica lag<strong>${escapeHtml(test.secondary_lag_ms)}</strong></span>
          </div>
          <div class="test-meta">
            <span>${escapeHtml(test.threads || "threads -")}</span>
            <span>${escapeHtml(test.result_dir || "result dir -")}</span>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderModules(modules) {
  byId("modules-body").innerHTML = asArray(modules)
    .map(
      (item) => `
        <tr>
          <td><strong>${escapeHtml(item.module)}</strong></td>
          <td>${escapeHtml(item.test)}</td>
          <td>${badge(item.status)}</td>
          <td>${escapeHtml(item.write_path)}</td>
          <td>${escapeHtml(item.read_path)}</td>
          <td>${escapeHtml(item.latency)}</td>
          <td>${escapeHtml(item.notes)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderDataModels(models) {
  const el = byId("data-models");
  if (!el) {
    return;
  }
  el.innerHTML = asArray(models)
    .map(
      (model) => `
        <article class="model-card">
          <div class="model-head">
            <strong>${escapeHtml(model.name)}</strong>
            ${badge(model.status)}
          </div>
          <p>${escapeHtml(model.use_case)}</p>
          <dl>
            <div><dt>Write</dt><dd>${escapeHtml(model.write_shape)}</dd></div>
            <div><dt>Query</dt><dd>${escapeHtml(model.query_shape)}</dd></div>
            <div><dt>Storage</dt><dd>${escapeHtml(model.storage_shape)}</dd></div>
            <div><dt>Consistency</dt><dd>${escapeHtml(model.consistency)}</dd></div>
          </dl>
          <span class="model-test">${escapeHtml(model.test_status)}</span>
        </article>
      `,
    )
    .join("");
}

function renderProducts(products) {
  const el = byId("products-grid");
  if (!el) {
    return;
  }
  el.innerHTML = asArray(products)
    .map(
      (product) => `
        <article class="product-card">
          <div class="product-head">
            <div>
              <strong>${escapeHtml(product.name)}</strong>
              <span>${escapeHtml(product.role)}</span>
            </div>
            ${badge(product.status)}
          </div>
          <p>${escapeHtml(product.summary)}</p>
          <div class="product-metrics">
            <span>Topology<strong>${escapeHtml(product.topology)}</strong></span>
            <span>Endpoint<strong>${escapeHtml(product.endpoint)}</strong></span>
            <span>Latest test<strong>${escapeHtml(product.latest_test)}</strong></span>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderProductServices(services) {
  renderTableRows("product-services-body", services, [
    "product",
    "service",
    "node",
    "status",
    "ports",
    "role",
    "latest_signal",
  ]);
}

function renderContextOps(context) {
  const data = context || fallbackHealth.context_ops;
  const status = data.status || "pending";
  const statusEl = byId("context-runtime-status");
  if (statusEl) {
    statusEl.className = `status-pill ${statusClass(status)}`;
    statusEl.innerHTML = `<span class="dot"></span>${escapeHtml(status)}`;
  }

  const kpis = byId("context-kpis");
  if (kpis) {
    kpis.innerHTML = asArray(data.kpis)
      .map(
        (item) => `
          <article class="context-kpi">
            <span>${escapeHtml(item.label)}</span>
            <strong>${escapeHtml(item.value)}</strong>
            <small>${escapeHtml(item.note)}</small>
          </article>
        `,
      )
      .join("");
  }

  const flow = byId("context-flow");
  if (flow) {
    flow.innerHTML = asArray(data.flow)
      .map(
        (item, index) => `
          <article class="flow-card">
            <span>${String(index + 1).padStart(2, "0")}</span>
            <div>
              <strong>${escapeHtml(item.name)}</strong>
              <p>${escapeHtml(item.detail)}</p>
            </div>
            ${badge(item.status)}
          </article>
        `,
      )
      .join("");
  }

  renderScaleTestsInto("context-tests", data.tests);
  renderOpsWorkspace(data);
  renderDataPlane(data);
  renderResourceSkillOps(data);
  renderTableRows("context-pipeline-body", data.pipeline, [
    "step",
    "input",
    "writes",
    "query",
    "output",
    "status",
  ]);
  renderTableRows("context-e2e-parity-body", data.e2e_parity_runs, [
    "run",
    "covers",
    "evidence",
    "output",
    "status",
  ]);

  const requests = byId("context-request-builder");
  if (requests) {
    requests.innerHTML = asArray(data.request_builder)
      .map(
        (request) => `
          <article class="request-card">
            <div>
              <strong>${escapeHtml(request.name)}</strong>
              <span>${escapeHtml(request.method)} ${escapeHtml(request.path)}</span>
            </div>
            <code>${escapeHtml(request.body)}</code>
          </article>
        `,
      )
      .join("");
  }

  const tree = byId("context-tree");
  if (tree) {
    const topology = hydrateContextTopology(data.topology || {});
    tree.__matrixarkTopology = topology;
    const selectedPath = new Set(asArray(topology.selected_path).map(String));
    const topologyNodes = asArray(topology.nodes);
    const selectedNode = getSelectedTopologyNode(topologyNodes, selectedPath);
    tree.innerHTML = `
      <h3>Node Topology</h3>
      ${topology.summary ? `<p class="topology-summary">${escapeHtml(topology.summary)}</p>` : ""}
      ${
        topologyNodes.length
          ? renderTopologyWorkspace(topology, selectedPath, selectedNode)
          : ""
      }
      <h3 class="subsection-title">Traversal Path</h3>
      ${asArray(data.tree)
        .map(
          (node) => `
            <div class="tree-node" style="--depth:${Number(node.depth) || 0}">
              <strong>${escapeHtml(node.label)}</strong>
              <span>${escapeHtml(node.meta)}</span>
            </div>
          `,
        )
        .join("")}
    `;
  }

  const pack = data.pack || {};
  const packEl = byId("context-pack");
  if (packEl) {
    packEl.innerHTML = `
      <h3>Context Pack</h3>
      <div class="pack-query">${escapeHtml(pack.query)}</div>
      <div class="pack-budget">${escapeHtml(pack.selected_tokens || "-")}</div>
      <div class="pack-columns">
        ${renderPackGroup("Events", pack.events)}
        ${renderPackGroup("Chunks", pack.chunks)}
        ${renderPackGroup("Filters", pack.filters)}
      </div>
    `;
  }

  renderFilesystemExplorer(data.filesystem, data.topology);
  wireContextTreeControls(hydrateContextTopology(data.topology || {}));
  renderContextObservation(data.observations);
  renderQueryWorkbench(data.query_workbench);
  renderContextConfig(data.config);
  renderModelRegistry(data.model_registry);
  renderContextOperators(data.operators);
  renderMetricList(
    "context-alerts",
    asArray(data.alerts).map((item) => ({
      label: item.label,
      html: `${badge(item.value)} <small>${escapeHtml(item.detail || "")}</small>`,
    })),
  );
  renderMetricList("context-audit", data.audit);
  renderMetricList("context-runbook", data.runbook);
  renderMetricList(
    "context-safeguards",
    [
      ...asArray(data.safeguards),
      ...asArray(data.readiness_gates).map((item) => ({
        ...item,
        label: `Gate: ${item.label}`,
      })),
      ...asArray(data.ui_readiness_gates).map((item) => ({
        ...item,
        label: `UI Gate: ${item.label}`,
      })),
    ].map((item) => ({
      label: item.label,
      html: renderSafeguardHtml(item),
    })),
  );
  const uiReadiness = byId("context-ui-readiness");
  const uiReadinessSummary = byId("context-ui-readiness-summary");
  if (uiReadinessSummary) {
    uiReadinessSummary.innerHTML = renderUiReadinessSummary(data);
  }
  if (uiReadiness) {
    uiReadiness.innerHTML = asArray(data.ui_readiness_gates)
      .map(
        (gate, index) => `
          <article class="ui-readiness-card">
            <div>
              <span>${String(index + 1).padStart(2, "0")}</span>
              ${badge(gate.value)}
            </div>
            <strong>${escapeHtml(gate.label)}</strong>
            <p>${escapeHtml(gate.detail || "")}</p>
            <code>${escapeHtml(`owner=${text(gate.owner)} / severity=${text(gate.severity)} / evidence=${text(gate.evidence)}`)}</code>
          </article>
        `,
      )
      .join("");
  }
}

function renderOpsWorkspace(data) {
  const el = byId("context-ops-workspace");
  if (!el) {
    return;
  }
  const passedE2e = asArray(data.e2e_parity_runs).filter((item) => text(item.status).toLowerCase() === "passed").length;
  const configGroups = asArray(data.config).length;
  const modelRoles = asArray(data.model_registry).length;
  const operatorCount = asArray(data.operators).length;
  const evidenceCount = asArray(data.audit).length + asArray(data.alerts).length + asArray(data.readiness_gates).length + asArray(data.ui_readiness_gates).length;
  const cards = [
    {
      label: "Operations",
      href: "#operations",
      value: `${operatorCount} operators`,
      detail: "runbook, safeguards, alerts, replay audit",
      status: data.status || "pending",
    },
    {
      label: "Configurations",
      href: "#configuration",
      value: `${configGroups} groups / ${modelRoles} model roles`,
      detail: "runtime knobs, local OSS models, provider choices",
      status: "ready",
    },
    {
      label: "Testing",
      href: "#testing",
      value: `${passedE2e}/${asArray(data.e2e_parity_runs).length} e2e passed`,
      detail: "pipeline cases, parity lanes, request builder",
      status: passedE2e === asArray(data.e2e_parity_runs).length ? "passed" : "watch",
    },
    {
      label: "Evidence",
      href: "#evidence",
      value: `${evidenceCount} signals`,
      detail: "topology, context pack, alerts, audit, readiness",
      status: "ready",
    },
  ];
  el.innerHTML = cards
    .map(
      (card) => `
        <a class="ops-workspace-card" href="${escapeHtml(card.href)}">
          <div>
            <span>${escapeHtml(card.label)}</span>
            ${badge(card.status)}
          </div>
          <strong>${escapeHtml(card.value)}</strong>
          <small>${escapeHtml(card.detail)}</small>
        </a>
      `,
    )
    .join("");
}

function renderDataPlane(data) {
  const el = byId("context-data-plane");
  if (!el) {
    return;
  }
  const pipeline = asArray(data.pipeline);
  const tests = asArray(data.tests);
  const topologyNodes = asArray(data.topology?.nodes);
  const pack = data.pack || {};
  const fallbackCards = [
    {
      label: "Context Nodes",
      value: `${topologyNodes.length} visible`,
      detail: "tenant/team/project/collection/resource/chunk topology with hashes and parent refs",
      evidence: "ContextNode + ChildRef",
      status: topologyNodes.length ? "ready" : "watch",
    },
    {
      label: "Events",
      value: `${countMatches(pipeline, "contextevent")} writes`,
      detail: "timestamped event leaves, feedback events, incident facts, and approval facts",
      evidence: "ContextEvent",
      status: "passed",
    },
    {
      label: "Extractions",
      value: `${countMatches(pipeline, "extracted") + countMatches(tests, "extraction")} lanes`,
      detail: "raw query/event/resource text into event type, status, filters, and time hints",
      evidence: "rules + optional LLM",
      status: "passed",
    },
    {
      label: "Ingestions",
      value: `${countMatches(pipeline, "ingest")} pipeline steps`,
      detail: "raw event, resource, feedback, API, stream, and batch ingestion paths",
      evidence: "idempotency + offsets",
      status: "passed",
    },
    {
      label: "Resources",
      value: `${countMatches(pipeline, "resource")} lanes`,
      detail: "markdown/text/pdf chunks, raw_uri refs, resource filters, and chunk embeddings",
      evidence: "ResourceChunk + embedding",
      status: "passed",
    },
    {
      label: "Feedback",
      value: `${countMatches(pipeline, "feedback")} path`,
      detail: "final answer confirmation becomes future retrievable memory",
      evidence: "feedback ContextEvent",
      status: "passed",
    },
    {
      label: "Summaries",
      value: `${countMatches(pipeline, "compression")} compression lanes`,
      detail: "dirty summary markers, non-destructive time compression, source event audit",
      evidence: "ContextCompressionEvent",
      status: "passed",
    },
    {
      label: "Context Packs",
      value: text(pack.selected_tokens || "audited"),
      detail: "token-budgeted events, chunks, filters, replay metadata, and pack audit",
      evidence: "ContextPackAudit",
      status: "passed",
    },
  ];
  const cards = asArray(data.data_plane).length ? data.data_plane : fallbackCards;
  el.innerHTML = cards
    .map(
      (card) => `
        <article class="data-plane-card">
          <div>
            <span>${escapeHtml(card.label)}</span>
            ${badge(card.status)}
          </div>
          <strong>${escapeHtml(card.value)}</strong>
          <p>${escapeHtml(card.detail)}</p>
          <code>${escapeHtml(card.evidence)}</code>
        </article>
      `,
    )
    .join("");
}

function countMatches(items, needle) {
  const pattern = needle.toLowerCase();
  return asArray(items).filter((item) => JSON.stringify(item).toLowerCase().includes(pattern)).length;
}

function defaultResourceSkillOps() {
  return {
    status: "watch",
    import_tasks: [],
    parse_warnings: [],
    resource_tree: [],
    chunk_preview: [],
    skill_registry: [],
    version_history: [],
    summary_lag: [],
    retrieval_replay: [],
  };
}

function renderResourceSkillOps(data) {
  const ops = {
    ...defaultResourceSkillOps(),
    ...(data.resource_skill_ops || {}),
  };
  const statusEl = byId("resource-skill-status");
  if (statusEl) {
    statusEl.className = `status-pill ${statusClass(ops.status)}`;
    statusEl.innerHTML = `<span class="dot"></span>${escapeHtml(ops.status)}`;
  }
  renderCardStack("resource-import-tasks", ops.import_tasks, [
    ["task_id", "task"],
    ["raw_uri", "raw uri"],
    ["status", "status"],
    ["progress", "progress"],
    ["chunks", "chunks"],
    ["warnings", "warnings"],
    ["summary_status", "summary"],
  ]);
  renderCompactRecords("resource-parse-warnings", ops.parse_warnings, [
    ["raw_uri", "raw uri"],
    ["parser", "parser"],
    ["warning", "warning"],
    ["action", "action"],
  ]);
  renderCardStack("resource-tree-view", ops.resource_tree, [
    ["path", "path"],
    ["resource_id", "resource"],
    ["resource_type", "type"],
    ["version", "version"],
    ["scope", "scope"],
    ["chunks", "chunks"],
    ["indexes", "indexes"],
  ]);
  renderCardStack("resource-chunk-preview", ops.chunk_preview, [
    ["chunk_id", "chunk"],
    ["source_ref", "source"],
    ["unit_kind", "unit"],
    ["tokens", "tokens"],
    ["selected", "selected"],
    ["reason", "reason"],
    ["text", "preview"],
  ]);
  renderCardStack("skill-registry-view", ops.skill_registry, [
    ["skill_id", "skill"],
    ["version", "version"],
    ["status", "status"],
    ["precedence", "precedence"],
    ["scope", "scope"],
    ["triggers", "triggers"],
    ["allowed_tools", "tools"],
  ]);
  renderCompactRecords("resource-version-history", ops.version_history, [
    ["raw_uri", "raw uri"],
    ["version", "version"],
    ["content_hash", "hash"],
    ["supersedes", "supersedes"],
    ["normal_retrieval", "latest-version filter"],
  ]);
  renderCompactRecords("resource-summary-lag", ops.summary_lag, [
    ["node_path", "node"],
    ["dirty_reason", "reason"],
    ["lag_ms", "lag ms"],
    ["status", "status"],
  ]);
  renderCardStack("resource-retrieval-replay", ops.retrieval_replay, [
    ["query_id", "query"],
    ["selected_refs", "selected"],
    ["dropped_refs", "dropped"],
    ["audit_ref", "audit"],
    ["reason", "reason"],
  ]);
}

function renderCardStack(id, records, fields) {
  const el = byId(id);
  if (!el) {
    return;
  }
  const items = asArray(records);
  if (!items.length) {
    el.innerHTML = `<p class="empty-records">No records yet.</p>`;
    return;
  }
  el.innerHTML = items
    .map(
      (record) => `
        <article class="ops-mini-card">
          <dl>
            ${fields.map(([key, label]) => renderMetadataRow(label, record[key])).join("")}
          </dl>
        </article>
      `,
    )
    .join("");
}

function renderCompactRecords(id, records, fields) {
  const el = byId(id);
  if (!el) {
    return;
  }
  const items = asArray(records);
  if (!items.length) {
    el.innerHTML = `<p class="empty-records">No records yet.</p>`;
    return;
  }
  el.innerHTML = items
    .map(
      (record) => `
        <article class="compact-record-card">
          ${fields.map(([key, label]) => renderMetadataRow(label, record[key])).join("")}
        </article>
      `,
    )
    .join("");
}

function renderMetadataRow(key, value) {
  const rendered = Array.isArray(value) ? value.join(", ") : value ?? "-";
  return `
    <div class="metadata-line">
      <dt>${escapeHtml(key)}</dt>
      <dd>${escapeHtml(rendered)}</dd>
    </div>
  `;
}

function hydrateContextTopology(topology) {
  const fallbackTopology = fallbackHealth.context_ops?.topology || {};
  const fallbackById = new Map(asArray(fallbackTopology.nodes).map((node) => [String(node.id), node]));
  const sourceNodes = asArray(topology.nodes).length ? topology.nodes : fallbackTopology.nodes;
  return {
    ...fallbackTopology,
    ...topology,
    selected_path: asArray(topology.selected_path).length ? topology.selected_path : fallbackTopology.selected_path,
    mock_ingestions: asArray(topology.mock_ingestions).length ? topology.mock_ingestions : fallbackTopology.mock_ingestions,
    nodes: asArray(sourceNodes).map((node) => {
      const fallbackNode = fallbackById.get(String(node.id)) || {};
      return {
        ...fallbackNode,
        ...node,
        metadata: {
          ...(fallbackNode.metadata || {}),
          ...(node.metadata || {}),
        },
        records: Object.keys(node.records || {}).length ? node.records : fallbackNode.records,
      };
    }),
  };
}

function renderUiReadinessSummary(data) {
  const runtimeGates = asArray(data.readiness_gates);
  const uiGates = asArray(data.ui_readiness_gates);
  const allGates = [...runtimeGates, ...uiGates];
  const passed = allGates.filter((gate) => text(gate.value).toLowerCase() === "pass").length;
  const blockers = allGates.filter((gate) => text(gate.severity).toLowerCase() === "blocker").length;
  const evidence = allGates.filter((gate) => text(gate.evidence).trim() !== "").length;
  const runbook = asArray(data.runbook).length;
  const status = allGates.length > 0 && passed === allGates.length ? "pass" : "watch";
  const items = [
    { label: "Gates passed", value: `${passed}/${allGates.length}` },
    { label: "Blockers covered", value: `${blockers}` },
    { label: "Evidence linked", value: `${evidence}/${allGates.length}` },
    { label: "Runbook commands", value: `${runbook}` },
  ];
  return `
    <div class="ui-readiness-summary-head">
      <strong>Production posture</strong>
      ${badge(status)}
    </div>
    <div class="ui-readiness-summary-grid">
      ${items
        .map(
          (item) => `
            <span>
              <small>${escapeHtml(item.label)}</small>
              <b>${escapeHtml(item.value)}</b>
            </span>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderSafeguardHtml(item) {
  const extras = [];
  if (item.owner) {
    extras.push(`owner=${text(item.owner)}`);
  }
  if (item.severity) {
    extras.push(`severity=${text(item.severity)}`);
  }
  if (item.evidence) {
    extras.push(`evidence=${text(item.evidence)}`);
  }
  return `
    <strong>${escapeHtml(item.value || "-")}</strong>
    <small>${escapeHtml(item.detail || "")}</small>
    ${extras.length ? `<code>${escapeHtml(extras.join(" / "))}</code>` : ""}
  `;
}

function getSelectedTopologyNode(nodes, selectedPath) {
  const bySelectedState = nodes.find((node) => text(node.status).toLowerCase() === "selected");
  if (contextTreeUiState.selectedNodeId) {
    const manuallySelected = nodes.find((node) => String(node.id) === contextTreeUiState.selectedNodeId);
    if (manuallySelected) {
      return manuallySelected;
    }
  }
  if (bySelectedState) {
    contextTreeUiState.selectedNodeId = String(bySelectedState.id);
    return bySelectedState;
  }
  const selectedPathNodes = nodes.filter((node) => selectedPath.has(String(node.id)));
  const deepestSelected = selectedPathNodes.sort((a, b) => (Number(b.depth) || 0) - (Number(a.depth) || 0))[0];
  const fallbackNode = deepestSelected || nodes[0] || null;
  contextTreeUiState.selectedNodeId = fallbackNode ? String(fallbackNode.id) : "";
  return fallbackNode;
}

function renderTopologyWorkspace(topology, selectedPath, selectedNode) {
  return `
    <div class="topology-actions" aria-label="Context graph controls">
      <button class="topology-action" type="button" data-tree-action="expand-all">Expand All</button>
      <button class="topology-action secondary" type="button" data-tree-action="collapse-all">Collapse All</button>
      <span>${escapeHtml(asArray(topology.nodes).length)} nodes / ${escapeHtml(asArray(topology.mock_ingestions).length)} mock ingestions</span>
    </div>
    <div class="topology-workspace">
      <div class="topology-map interactive" aria-label="Interactive context node graph">
        ${renderTopologyGraph(topology, selectedPath)}
      </div>
      <aside id="context-node-detail" class="context-node-detail" aria-live="polite">
        ${contextTreeUiState.selectedEdgeId ? renderEdgeDetailById(contextTreeUiState.selectedEdgeId, topology) : renderNodeDetail(selectedNode, topology)}
      </aside>
    </div>
  `;
}

function renderTopologyGraph(topology, selectedPath) {
  const graph = buildTopologyGraph(topology);
  const visibleNodes = graph.nodes.filter((node) => !hasCollapsedAncestor(node, graph.nodeById));
  const visibleIds = new Set(visibleNodes.map((node) => String(node.id)));
  const visibleEdges = graph.edges.filter((edge) => visibleIds.has(edge.parentId) && visibleIds.has(edge.childId));
  const maxDepth = Math.max(0, ...visibleNodes.map((node) => Number(node.depth) || 0));
  const laneHeight = 92;
  const laneWidth = 220;
  const width = Math.max(760, (maxDepth + 1) * laneWidth + 120);
  const height = Math.max(360, visibleNodes.length * laneHeight + 80);
  visibleNodes.forEach((node, index) => {
    node.__x = 44 + (Number(node.depth) || 0) * laneWidth;
    node.__y = 40 + index * laneHeight;
  });
  const selectedNodeId = contextTreeUiState.selectedNodeId;
  const selectedEdgeId = contextTreeUiState.selectedEdgeId;
  return `
    <div class="graph-shell">
      <svg class="topology-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="MatrixArk context tree graph">
        <defs>
          <marker id="edge-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z"></path>
          </marker>
        </defs>
        <g class="graph-edges">
          ${visibleEdges.map((edge) => renderGraphEdge(edge, graph.nodeById, selectedEdgeId)).join("")}
        </g>
        <g class="graph-nodes">
          ${visibleNodes.map((node) => renderGraphNode(node, graph.childrenByParent, selectedPath, selectedNodeId)).join("")}
        </g>
      </svg>
      <div class="graph-legend">
        <span><b></b> selected path</span>
        <span><b></b> selected item</span>
        <span>Click node for data. Click edge for parent-child metadata.</span>
      </div>
    </div>
    <div class="graph-edge-table">
      <h4>Visible Edges</h4>
      ${visibleEdges
        .map((edge) => {
          const parent = graph.nodeById.get(edge.parentId);
          const child = graph.nodeById.get(edge.childId);
          const active = edge.id === selectedEdgeId ? " active" : "";
          return `
            <button class="edge-row${active}" type="button" data-edge-id="${escapeHtml(edge.id)}">
              <span>${escapeHtml(parent?.label || edge.parentId)} -> ${escapeHtml(child?.label || edge.childId)}</span>
              <small>${escapeHtml(edge.metadata.storage_key || edge.metadata.parent_ref || edge.id)}</small>
            </button>
          `;
        })
        .join("")}
    </div>
  `;
}

function buildTopologyGraph(topology) {
  const nodes = asArray(topology.nodes).map((node) => ({ ...node, id: String(node.id), parent: text(node.parent, "") }));
  const nodeById = new Map(nodes.map((node) => [String(node.id), node]));
  const childrenByParent = new Map();
  nodes.forEach((node) => {
    const parentKey = text(node.parent, "");
    if (!childrenByParent.has(parentKey)) {
      childrenByParent.set(parentKey, []);
    }
    childrenByParent.get(parentKey).push(node);
  });
  childrenByParent.forEach((children) => {
    children.sort((a, b) => (Number(a.depth) || 0) - (Number(b.depth) || 0) || text(a.label || a.id).localeCompare(text(b.label || b.id)));
  });
  const edges = nodes
    .filter((node) => node.parent && nodeById.has(String(node.parent)))
    .map((node) => {
      const parent = nodeById.get(String(node.parent));
      const edgeId = `${node.parent}->${node.id}`;
      return {
        id: edgeId,
        parentId: String(node.parent),
        childId: String(node.id),
        metadata: buildEdgeMetadata(parent, node, edgeId),
      };
    });
  return { nodes, nodeById, childrenByParent, edges };
}

function buildEdgeMetadata(parent, child, edgeId) {
  const childMetadata = child?.metadata || {};
  const parentMetadata = parent?.metadata || {};
  return {
    edge_id: edgeId,
    parent_hash: parent?.id || "",
    parent_label: parent?.label || "",
    child_hash: child?.id || "",
    child_label: child?.label || "",
    child_type: child?.type || "node",
    child_depth: text(child?.depth, "0"),
    storage_model: "ContextChildRef",
    storage_key: parentMetadata.child_ref_key || `ctx:child:tenant:${parent?.id || "root"}`,
    parent_ref: childMetadata.parent_ref || `${parent?.id || "root"} -> ${child?.id || ""}`,
    updated_at_ms: child?.updated_at_ms || parent?.updated_at_ms || "",
    child_status: child?.status || "",
    child_object_key: childMetadata.object_key || "",
    child_embedding_ref: childMetadata.embedding_ref || "",
    query_role: childMetadata.query_role || childMetadata.pack_role || "",
  };
}

function hasCollapsedAncestor(node, nodeById) {
  let cursor = node;
  while (cursor?.parent) {
    if (contextTreeUiState.collapsedNodeIds.has(String(cursor.parent))) {
      return true;
    }
    cursor = nodeById.get(String(cursor.parent));
  }
  return false;
}

function renderGraphEdge(edge, nodeById, selectedEdgeId) {
  const parent = nodeById.get(edge.parentId);
  const child = nodeById.get(edge.childId);
  if (!parent || !child) {
    return "";
  }
  const x1 = parent.__x + 156;
  const y1 = parent.__y + 31;
  const x2 = child.__x;
  const y2 = child.__y + 31;
  const midX = x1 + Math.max(42, (x2 - x1) / 2);
  const active = edge.id === selectedEdgeId ? " selected" : "";
  const labelX = (x1 + x2) / 2;
  const labelY = (y1 + y2) / 2 - 7;
  const path = `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`;
  return `
    <g class="graph-edge${active}" data-edge-id="${escapeHtml(edge.id)}">
      <path class="edge-hit" d="${path}"></path>
      <path class="edge-line" d="${path}" marker-end="url(#edge-arrow)"></path>
      <text x="${labelX}" y="${labelY}">${escapeHtml(edge.metadata.storage_model)}</text>
    </g>
  `;
}

function renderGraphNode(node, childrenByParent, selectedPath, selectedNodeId) {
  const children = childrenByParent.get(String(node.id)) || [];
  const hasChildren = children.length > 0;
  const collapsed = contextTreeUiState.collapsedNodeIds.has(String(node.id));
  const selected = String(node.id) === String(selectedNodeId);
  const inSelectedPath = selectedPath.has(String(node.id));
  const classes = ["graph-node"];
  if (selected) classes.push("selected");
  if (inSelectedPath) classes.push("in-path");
  if (hasChildren) classes.push("has-children");
  const status = text(node.status, "ready");
  const title = text(node.label || node.id);
  const shortTitle = title.length > 24 ? `${title.slice(0, 21)}...` : title;
  const meta = `${text(node.type, "node")} / ${text(node.id)}`;
  return `
    <g class="${classes.join(" ")}" transform="translate(${node.__x}, ${node.__y})" data-node-id="${escapeHtml(node.id)}">
      <rect width="158" height="62" rx="8"></rect>
      <text class="node-title" x="12" y="22">${escapeHtml(shortTitle)}</text>
      <text class="node-meta" x="12" y="41">${escapeHtml(meta)}</text>
      <text class="node-status" x="12" y="55">${escapeHtml(status)}</text>
      ${
        hasChildren
          ? `<g class="graph-toggle" data-toggle-id="${escapeHtml(node.id)}" transform="translate(136, 10)">
              <circle r="10"></circle>
              <text text-anchor="middle" y="4">${collapsed ? "+" : "-"}</text>
            </g>`
          : ""
      }
    </g>
  `;
}

function renderEdgeDetailById(edgeId, topology) {
  const graph = buildTopologyGraph(topology || {});
  const edge = graph.edges.find((item) => item.id === edgeId);
  if (!edge) {
    contextTreeUiState.selectedEdgeId = "";
    const selectedPath = new Set(asArray(topology?.selected_path).map(String));
    return renderNodeDetail(getSelectedTopologyNode(asArray(topology?.nodes), selectedPath), topology);
  }
  return `
    <h3>Edge Details</h3>
    <div class="node-detail-head">
      <div>
        <strong>${escapeHtml(edge.metadata.parent_label)} -> ${escapeHtml(edge.metadata.child_label)}</strong>
        <span>${escapeHtml(edge.metadata.storage_model)} / ${escapeHtml(edge.id)}</span>
      </div>
      ${badge(edge.metadata.child_status || "ready")}
    </div>
    <dl class="node-detail-grid">
      ${Object.entries(edge.metadata).map(([key, value]) => renderDetailCell(key.replaceAll("_", " "), value)).join("")}
    </dl>
  `;
}

function renderInteractiveTopology(nodes, selectedPath) {
  const childrenByParent = new Map();
  asArray(nodes).forEach((node) => {
    const parentKey = text(node.parent, "");
    if (!childrenByParent.has(parentKey)) {
      childrenByParent.set(parentKey, []);
    }
    childrenByParent.get(parentKey).push(node);
  });
  childrenByParent.forEach((children) => {
    children.sort((a, b) => {
      const depthDelta = (Number(a.depth) || 0) - (Number(b.depth) || 0);
      return depthDelta || text(a.label || a.id).localeCompare(text(b.label || b.id));
    });
  });
  const roots = childrenByParent.get("") || asArray(nodes).filter((node) => !node.parent);
  return roots.map((node) => renderTopologyBranch(node, childrenByParent, selectedPath)).join("");
}

function renderTopologyBranch(node, childrenByParent, selectedPath) {
  const children = childrenByParent.get(String(node.id)) || [];
  const selected = contextTreeUiState.selectedNodeId === String(node.id);
  const inSelectedPath = selectedPath.has(String(node.id));
  const depth = Math.max(0, Number(node.depth) || 0);
  const shouldOpen = inSelectedPath || depth < 3;
  return `
    <details class="topology-branch" data-node-id="${escapeHtml(node.id)}" ${shouldOpen ? "open" : ""}>
      <summary>
        ${renderTopologyNode(node, selected, inSelectedPath)}
      </summary>
      <div class="topology-branch-tools">
        <button class="topology-inspect" type="button" data-node-id="${escapeHtml(node.id)}">Inspect Node Data</button>
      </div>
      ${
        children.length
          ? `<div class="topology-children">${children.map((child) => renderTopologyBranch(child, childrenByParent, selectedPath)).join("")}</div>`
          : ""
      }
    </details>
  `;
}

function renderTopologyNode(node, selected, inSelectedPath) {
  const depth = Math.max(0, Number(node.depth) || 0);
  const classes = ["topology-node"];
  if (selected) {
    classes.push("selected");
  }
  if (inSelectedPath) {
    classes.push("in-selected-path");
  }
  const metadata = node.metadata || {};
  const parent = node.parent ? `parent ${node.parent}` : "root";
  const details = [
    { label: "node hash", value: node.id },
    { label: "parent", value: node.parent || "root" },
    { label: "type", value: node.type || "node" },
    { label: "children", value: text(node.child_count, "0") },
    { label: "updated", value: text(node.updated_at_ms) },
    { label: "score", value: text(node.score) },
    { label: "object key", value: metadata.object_key },
    { label: "child ref", value: metadata.child_ref_key || metadata.parent_ref },
    { label: "embedding", value: metadata.embedding_ref },
    { label: "filter", value: metadata.serving_filter || metadata.resource_type || metadata.raw_uri },
  ].filter((item) => text(item.value, "") !== "");
  return `
    <article class="${classes.join(" ")}" style="--depth:${depth}" data-node-id="${escapeHtml(node.id)}">
      <div class="topology-main">
        <span class="topology-depth">L${depth}</span>
        <div>
          <strong>${escapeHtml(node.label || node.id)}</strong>
          <small>${escapeHtml(node.type || "node")} / ${escapeHtml(parent)}</small>
        </div>
      </div>
      <div class="topology-flags">
        <span class="topology-state">${escapeHtml(node.status || "ready")}</span>
        ${inSelectedPath ? `<span class="topology-selected">selected path</span>` : ""}
      </div>
      <dl class="topology-detail-grid">
        ${details
          .map(
            (item) => `
              <div>
                <dt>${escapeHtml(item.label)}</dt>
                <dd>${escapeHtml(text(item.value))}</dd>
              </div>
            `,
          )
          .join("")}
      </dl>
      <details class="topology-metadata" onclick="event.stopPropagation()">
        <summary>Metadata Details (${Object.keys(metadata).length})</summary>
        <dl>${renderMetadataRows(metadata)}</dl>
      </details>
    </article>
  `;
}

function renderNodeDetail(node, topology) {
  if (!node) {
    return `
      <h3>Node Details</h3>
      <p class="topology-summary">No node selected.</p>
    `;
  }
  const metadata = node.metadata || {};
  const records = Object.keys(node.records || {}).length ? node.records : defaultMockRecordsForNode(node);
  const ingestions = [
    ...asArray(topology?.mock_ingestions),
    ...defaultMockIngestionsForNode(node),
  ].filter((item, index, all) => String(item.node_hash) === String(node.id) && all.findIndex((candidate) => candidate.source === item.source && candidate.input === item.input) === index);
  return `
    <h3>Node Details</h3>
    <div class="node-detail-head">
      <div>
        <strong>${escapeHtml(node.label || node.id)}</strong>
        <span>${escapeHtml(node.type || "ContextNode")} / ${escapeHtml(node.id)}</span>
      </div>
      ${badge(node.status || "ready")}
    </div>
    <dl class="node-detail-grid">
      ${renderDetailCell("Parent", node.parent || "root")}
      ${renderDetailCell("Children", text(node.child_count, "0"))}
      ${renderDetailCell("Updated", text(node.updated_at_ms))}
      ${renderDetailCell("Score", text(node.score))}
      ${renderDetailCell("Object Key", metadata.object_key)}
      ${renderDetailCell("Embedding", metadata.embedding_ref)}
    </dl>
    ${renderRecordSection("Mock Ingestions", ingestions, ["source", "input", "writes", "status"])}
    ${renderRecordSection("Events", records.events, ["id", "type", "text", "ingestion_time_ms", "event_time_ms", "confidence"])}
    ${renderRecordSection("Entities", records.entities, ["name", "type", "value", "confidence"])}
    ${renderRecordSection("Summaries", records.summaries, ["level", "text"])}
    ${renderRecordSection("Embeddings", records.embeddings, ["ref", "model", "dim", "score"])}
    ${renderRecordSection("Indexes", records.indexes, ["name", "value"])}
    ${renderRecordSection("Resources", records.resources, ["raw_uri", "source_ref", "type", "chunks", "parser", "text"])}
    ${renderRecordSection("Compression", records.compression, ["id", "window", "summary"])}
    <details class="topology-metadata detail-metadata" open>
      <summary>Metadata Details (${Object.keys(metadata).length})</summary>
      <dl>${renderMetadataRows(metadata)}</dl>
    </details>
  `;
}

function defaultMockIngestionsForNode(node) {
  const id = String(node?.id || "");
  const defaults = {
    "10018891": [
      {
        source: "agent_hook.before_llm",
        input: "Alice approved GPU purchase request 8891.",
        writes: "node path + event + entity + indexes + embedding + dirty summary",
        node_hash: "10018891",
        status: "accepted",
      },
    ],
    "10029900": [
      {
        source: "agent_hook.resource_added",
        input: "incident_77.pdf",
        writes: "resource node + chunk nodes + chunk embeddings + resource_fact event",
        node_hash: "10029900",
        status: "accepted",
      },
      {
        source: "agent_hook.after_llm",
        input: "User confirmed INC-77 rollback answer.",
        writes: "feedback ContextEvent + accepted refs + ContextPackAudit",
        node_hash: "10029900",
        status: "confirmed",
      },
    ],
  };
  return defaults[id] || [];
}

function defaultMockRecordsForNode(node) {
  const id = String(node?.id || "");
  if (id === "10018891") {
    return {
      events: [
        {
          id: "991001",
          type: "approval_confirmation",
          text: "Alice approved GPU purchase request 8891 for project_1.",
          ingestion_time_ms: "1781500000500",
          event_time_ms: "1781499900000",
          confidence: "0.96",
        },
        {
          id: "991002",
          type: "cost_update",
          text: "Finance attached 42000 USD budget for GPU purchase request 8891.",
          ingestion_time_ms: "1781500000600",
          event_time_ms: "1781499950000",
          confidence: "0.91",
        },
      ],
      entities: [
        { name: "GPU purchase request 8891", type: "approval", value: "approved", confidence: "0.96" },
        { name: "budget", type: "cost", value: "42000 USD", confidence: "0.91" },
      ],
      summaries: [{ level: "L0", text: "GPU request 8891 is approved with 42000 USD budget evidence." }],
      embeddings: [{ ref: "ctx:emb:1001:10018891", model: "all-MiniLM-L6-v2", dim: 384, score: "0.92" }],
      indexes: [
        { name: "event_kind", value: "approval_confirmation" },
        { name: "entity", value: "gpu_purchase_request_8891" },
        { name: "status", value: "approved" },
        { name: "event_time_bucket", value: "1781499600000" },
      ],
    };
  }
  if (id === "10029900") {
    return {
      events: [
        {
          id: "994020",
          type: "resource_fact",
          text: "Incident INC-77 rollback was stable after stream proxy restart.",
          ingestion_time_ms: "1781509300000",
          event_time_ms: "1781509200000",
          confidence: "0.95",
        },
      ],
      summaries: [
        { level: "L0", text: "INC-77 postmortem confirms rollback stability and source runbook evidence." },
        { level: "L1", text: "PDF resource with page-level chunks covering rollback sequence, confirmation, and postmortem notes." },
      ],
      embeddings: [{ ref: "ctx:emb:1001:10029900", model: "all-MiniLM-L6-v2", dim: 384, score: "0.98" }],
      resources: [{ raw_uri: "incident_77.pdf", type: "pdf", chunks: "3", parser: "pdf + VLM fallback" }],
      indexes: [
        { name: "resource_type", value: "pdf" },
        { name: "event_kind", value: "incident_update" },
        { name: "status", value: "stable" },
      ],
      compression: [
        {
          id: "77001",
          window: "1781500000000..1781509000000",
          summary: "Older INC-77 rollback context compressed; source event ids retained.",
        },
      ],
    };
  }
  if (id === "10029901") {
    return {
      resources: [
        {
          raw_uri: "incident_77.pdf",
          source_ref: "page=1:L0",
          text: "INC-77 rollback stable after stream proxy restart; monitor queue drain before close.",
        },
      ],
      embeddings: [{ ref: "ctx:emb:1001:10029901", model: "all-MiniLM-L6-v2", dim: 384, score: "0.94" }],
      indexes: [
        { name: "resource_type", value: "pdf" },
        { name: "raw_uri", value: "incident_77.pdf" },
      ],
    };
  }
  return {
    summaries: [{ level: "L0", text: node?.metadata?.summary || `${text(node?.label, "Context node")} routing and traversal metadata.` }],
    indexes: node?.metadata?.serving_filter ? [{ name: "serving_filter", value: node.metadata.serving_filter }] : [],
  };
}

function renderDetailCell(label, value) {
  return `
    <div>
      <dt>${escapeHtml(label)}</dt>
      <dd>${escapeHtml(text(value))}</dd>
    </div>
  `;
}

function renderRecordSection(title, rows, preferredKeys) {
  const safeRows = asArray(rows);
  return `
    <section class="node-record-section">
      <div>
        <h4>${escapeHtml(title)}</h4>
        <span>${escapeHtml(safeRows.length)} records</span>
      </div>
      ${
        safeRows.length
          ? safeRows
              .map((row) => {
                const keys = preferredKeys.filter((key) => Object.prototype.hasOwnProperty.call(row, key));
                const remaining = Object.keys(row).filter((key) => !keys.includes(key));
                return `
                  <dl class="node-record-grid">
                    ${[...keys, ...remaining].map((key) => renderDetailCell(key.replaceAll("_", " "), row[key])).join("")}
                  </dl>
                `;
              })
              .join("")
          : `<p class="empty-records">No records on this node.</p>`
      }
    </section>
  `;
}

function wireContextTreeControls(topology) {
  const tree = byId("context-tree");
  const selectedPath = new Set(asArray(topology?.selected_path).map(String));
  if (tree) {
    tree.__matrixarkTopology = topology;
    if (!tree.dataset) {
      tree.dataset = {};
    }
  }
  if (!tree || tree.dataset.wired === "true") {
    return;
  }
  const rerenderWorkspace = () => {
    const activeTopology = hydrateContextTopology(tree.__matrixarkTopology || topology || {});
    const activeSelectedPath = new Set(asArray(activeTopology.selected_path).map(String));
    const nodes = asArray(activeTopology.nodes);
    const selectedNode = getSelectedTopologyNode(nodes, activeSelectedPath);
    const shell = document.createElement("div");
    shell.innerHTML = renderTopologyWorkspace(activeTopology, activeSelectedPath, selectedNode);
    const nextActions = shell.querySelector(".topology-actions");
    const nextWorkspace = shell.querySelector(".topology-workspace");
    const currentActions = tree.querySelector(".topology-actions");
    const currentWorkspace = tree.querySelector(".topology-workspace");
    if (nextActions && currentActions) {
      currentActions.replaceWith(nextActions);
    }
    if (nextWorkspace && currentWorkspace) {
      currentWorkspace.replaceWith(nextWorkspace);
    }
  };
  tree.dataset.wired = "true";
  tree.addEventListener("click", (event) => {
    const action = event.target.closest("[data-tree-action]");
    if (action) {
      if (action.dataset.treeAction === "expand-all") {
        contextTreeUiState.collapsedNodeIds.clear();
      } else {
        const activeTopology = hydrateContextTopology(tree.__matrixarkTopology || topology || {});
        asArray(activeTopology.nodes)
          .filter((node) => Number(node.child_count) > 0 || asArray(activeTopology.nodes).some((candidate) => String(candidate.parent) === String(node.id)))
          .forEach((node) => contextTreeUiState.collapsedNodeIds.add(String(node.id)));
        const roots = asArray(activeTopology.nodes).filter((node) => !node.parent);
        roots.forEach((node) => contextTreeUiState.collapsedNodeIds.delete(String(node.id)));
      }
      contextTreeUiState.selectedEdgeId = "";
      rerenderWorkspace();
      return;
    }
    const toggle = event.target.closest("[data-toggle-id]");
    if (toggle) {
      event.preventDefault();
      event.stopPropagation();
      const nodeId = String(toggle.dataset.toggleId);
      if (contextTreeUiState.collapsedNodeIds.has(nodeId)) {
        contextTreeUiState.collapsedNodeIds.delete(nodeId);
      } else {
        contextTreeUiState.collapsedNodeIds.add(nodeId);
      }
      contextTreeUiState.selectedEdgeId = "";
      rerenderWorkspace();
      return;
    }
    const edgeTarget = event.target.closest("[data-edge-id]");
    if (edgeTarget) {
      event.preventDefault();
      event.stopPropagation();
      const activeTopology = hydrateContextTopology(tree.__matrixarkTopology || topology || {});
      contextTreeUiState.selectedEdgeId = String(edgeTarget.dataset.edgeId);
      const detail = byId("context-node-detail");
      if (detail) {
        detail.innerHTML = renderEdgeDetailById(contextTreeUiState.selectedEdgeId, activeTopology);
      }
      tree.querySelectorAll("[data-edge-id]").forEach((el) => {
        el.classList.toggle("selected", el.dataset.edgeId === contextTreeUiState.selectedEdgeId);
        el.classList.toggle("active", el.dataset.edgeId === contextTreeUiState.selectedEdgeId);
      });
      return;
    }
    const inspect = event.target.closest("[data-node-id]");
    if (!inspect || inspect.classList.contains("topology-branch")) return;
    event.preventDefault();
    event.stopPropagation();
    const nodeId = inspect.dataset.nodeId;
    const activeTopology = hydrateContextTopology(tree.__matrixarkTopology || topology || {});
    const node = asArray(activeTopology?.nodes).find((item) => String(item.id) === String(nodeId));
    const detail = byId("context-node-detail");
    if (!node || !detail) {
      return;
    }
    contextTreeUiState.selectedNodeId = String(node.id);
    contextTreeUiState.selectedEdgeId = "";
    detail.innerHTML = renderNodeDetail(node, activeTopology);
    tree.querySelectorAll(".graph-node, .topology-node").forEach((el) => {
      el.classList.toggle("selected", el.dataset.nodeId === String(node.id));
    });
    tree.querySelectorAll("[data-edge-id]").forEach((el) => {
      el.classList.remove("selected", "active");
    });
  });
}

function renderMetadataRows(metadata) {
  const entries = Object.entries(metadata || {});
  if (!entries.length) {
    return `<div><dt>metadata</dt><dd>-</dd></div>`;
  }
  return entries
    .map(
      ([key, value]) => `
        <div class="topology-meta-row">
          <dt>${escapeHtml(key.replaceAll("_", " "))}</dt>
          <dd>${escapeHtml(text(value))}</dd>
        </div>
      `,
    )
    .join("");
}

function renderFilesystemExplorer(filesystem, topology) {
  const el = byId("context-filesystem-explorer");
  if (!el) {
    return;
  }
  const nodes = asArray(topology?.nodes);
  const rows = asArray(filesystem).length
    ? filesystem
    : nodes.map((node) => ({
        path: `/${node.label || node.id}`,
        node_hash: node.id,
        model: node.metadata?.model || node.type || "ContextNode",
        storage: node.metadata?.object_key || "-",
        children: text(node.child_count, "0"),
        events: node.metadata?.event_key || "-",
        summary: node.metadata?.summary || node.metadata?.embedding_ref || "-",
        status: node.status || "ready",
      }));
  el.innerHTML = `
    <div class="filesystem-head">
      <div>
        <h3>Filesystem-Like Explorer</h3>
        <p>Context paths mapped to TemporalStore nodes, child refs, events, indexes, summaries, and chunks.</p>
      </div>
      ${badge(rows.length ? "ready" : "empty")}
    </div>
    <div class="filesystem-table-wrap">
      <table class="filesystem-table">
        <thead>
          <tr>
            <th>Path</th>
            <th>Node Hash</th>
            <th>Model</th>
            <th>Storage</th>
            <th>Children</th>
            <th>Events</th>
            <th>Summary</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  <td><code>${escapeHtml(row.path || "/")}</code></td>
                  <td>${escapeHtml(text(row.node_hash))}</td>
                  <td>${escapeHtml(row.model || "-")}</td>
                  <td>${escapeHtml(row.storage || "-")}</td>
                  <td>${escapeHtml(text(row.children, "0"))}</td>
                  <td>${escapeHtml(text(row.events, "-"))}</td>
                  <td>${escapeHtml(row.summary || "-")}</td>
                  <td>${badge(row.status || "ready")}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderContextObservation(observations) {
  const el = byId("context-observation");
  if (!el) {
    return;
  }
  const rows = asArray(observations);
  el.innerHTML = rows
    .map(
      (item) => `
        <article class="observation-card">
          <div>
            <span>${escapeHtml(item.label)}</span>
            ${badge(item.status || item.value || "watch")}
          </div>
          <strong>${escapeHtml(item.value || "-")}</strong>
          <p>${escapeHtml(item.detail || "")}</p>
        </article>
      `,
    )
    .join("");
}

function renderQueryWorkbench(workbench) {
  const el = byId("context-query-workbench");
  if (!el) {
    return;
  }
  const data = workbench || {};
  const result = data.result || {};
  el.innerHTML = `
    <article class="query-card query-card-wide">
      <span>Raw query</span>
      <strong>${escapeHtml(data.raw_query || "-")}</strong>
      <small>${escapeHtml(data.route || "scope -")} · query ${escapeHtml(data.query_id || "-")}</small>
    </article>
    <article class="query-card">
      <span>Intent</span>
      <div class="tag-list">
        ${asArray(data.intent)
          .map((item) => `<b>${escapeHtml(item.label)}=${escapeHtml(item.value)}</b>`)
          .join("")}
      </div>
    </article>
    <article class="query-card">
      <span>Controls</span>
      <div class="control-grid">
        ${asArray(data.controls)
          .map(
            (item) => `
              <label>
                ${escapeHtml(item.label)}
                <input type="text" value="${escapeHtml(item.value)}" readonly />
              </label>
            `,
          )
          .join("")}
      </div>
    </article>
    <article class="query-card">
      <span>Filters</span>
      <div class="tag-list">
        ${asArray(data.filters)
          .map((item) => `<b>${escapeHtml(item)}</b>`)
          .join("")}
      </div>
    </article>
    <article class="query-card">
      <span>Result pack</span>
      <dl class="result-dl">
        <div><dt>Events</dt><dd>${escapeHtml(asArray(result.events).join(", ") || "-")}</dd></div>
        <div><dt>Chunks</dt><dd>${escapeHtml(asArray(result.chunks).join(", ") || "-")}</dd></div>
        <div><dt>Tokens</dt><dd>${escapeHtml(result.tokens || "-")}</dd></div>
      </dl>
    </article>
  `;
}

function renderContextConfig(configGroups) {
  const el = byId("context-config");
  if (!el) {
    return;
  }
  const groups = asArray(configGroups);
  el.innerHTML = `
    ${groups
      .map(
      (group) => `
        <article class="config-card">
          <strong>${escapeHtml(group.group)}</strong>
          <div class="config-control-grid">
            ${asArray(group.items)
              .map((item) => renderRuntimeConfigControl(group.group, item))
              .join("")}
          </div>
        </article>
      `,
    )
    .join("")}
    <article class="config-card config-card-wide">
      <strong>Draft Runtime Config</strong>
      <code>${escapeHtml(JSON.stringify(buildRuntimeConfigDraft(groups), null, 2))}</code>
    </article>
  `;
}

function renderRuntimeConfigControl(group, item) {
  const id = `config-${group}-${item.label}`.replace(/[^a-z0-9_-]+/gi, "-").toLowerCase();
  return `
    <label class="config-control" for="${escapeHtml(id)}">
      <span>${escapeHtml(item.label)}</span>
      <input id="${escapeHtml(id)}" type="text" value="${escapeHtml(item.value)}" data-config-group="${escapeHtml(group)}" />
    </label>
  `;
}

function buildRuntimeConfigDraft(groups) {
  return Object.fromEntries(
    groups.map((group) => [
      group.group,
      Object.fromEntries(asArray(group.items).map((item) => [item.label, item.value])),
    ]),
  );
}

function renderModelRegistry(models) {
  const el = byId("context-model-registry");
  if (!el) {
    return;
  }
  const registry = asArray(models);
  el.innerHTML = `
    ${registry
      .map(
      (model) => `
        <article class="registry-card">
          <div class="registry-head">
            <strong>${escapeHtml(model.role)}</strong>
            <span>${escapeHtml(model.runtime)}</span>
          </div>
          <dl>
            <div><dt>Default</dt><dd>${escapeHtml(model.default_model)}</dd></div>
            <div><dt>Alternatives</dt><dd>${escapeHtml(model.alternatives)}</dd></div>
            <div><dt>I/O</dt><dd>${escapeHtml(model.io)}</dd></div>
            <div><dt>Use</dt><dd>${escapeHtml(model.use)}</dd></div>
          </dl>
          ${renderModelConfigControls(model)}
        </article>
      `,
    )
    .join("")}
    <article class="registry-card registry-card-wide">
      <div class="registry-head">
        <strong>Draft Model Config</strong>
        <span>local editable</span>
      </div>
      <code>${escapeHtml(JSON.stringify(buildModelConfigDraft(registry), null, 2))}</code>
    </article>
  `;
}

function renderModelConfigControls(model) {
  const role = text(model.role, "model").replace(/[^a-z0-9_-]+/gi, "-").toLowerCase();
  const options = splitModelOptions(model);
  return `
    <div class="model-config-controls">
      <label for="model-${escapeHtml(role)}">
        Model
        <select id="model-${escapeHtml(role)}" data-model-role="${escapeHtml(model.role)}">
          ${options.map((option) => `<option${option === model.default_model ? " selected" : ""}>${escapeHtml(option)}</option>`).join("")}
        </select>
      </label>
      <label for="runtime-${escapeHtml(role)}">
        Runtime
        <input id="runtime-${escapeHtml(role)}" type="text" value="${escapeHtml(model.runtime)}" />
      </label>
      <label for="provider-${escapeHtml(role)}">
        Provider
        <select id="provider-${escapeHtml(role)}">
          ${["local OSS", "OpenAI compatible", "agent supplied"].map((option) => `<option>${escapeHtml(option)}</option>`).join("")}
        </select>
      </label>
      <label class="model-toggle" for="enabled-${escapeHtml(role)}">
        <input id="enabled-${escapeHtml(role)}" type="checkbox" checked />
        Enabled
      </label>
    </div>
  `;
}

function splitModelOptions(model) {
  const values = [model.default_model, ...text(model.alternatives, "").split(",")].map((value) => value.trim()).filter(Boolean);
  return [...new Set(values)];
}

function buildModelConfigDraft(models) {
  return models.map((model) => ({
    role: model.role,
    model: model.default_model,
    alternatives: splitModelOptions(model).filter((item) => item !== model.default_model),
    runtime: model.runtime,
    provider: "local OSS",
    enabled: true,
  }));
}

function renderContextOperators(operators) {
  const el = byId("context-operators");
  if (!el) {
    return;
  }
  el.innerHTML = asArray(operators)
    .map(
      (item) => `
        <article class="operator-card">
          <div>
            <strong>${escapeHtml(item.name)}</strong>
            ${badge(item.status)}
          </div>
          <code>${escapeHtml(item.command)}</code>
          <p>${escapeHtml(item.detail)}</p>
        </article>
      `,
    )
    .join("");
}

function renderScaleTestsInto(id, tests) {
  const el = byId(id);
  if (!el) {
    return;
  }
  el.innerHTML = asArray(tests)
    .map(
      (test) => `
        <article class="test-card">
          <div class="test-title">
            <strong>${escapeHtml(test.name)}</strong>
            ${badge(test.status)}
          </div>
          <p>${escapeHtml(test.workload)}</p>
          <div class="mini-grid">
            <span>Write<strong>${escapeHtml(test.write_qps)}</strong></span>
            <span>Extract<strong>${escapeHtml(test.read_p50_ms)}</strong></span>
            <span>Query<strong>${escapeHtml(test.read_p99_ms)}</strong></span>
            <span>Replay<strong>${escapeHtml(test.secondary_lag_ms)}</strong></span>
          </div>
          <div class="test-meta">
            <span>${escapeHtml(test.threads || "scope -")}</span>
            <span>${escapeHtml(test.result_dir || "case -")}</span>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderPackGroup(title, values) {
  return `
    <section>
      <strong>${escapeHtml(title)}</strong>
      ${asArray(values)
        .map((value) => `<span>${escapeHtml(value)}</span>`)
        .join("")}
    </section>
  `;
}

let lastGoodHealth = null;
let refreshInFlight = false;

function withHealthSource(data, source) {
  return {
    ...data,
    __source: {
      kind: source.kind,
      label: source.label,
      detail: source.detail,
    },
  };
}

function validateHealthPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("invalid health payload: expected object");
  }
  return payload;
}

function saveCachedHealth(data) {
  try {
    if (!globalThis.localStorage) {
      return;
    }
    globalThis.localStorage.setItem(healthCacheKey, JSON.stringify({
      saved_at_ms: Date.now(),
      data,
    }));
  } catch (err) {
    return;
  }
}

function loadCachedHealth() {
  try {
    if (!globalThis.localStorage) {
      return null;
    }
    const raw = globalThis.localStorage.getItem(healthCacheKey);
    if (!raw) {
      return null;
    }
    const cached = JSON.parse(raw);
    const savedAtMs = Number(cached?.saved_at_ms) || 0;
    if (!savedAtMs || Date.now() - savedAtMs > healthCacheMaxAgeMs) {
      globalThis.localStorage.removeItem?.(healthCacheKey);
      return null;
    }
    const data = normalizeHealth(validateHealthPayload(cached?.data));
    const savedAt = new Date(savedAtMs).toLocaleString();
    return withHealthSource(data, {
      kind: "cached",
      label: "Cached health data",
      detail: `last successful payload cached at ${savedAt}; expires after ${Math.round(healthCacheMaxAgeMs / 60000)} minutes`,
    });
  } catch (err) {
    return null;
  }
}

function normalizeHealth(payload) {
  if (payload.health || payload.cluster || payload.nodes) {
    const hasFullHealth = Boolean(payload.health && payload.cluster && payload.context_ops);
    const label = hasFullHealth ? "Live health data" : "Partial health data";
    const detail = hasFullHealth
      ? "health.json loaded with cluster, runtime, and context operations"
      : "health.json loaded but is missing one or more production sections";
    return withHealthSource({
      ...fallbackHealth,
      ...payload,
      health: { ...fallbackHealth.health, ...(payload.health || {}) },
      runtime_config: { ...fallbackHealth.runtime_config, ...(payload.runtime_config || {}) },
      replication: { ...fallbackHealth.replication, ...(payload.replication || {}) },
    }, { kind: hasFullHealth ? "live" : "partial", label, detail });
  }
  return withHealthSource({
    ...fallbackHealth,
    health: {
      ...fallbackHealth.health,
      metaserver: payload.metaserver || fallbackHealth.health.metaserver,
      proxy: payload.proxy || fallbackHealth.health.proxy,
      exporter: payload.exporter || fallbackHealth.health.exporter,
    },
  }, {
    kind: "partial",
    label: "Compatibility health data",
    detail: "legacy health payload loaded without full context operations",
  });
}

function renderHealthSource(data) {
  const el = byId("health-source-banner");
  if (!el) {
    return;
  }
  const source = data.__source || {
    kind: "fallback",
    label: "Fallback sample data",
    detail: "health.json unavailable; rendering bundled sample data",
  };
  const isLive = source.kind === "live";
  el.className = `health-source-banner ${isLive ? "ok" : "warn"}`;
  el.innerHTML = `
    <strong>${escapeHtml(source.label || "Health data source")}</strong>
    <span>${escapeHtml(source.detail || "")}</span>
  `;
}

function markHealthStale(data, err) {
  const previous = data.__source || {};
  const reason = err?.message ? `Last refresh failed: ${err.message}` : "Last refresh failed";
  return withHealthSource(data, {
    kind: "stale",
    label: "Stale live health data",
    detail: `${reason}; keeping last successful payload from ${previous.label || "health.json"}`,
  });
}

function setRefreshBusy(isBusy) {
  const button = byId("refresh");
  if (!button) {
    return;
  }
  button.disabled = isBusy;
  button.setAttribute("aria-busy", String(isBusy));
  button.textContent = isBusy ? "Refreshing" : "Refresh";
}

function fetchHealthJson() {
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  let timeoutId = null;
  const timeout = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      if (controller) {
        controller.abort();
      }
      reject(new Error(`refresh timeout after ${refreshTimeoutMs} ms`));
    }, refreshTimeoutMs);
  });
  const request = fetch(`/health.json?ts=${Date.now()}`, controller ? { signal: controller.signal } : undefined);
  return Promise.race([request, timeout]).finally(() => {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
  });
}

function render(data) {
  const cluster = data.cluster || {};
  const health = data.health || {};
  const config = data.runtime_config || {};
  const replication = data.replication || {};
  const diagnostics = data.diagnostics || {};
  const healthyCount = healthIds.filter((id) => statusClass(health[id]?.status) === "ok").length;
  const scaleMatrix = asArray(data.scale_matrix);
  const bestScale = scaleMatrix.find((row) => row.workload?.includes("primary") && row.threads === "16") ||
    scaleMatrix.find((row) => row.workload?.includes("primary")) ||
    data.scale_tests?.[0];

  renderHealthSource(data);

  byId("cluster-status").className = `status-pill ${statusClass(cluster.status)}`;
  byId("cluster-status").innerHTML = `<span class="dot"></span>${escapeHtml(cluster.environment || cluster.name || "cluster")}`;

  setText("summary-topology", `${text(cluster.metaservers, 1)} meta / ${text(cluster.data_nodes, 2)} data`);
  setText("summary-topology-note", text(cluster.name, "cluster"));
  setText("summary-write-qps", bestScale?.write_qps || data.scale_tests?.[0]?.write_qps || "-");
  setText("summary-write-note", bestScale?.threads ? `${bestScale.threads} threads / ${bestScale.workload}` : "latest scale run");
  setText("summary-lag", text(replication.secondary_lag_ms));
  setText("summary-lag-note", text(replication.mode, "replication"));
  setText("summary-cache", text(config.blockcache_ssd_capacity || "configured"));
  setText("summary-cache-note", `DRAM ${text(config.blockcache_dram_capacity)}`);

  renderNodes(data.nodes);
  renderProducts(data.products);
  renderProductServices(data.product_services);
  renderContextOps(data.context_ops);
  renderScaleTests(data.scale_tests);
  renderModules(data.module_tests);
  renderDataModels(data.data_models);
  renderTableRows("scale-matrix-body", scaleMatrix, [
    "workload",
    "threads",
    "features",
    "writes",
    "write_qps",
    "write_p99",
    "read_qps",
    "read_p99",
    "errors",
  ]);
  renderTableRows("replication-matrix-body", replication.lag_matrix, [
    "threads",
    "visible",
    "missing",
    "p99_lag",
    "max_lag",
  ]);

  const modeRows = asArray(config.modes).map((mode) => ({
    label: mode.name,
    html: `<span class="config-mode">${escapeHtml(mode.oplog_batch)} · ${escapeHtml(mode.replay_loop)}<small>${escapeHtml(mode.use)}</small></span>`,
  }));
  renderMetricList("config-profile", [
    { label: "Active profile", value: config.profile },
    ...modeRows,
  ]);

  renderMetricList(
    "health-list",
    healthIds.map((id) => ({
      label: id.replaceAll("_", " "),
      html: `${badge(health[id]?.status)} <small>${escapeHtml(health[id]?.detail || "")}</small>`,
    })),
  );

  renderMetricList(
    "config-list",
    Object.entries(config).filter(([, value]) => !Array.isArray(value)).map(([key, value]) => ({
      label: key.replaceAll("_", " "),
      value,
    })),
  );

  renderMetricList("replication-list", [
    { label: "Mode", value: replication.mode },
    { label: "Replay source", value: replication.replay_source },
    { label: "Secondary lag", value: replication.secondary_lag_ms },
    { label: "Visibility", value: replication.visibility },
  ]);

  const bytekv = data.bytekv_scale || {};
  renderMetricList("bytekv-scale-list", [
    { label: "Read QPS", value: bytekv.read_qps },
    { label: "Read p95", value: bytekv.read_p95 },
    { label: "Mixed QPS", value: bytekv.mixed_qps },
    { label: "Write QPS", value: bytekv.write_qps },
    { label: "Write p95", value: bytekv.write_p95 },
    { label: "Open issue", value: bytekv.open_issue },
  ]);

  const abase = data.abase_api || {};
  renderMetricList("abase-api-list", [
    { label: "Master", value: abase.master },
    { label: "Proxy", value: abase.proxy },
    { label: "Datanodes", value: abase.datanodes },
    { label: "RESP endpoint", value: abase.resp_endpoint },
    { label: "Redis PING", value: abase.redis_ping },
    { label: "Next step", value: abase.next_step },
  ]);

  renderMetricList("diagnostics-list", [
    { label: "Healthy checks", value: `${healthyCount}/${healthIds.length}` },
    { label: "Last result", value: diagnostics.last_result_dir },
    { label: "Release build", value: diagnostics.release_build },
    { label: "Direct SDK", value: diagnostics.direct_sdk },
    { label: "Proxy SDK", value: diagnostics.proxy_sdk },
  ]);

  setText("last-refresh", `refreshed ${new Date().toLocaleString()}`);
}

async function refreshHealth() {
  if (refreshInFlight) {
    return;
  }
  refreshInFlight = true;
  setRefreshBusy(true);
  setText("last-refresh", `refreshing ${new Date().toLocaleString()}`);
  try {
    const res = await fetchHealthJson();
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    lastGoodHealth = normalizeHealth(validateHealthPayload(await res.json()));
    saveCachedHealth(lastGoodHealth);
    render(lastGoodHealth);
  } catch (err) {
    if (lastGoodHealth) {
      render(markHealthStale(lastGoodHealth, err));
      setText("last-refresh", `stale ${new Date().toLocaleString()}`);
    } else {
      const failureDetail = err?.message
        ? `health.json unavailable (${err.message}); rendering bundled sample data`
        : "health.json unavailable; rendering bundled sample data";
      render(withHealthSource(fallbackHealth, {
        kind: "fallback",
        label: "Fallback sample data",
        detail: failureDetail,
      }));
      setText("last-refresh", `offline sample ${new Date().toLocaleString()}`);
    }
  } finally {
    refreshInFlight = false;
    setRefreshBusy(false);
  }
}

function autoRefreshHealth() {
  if (globalThis.document?.hidden) {
    setText("last-refresh", `paused while hidden ${new Date().toLocaleString()}`);
    return;
  }
  refreshHealth();
}

function handleVisibilityChange() {
  if (!globalThis.document?.hidden) {
    return refreshHealth();
  }
  return undefined;
}

byId("refresh").addEventListener("click", refreshHealth);
if (globalThis.document?.addEventListener) {
  globalThis.document.addEventListener("visibilitychange", handleVisibilityChange);
}
lastGoodHealth = loadCachedHealth();
if (lastGoodHealth) {
  render(markHealthStale(lastGoodHealth, new Error("waiting for live refresh")));
  setText("last-refresh", `cached ${new Date().toLocaleString()}`);
}
refreshHealth();
setInterval(autoRefreshHealth, refreshIntervalMs);
