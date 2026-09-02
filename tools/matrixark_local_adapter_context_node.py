# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_LocalAdapterContextNodeMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    Any,
    compact_context_embedding_record,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    Any,
    compact_context_embedding_record,
)


class _LocalAdapterContextNodeMixin:
    def default_session_node_path(self, scope: Json) -> list[str]:
        tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
        user_id = str(scope.get("user_id") or local_account_user_id())
        session_id = str(scope.get("session_id") or user_id or "default_session")
        return [f"tenant:{tenant_id}", f"user:{user_id}", f"session:{session_id}"]

    def default_shared_context_node_path(self, scope: Json, *, kind: str, sharing_scope: str) -> list[str]:
        collection = "skills" if kind == "skill" else "resources"
        if sharing_scope == "global_shared":
            return ["global", "shared", collection]
        tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
        return [f"tenant:{tenant_id}", "shared", collection]

    def resource_sharing_scope(self, args: Json, envelope: Json, deployment_scope: str) -> str:
        metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
        explicit = str(args.get("sharing_scope") or metadata.get("sharing_scope") or "").strip().lower()
        if explicit in {"tenant_shared", "global_shared", "private_user"}:
            return explicit
        if deployment_scope == "global":
            return "global_shared"
        scope = envelope.get("scope", {}) if isinstance(envelope.get("scope"), dict) else {}
        if not scope.get("user_id") and not scope.get("session_id"):
            return "tenant_shared" if scope.get("tenant_id") else "global_shared"
        return "private_user"

    def default_resource_node_path(self, args: Json, envelope: Json, *, deployment_scope: str, sharing_scope: str) -> list[str]:
        metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
        if metadata.get("node_path"):
            return [str(part) for part in metadata.get("node_path", []) if str(part)]
        if sharing_scope in {"tenant_shared", "global_shared"}:
            return self.default_shared_context_node_path(envelope.get("scope", {}), kind=str(envelope.get("kind") or "resource"), sharing_scope=sharing_scope)
        return self.default_session_node_path(envelope.get("scope", {}))

    def _existing_node_embedding_refs(self, current_model_ref: str) -> set[int]:
        """Node hashes that already have a usable embedding for this model.

        Answered from an index the adapter keeps, not by walking the store. This runs on every
        ingest -- `ensure_context_node_path` is called several times per one -- and was one of the
        remaining reads of the whole record set per write.

        The index is built from one read the first time it is asked, folded forward on every
        append, and dropped whenever the read cache is, so it cannot outlive the view it came from.
        """
        index = getattr(self, "_node_embedding_refs_index", None)
        if index is None:
            index = {}
            for record in self.read_all():
                self._note_node_embedding_ref(index, record)
            self._node_embedding_refs_index = index
        return set(index.get(current_model_ref, ()))

    @staticmethod
    def _node_embedding_ref_of(record):
        """(model_ref, node_hash) for a row that carries a node embedding, else None.

        A node embedding now lives ON the node: fold_embedding_records moves the vector onto the
        owner and drops the separate context_embedding row, which is where every vector in a new
        log is. Looking only for that row found nothing in any log written since, so every node was
        reported un-embedded and re-embedded on every ingest -- 60 embeddings for 3 distinct nodes
        over 20 ingests. Logs written before the fold still carry the separate row.
        """
        if not isinstance(record, dict):
            return None
        record_type = str(record.get("record_type") or "")
        if record_type == "context_node":
            meta = record.get("embedding_meta")
            if not isinstance(meta, dict):
                return None
            model_ref = str(meta.get("model_ref") or "")
            if not model_ref:
                return None
            if not record.get("vector") and not record_vector(record):
                return None
            try:
                return (model_ref, int(record.get("node_hash")))
            except (TypeError, ValueError):
                return None
        if (
            record_type != "context_embedding"
            or record.get("ref_type") != "node"
            or record.get("ref_hash") is None
            or record.get("embedding_type") != "context_node"
            or not record_vector(record)
            or not record.get("vector")
        ):
            return None
        model_ref = str(record.get("model_ref") or "")
        if not model_ref:
            return None
        try:
            return (model_ref, int(record.get("ref_hash")))
        except (TypeError, ValueError):
            return None

    @classmethod
    def _note_node_embedding_ref(cls, index, record) -> None:
        pair = cls._node_embedding_ref_of(record)
        if pair is None:
            return
        model_ref, node_hash = pair
        index.setdefault(model_ref, set()).add(node_hash)

    def _record_node_embedding_ref(self, node_hash: int, current_model_ref: str) -> None:
        """Note that a node embedding now exists. No-op here; the log IS the record.

        The native adapter overrides this to keep its keyed index current.
        """
        return None

    def ensure_context_node_path(self, *, node_path: list[str], scope: Json, updated_at_ms: int) -> Json:
        prefixes = node_prefixes(node_path)
        if not prefixes:
            return {"nodes_created": 0, "child_refs_created": 0, "node_hashes": []}

        compact_scope = serving_scope_ref(scope)
        self._ensure_context_node_cache_loaded()
        existing_nodes = self._context_node_hashes
        existing_child_refs = self._context_child_ref_hashes
        current_model = embedding_model_name()
        current_model_ref = embedding_model_ref_for_name(current_model)
        existing_node_embeddings = self._existing_node_embedding_refs(current_model_ref)
        node_hashes: list[int] = []
        nodes_created = 0
        child_refs_created = 0
        node_embeddings_created = 0
        for prefix in prefixes:
            node_hash = stable_hash("/".join(prefix))
            node_hashes.append(node_hash)
            parent_path = prefix[:-1]
            parent_hash = stable_hash("/".join(parent_path)) if parent_path else 0
            if node_hash not in existing_nodes:
                self.append(
                    {
                        "record_type": "context_node",
                        "node_hash": node_hash,
                        "parent_hash": parent_hash,
                        "node_name": prefix[-1],
                        "node_path": prefix,
                        "depth": len(prefix),
                        "scope": scope,
                        "created_at_ms": updated_at_ms,
                        "updated_at_ms": updated_at_ms,
                    }
                )
                existing_nodes.add(node_hash)
                nodes_created += 1
            if node_hash not in existing_node_embeddings:
                embedding_text = " ".join([*prefix, f"depth:{len(prefix)}"])
                node_vector = embedding_for_text(embedding_text)
                self.append(
                    compact_context_embedding_record({
                        "record_type": "context_embedding",
                        "embedding_type": "context_node",
                        "ref_type": "node",
                        "ref_hash": node_hash,
                        "node_hash": node_hash,
                        "node_path": prefix,
                        "dim": len(node_vector),
                        "model": current_model,
                        "model_ref": current_model_ref,
                        "vector": node_vector,
                        "scope": scope,
                        "source_record_type": "context_node",
                        "source_updated_at_ms": updated_at_ms,
                        "updated_at_ms": updated_at_ms,
                    })
                )
                existing_node_embeddings.add(node_hash)
                self._record_node_embedding_ref(node_hash, current_model_ref)
                node_embeddings_created += 1
            if parent_path:
                child_ref_hash = stable_hash(f"child:{parent_hash}:{node_hash}")
                if child_ref_hash not in existing_child_refs:
                    self.append(
                        {
                            "record_type": "context_child_ref",
                            "child_ref_hash": child_ref_hash,
                            "parent_hash": parent_hash,
                            "child_hash": node_hash,
                            "child_name": prefix[-1],
                            "parent_path": parent_path,
                            "child_path": prefix,
                            "depth": len(prefix),
                            "scope": scope,
                            "created_at_ms": updated_at_ms,
                            "updated_at_ms": updated_at_ms,
                        }
                    )
                    existing_child_refs.add(child_ref_hash)
                    child_refs_created += 1
        return {
            "nodes_created": nodes_created,
            "child_refs_created": child_refs_created,
            "node_embeddings_created": node_embeddings_created,
            "node_hashes": node_hashes,
        }

    def _embedding_target_for_context_record(self, record: Json) -> Json | None:
        record_type = str(record.get("record_type") or "")
        node_path = record.get("node_path") if isinstance(record.get("node_path"), list) else []
        ref_type = ""
        ref_hash: Any = None
        embedding_type = ""
        text = ""
        if record_type == "context_event":
            ref_type = "event"
            ref_hash = record.get("event_id_hash")
            embedding_type = "event_text"
            text = str(record.get("summary_text") or record.get("text") or "")
        elif record_type == "context_segment":
            ref_type = "segment"
            ref_hash = record.get("segment_hash")
            embedding_type = "segment_text"
            text = " ".join(str(item or "") for item in [record.get("topic"), record.get("summary_text"), record.get("text")]).strip()
        elif record_type == "context_entity":
            ref_type = "entity"
            ref_hash = record.get("entity_hash")
            embedding_type = "profile_entity_state" if str(record.get("memory_scope") or "") == "user_profile" else "entity_state"
            text = " ".join(
                str(item or "")
                for item in [record.get("entity_type"), record.get("entity_name"), record.get("state"), record.get("value")]
            ).strip()
        elif record_type == "context_node":
            ref_type = "node"
            ref_hash = record.get("node_hash")
            embedding_type = "context_node"
            text = " ".join([*(str(item) for item in node_path), str(record.get("node_name") or ""), f"depth:{record.get('depth') or len(node_path)}"]).strip()
        elif record_type == "context_summary":
            ref_type = "summary"
            ref_hash = record.get("summary_hash") or record.get("node_hash")
            embedding_type = str(record.get("summary_type") or "summary_text")
            text = " ".join([*(str(item) for item in node_path), str(record.get("summary_text") or "")]).strip()
        elif record_type == "context_compression_event":
            ref_type = "compression"
            ref_hash = record.get("compression_id_hash")
            embedding_type = "compression_summary"
            text = " ".join([*(str(item) for item in node_path), str(record.get("summary_text") or "")]).strip()
        if not ref_type or ref_hash is None or not text:
            return None
        try:
            ref_hash_int = int(ref_hash)
        except (TypeError, ValueError):
            return None
        return {
            "record_type": record_type,
            "embedding_type": embedding_type,
            "ref_type": ref_type,
            "ref_hash": ref_hash_int,
            "node_hash": record.get("node_hash"),
            "node_path": node_path,
            "text": text,
            "scope": candidate_access_scope(record),
            "memory_scope": record.get("memory_scope", ""),
            "session_continuity": record.get("session_continuity", ""),
            "source_updated_at_ms": record.get("updated_at_ms"),
        }

    def ensure_context_embeddings(
        self,
        *,
        scope: Json | None = None,
        limit: int = 512,
        updated_at_ms: int | None = None,
        record_types: set[str] | None = None,
        records: list[Json] | None = None,
    ) -> Json:
        limit = max(1, int(limit or 1))
        refreshed_at_ms = updated_at_ms if isinstance(updated_at_ms, int) else now_ms()
        current_model = embedding_model_name()
        current_model_ref = embedding_model_ref_for_name(current_model)
        existing_embeddings: dict[tuple[str, str, int], Json] = {}
        # A caller that has already read the log this pass can hand its snapshot in, but only when
        # nothing has been written since -- this needs to see rows produced earlier in the pass.
        records = self.read_all() if records is None else records
        for record in records:
            if record.get("record_type") != "context_embedding":
                continue
            try:
                key = (str(record.get("embedding_type") or ""), str(record.get("ref_type") or ""), int(record.get("ref_hash")))
            except (TypeError, ValueError):
                continue
            current = existing_embeddings.get(key)
            if current is None or int(record.get("updated_at_ms") or 0) >= int(current.get("updated_at_ms") or 0):
                existing_embeddings[key] = record

        targets: list[Json] = []
        skipped_current = 0
        skipped_scope = 0
        skipped_type = 0
        for record in records:
            record_type = str(record.get("record_type") or "")
            if record_type == "context_embedding":
                continue
            if record_types is not None and record_type not in record_types:
                skipped_type += 1
                continue
            target = self._embedding_target_for_context_record(record)
            if target is None:
                continue
            if scope and not scope_matches(target["scope"], scope):
                skipped_scope += 1
                continue
            key = (target["embedding_type"], target["ref_type"], target["ref_hash"])
            existing = existing_embeddings.get(key)
            source_updated_at_ms = int(target.get("source_updated_at_ms") or 0)
            existing_updated_at_ms = int(existing.get("updated_at_ms") or 0) if existing else 0
            existing_model_ref = str((existing or {}).get("model_ref") or "")
            if (
                existing
                and record_vector(existing)
                and existing_model_ref == current_model_ref
                and existing_updated_at_ms >= source_updated_at_ms
            ):
                skipped_current += 1
                continue
            targets.append(target)
            if len(targets) >= limit:
                break

        vectors = embeddings_for_texts([str(target["text"]) for target in targets])
        generated_records: list[Json] = []
        generated_by_type: Json = {}
        for target, vector in zip(targets, vectors):
            if not vector:
                continue
            generated_by_type[target["record_type"]] = int(generated_by_type.get(target["record_type"], 0)) + 1
            generated_records.append(
                compact_context_embedding_record(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": target["embedding_type"],
                        "ref_type": target["ref_type"],
                        "ref_hash": target["ref_hash"],
                        "node_hash": target.get("node_hash"),
                        "node_path": target.get("node_path", []),
                        "dim": len(vector),
                        "model": current_model,
                        "vector": vector,
                        "scope": target["scope"],
                        "memory_scope": target.get("memory_scope", ""),
                        "session_continuity": target.get("session_continuity", ""),
                        "source_record_type": target["record_type"],
                        "source_updated_at_ms": target.get("source_updated_at_ms"),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
            )
        if generated_records:
            self.append_many(generated_records)
        return {
            "status": "ok",
            "model": current_model,
            "model_ref": current_model_ref,
            "scanned_count": len(records),
            "target_count": len(targets),
            "generated_count": len(generated_records),
            "generated_by_record_type": generated_by_type,
            "skipped_current_count": skipped_current,
            "skipped_scope_count": skipped_scope,
            "skipped_type_count": skipped_type,
            "limit": limit,
        }

