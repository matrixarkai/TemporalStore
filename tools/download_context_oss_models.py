#!/usr/bin/env python3
"""Download OSS models for the MatrixArk context pipeline.

The default route uses ModelScope because direct Hugging Face access can be
blocked on some local networks. The script prints a JSON manifest with local
paths that can be passed to run_context_pipeline_scale_e2e.py.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_VLM_MODEL = "Salesforce/blip-image-captioning-base"


def download_modelscope(model_id: str, cache_dir: Path, revision: str) -> str:
    try:
        from modelscope import snapshot_download  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on local env.
        raise SystemExit(
            "modelscope is required for --source modelscope. Install it with:\n"
            "  python3 -m pip install --user modelscope"
        ) from exc
    return snapshot_download(model_id, cache_dir=str(cache_dir), revision=revision)


def download_huggingface(model_id: str, cache_dir: Path, revision: str) -> str:
    try:
        from huggingface_hub import snapshot_download  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on local env.
        raise SystemExit(
            "huggingface_hub is required for --source huggingface. Install it with:\n"
            "  python3 -m pip install --user huggingface_hub"
        ) from exc
    return snapshot_download(repo_id=model_id, cache_dir=str(cache_dir), revision=revision)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["modelscope", "huggingface"], default="modelscope")
    parser.add_argument("--cache-dir", type=Path, default=Path(".local/context-oss-models"))
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--vlm-model", default=DEFAULT_VLM_MODEL)
    parser.add_argument("--revision", default="master")
    parser.add_argument(
        "--skip-vlm",
        action="store_true",
        help="download only the embedding model; current E2E only requires VLM packages.",
    )
    parser.add_argument("--manifest", type=Path, default=Path(".local/context-oss-models/manifest.json"))
    args = parser.parse_args()

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    if args.source == "modelscope":
        embedding_path = download_modelscope(args.embedding_model, args.cache_dir, args.revision)
        vlm_path = ""
        if not args.skip_vlm:
            try:
                vlm_path = download_modelscope(args.vlm_model, args.cache_dir, args.revision)
            except Exception as exc:
                vlm_path = ""
                print(f"warning: VLM model download skipped after failure: {exc}")
    else:
        embedding_path = download_huggingface(args.embedding_model, args.cache_dir, args.revision)
        vlm_path = ""
        if not args.skip_vlm:
            try:
                vlm_path = download_huggingface(args.vlm_model, args.cache_dir, args.revision)
            except Exception as exc:
                vlm_path = ""
                print(f"warning: VLM model download skipped after failure: {exc}")

    manifest = {
        "source": args.source,
        "embedding_model": args.embedding_model,
        "embedding_model_path": embedding_path,
        "vlm_model": args.vlm_model,
        "vlm_model_path": vlm_path,
    }
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
