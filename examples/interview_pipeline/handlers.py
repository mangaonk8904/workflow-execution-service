"""Step handlers for the expert-interview pipeline.

The media and model work is stubbed so the example runs anywhere with only
Postgres: each handler notes what a production version would call instead.
What is real is the shape of the data flow: one opaque recording becomes
aligned segments, then text, then structured records that cite their evidence.

Run input:
    {
      "video_uri": "s3://.../interview.mp4",
      "expert": {"id": "exp_123", "name": "Jane Doe"},
      "consent": {"share_with_customers": true},
      "turns": [{"question": ..., "answer": ..., "start": 0.0, "end": 41.5, "asr_confidence": 0.97}]
    }
`turns` stands in for what diarisation and speech recognition would produce.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

from wes import Context, HandlerRegistry, PermanentError

registry = HandlerRegistry()

PIPELINE = "expert-interview-handlers/0.1.0"
MODELS = {"asr": "stub-asr-1", "extractor": "stub-extractor-1", "pii": "regex-pii-1"}

EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE = re.compile(r"(?<!\w)(?:\+?\d{1,2}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}(?!\w)")
SENTENCE = re.compile(r"(?<=[.!?])\s+")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _turns(ctx: Context) -> list[dict]:
    turns = ctx.run_input.get("turns")
    if not turns:
        raise PermanentError("run input has no 'turns' (stub for diarisation + ASR output)")
    return turns


@registry.register("probe")
def probe(ctx: Context) -> dict:
    """Production: ffprobe the object, verify checksum, reject corrupt or unsupported containers."""
    uri = ctx.run_input.get("video_uri")
    if not uri:
        raise PermanentError("video_uri is required")
    if not ctx.run_input.get("consent", {}).get("share_with_customers"):
        raise PermanentError("expert has not consented to sharing with customers")
    turns = _turns(ctx)
    return {
        "video_uri": uri,
        "container": uri.rsplit(".", 1)[-1].lower(),
        "duration_seconds": max(t["end"] for t in turns),
        "source_sha256": _sha(uri),
    }


@registry.register("normalize")
def normalize(ctx: Context) -> dict:
    """Production: transcode to a mezzanine format, split the audio track, render a review proxy.

    Output paths are derived from the content hash, so a retry overwrites the
    same objects instead of creating duplicates.
    """
    sha = ctx.deps["probe"]["source_sha256"]
    base = f"store://derived/{sha[:16]}"
    return {
        "mezzanine_uri": f"{base}/mezzanine.mp4",
        "audio_uri": f"{base}/audio.flac",
        "proxy_uri": f"{base}/proxy_480p.mp4",
    }


@registry.register("segment")
def segment(ctx: Context) -> dict:
    """Production: voice-activity detection + diarisation, then cut on question turns."""
    return {
        "segments": [
            {"index": i, "start": t["start"], "end": t["end"], "question": t["question"]}
            for i, t in enumerate(_turns(ctx))
        ]
    }


@registry.register("transcribe")
def transcribe(ctx: Context) -> dict:
    """Production: GPU speech recognition on one audio segment, with word timestamps."""
    seg = ctx.item
    turn = _turns(ctx)[seg["index"]]
    return {
        **seg,
        "text": turn["answer"],
        "asr_confidence": turn.get("asr_confidence", 0.95),
        "model": MODELS["asr"],
    }


@registry.register("redact")
def redact(ctx: Context) -> dict:
    """Production: NER-based PII detection; also mute/blur the matching media spans."""
    expert_name = (ctx.run_input.get("expert") or {}).get("name")
    turns, total = [], 0
    for t in ctx.deps["transcribe"]["items"]:
        text, n_email = EMAIL.subn("[EMAIL]", t["text"])
        text, n_phone = PHONE.subn("[PHONE]", text)
        n_name = 0
        if expert_name:
            text, n_name = re.subn(re.escape(expert_name), "[EXPERT]", text, flags=re.IGNORECASE)
        total += n_email + n_phone + n_name
        turns.append({**t, "text": text, "redactions": n_email + n_phone + n_name})
    return {"turns": turns, "redactions": total, "model": MODELS["pii"]}


@registry.register("extract")
def extract(ctx: Context) -> dict:
    """Production: an LLM fills the customer's schema for one question/answer pair.

    Every record carries `evidence`: the segment and timestamps it came from.
    """
    turn = ctx.item
    sentences = [s.strip() for s in SENTENCE.split(turn["text"]) if s.strip()]
    lowered = f"{turn['question']} {turn['text']}".lower()
    vocabulary = ctx.params.get("skills_vocabulary", [])
    return {
        "question": turn["question"],
        "summary": sentences[0] if sentences else "",
        "skills": [s for s in vocabulary if s in lowered],
        "claims": [s for s in sentences if re.search(r"\d|because|therefore", s, re.IGNORECASE)],
        "evidence": {"segment_index": turn["index"], "start": turn["start"], "end": turn["end"]},
        "asr_confidence": turn["asr_confidence"],
        "model": MODELS["extractor"],
    }


@registry.register("quality")
def quality(ctx: Context) -> dict:
    """Automated checks, then decide whether a human needs to look.

    Review is triggered by low transcription confidence, an empty extraction,
    or a deterministic random sample (so retries make the same decision).
    """
    records = ctx.deps["extract"]["items"]
    threshold = ctx.params.get("min_asr_confidence", 0.85)
    low = [r["evidence"]["segment_index"] for r in records if r["asr_confidence"] < threshold]
    empty = [r["evidence"]["segment_index"] for r in records if not r["summary"]]
    bucket = int(_sha(ctx.run_id)[:8], 16) / 0xFFFFFFFF
    sampled = bucket < ctx.params.get("review_sample_rate", 0.0)
    reasons = (
        (["low_asr_confidence"] if low else [])
        + (["empty_extraction"] if empty else [])
        + (["random_sample"] if sampled else [])
    )
    confidences = [r["asr_confidence"] for r in records]
    return {
        "needs_review": bool(reasons),
        "reasons": reasons,
        "low_confidence_segments": low,
        "empty_segments": empty,
        "mean_asr_confidence": round(sum(confidences) / len(confidences), 4) if records else None,
        "records": len(records),
    }


@registry.register("package")
def package(ctx: Context) -> dict:
    """Assemble the delivery manifest, with lineage, and write it to the delivery store."""
    review = ctx.deps.get("expert_review")  # None when the review step was skipped
    if review and review.get("verdict") == "reject":
        raise PermanentError(f"rejected in expert review: {review.get('notes', 'no notes')}")
    expert_id = (ctx.run_input.get("expert") or {}).get("id", "unknown")
    manifest = {
        "manifest_version": 1,
        "run_id": ctx.run_id,
        "expert_ref": "exp_" + _sha(f"{ctx.tenant_id}:{expert_id}")[:12],  # pseudonymous, stable per tenant
        "records": ctx.deps["extract"]["items"],
        "quality": ctx.deps["quality"],
        "review": review or {"verdict": "auto_accepted"},
        "lineage": {"handlers": PIPELINE, "models": MODELS},
    }
    out_dir = Path(os.environ.get("WES_DELIVERY_DIR", "deliveries")) / ctx.tenant_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{ctx.run_id}.json"
    path.write_text(json.dumps(manifest, indent=2))  # same path on retry: idempotent
    return {"manifest_uri": str(path), "records": len(manifest["records"])}
