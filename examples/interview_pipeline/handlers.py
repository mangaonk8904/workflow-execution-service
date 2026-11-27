"""Step handlers for the expert-interview pipeline.

The media and model work is stubbed so the example runs anywhere with only
Postgres and a local directory: each handler notes what a production version
would call. What is real is the data flow through the asset catalog. Every
intermediate is a content-addressed asset with lineage back to the uploaded
recording, and its consent and PII state follow from its parents.

    upload (video, consent, pii=present)
      -> mezzanine (transcode_of) -> audio, review proxy
      -> transcript per segment (transcript_of audio @ time range)
      -> redacted transcript (redaction_of, pii lowered to "redacted": the only
         loosening, granted by the step's spec)
      -> record per question (extracted_from transcript; each claim maps to a
         character span)
      -> manifest (derived_from records; references the source)

Run input:
    {
      "source": {"$asset": "<uploaded interview video>"},
      "expert": {"id": "exp_123", "name": "Jane Doe"},
      "turns": [{"question": ..., "answer": ..., "start": 0.0, "end": 41.5, "asr_confidence": 0.97}]
    }
`turns` stands in for what diarisation and speech recognition would produce.
"""

from __future__ import annotations

import hashlib
import re

from wes import Context, Derivation, HandlerRegistry, PermanentError, ref
from wes.models import PointerSelector, TextSelector, TimeSelector

registry = HandlerRegistry()

EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE = re.compile(r"(?<!\w)(?:\+?\d{1,2}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}(?!\w)")
SENTENCE = re.compile(r"[^.!?]+[.!?]*")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _turns(ctx: Context) -> list[dict]:
    turns = ctx.run_input.get("turns")
    if not turns:
        raise PermanentError("run input has no 'turns' (stub for diarisation + ASR output)")
    return turns


def _stub_media(label: str, source_sha: str) -> bytes:
    """Deterministic stand-in bytes for a derived media file."""
    return f"stub {label} rendered from {source_sha}\n".encode()


@registry.register("probe", version="2.0.0", models={"probe": "stub-ffprobe"})
def probe(ctx: Context) -> dict:
    """Production: ffprobe the object and reject corrupt or unsupported containers.

    Checks the stored bytes against the catalog's hash and the expert's
    consent before any compute is spent.
    """
    source = ctx.run_input.get("source")
    if not source:
        raise PermanentError("run input needs 'source': the uploaded interview recording")
    src = ctx.assets.get(source)
    if src["kind"] != "video":
        raise PermanentError(f"source is a {src['kind']} asset; this pipeline needs video")
    if not ctx.assets.verify(src):
        raise PermanentError("stored bytes do not match the catalog hash; refusing to process")
    missing = sorted(set(ctx.params.get("required_consent", [])) - set(src["policy"]["consent"]))
    if missing:
        raise PermanentError(f"expert has not consented to: {missing}")
    turns = _turns(ctx)
    return {
        "source": ref(src),
        "sha256": src["sha256"],
        "media_type": src["media_type"],
        "duration_seconds": src["metadata"].get("duration_seconds") or max(t["end"] for t in turns),
    }


@registry.register("normalize", version="2.0.0", models={"transcoder": "stub-ffmpeg"})
def normalize(ctx: Context) -> dict:
    """Production: transcode to a mezzanine format, split the audio track, render a review proxy."""
    p = ctx.deps["probe"]
    duration, sha = p["duration_seconds"], p["sha256"]
    mezz = ctx.assets.put(
        _stub_media("mezzanine", sha), name="mezzanine", kind="video", media_type="video/mp4",
        derived_from=[(p["source"], "transcode_of")],
        metadata={"duration_seconds": duration, "codec": "h264", "width": 1920, "height": 1080},
    )
    audio = ctx.assets.put(
        _stub_media("audio", sha), name="audio", kind="audio", media_type="audio/flac",
        derived_from=[(mezz, "derived_from")],
        metadata={"duration_seconds": duration, "sample_rate": 16000, "channels": 1},
    )
    proxy = ctx.assets.put(
        _stub_media("proxy_480p", sha), name="review_proxy", kind="video", media_type="video/mp4",
        derived_from=[(mezz, "rendition_of")],
        metadata={"duration_seconds": duration, "codec": "h264", "width": 854, "height": 480},
    )
    return {"mezzanine": ref(mezz), "audio": ref(audio), "review_proxy": ref(proxy)}


@registry.register("segment", version="2.0.0", models={"vad": "stub-vad"})
def segment(ctx: Context) -> dict:
    """Production: voice-activity detection + diarisation, then cut on question turns.

    Segments are selectors into the audio, not copies of it.
    """
    audio = ctx.deps["normalize"]["audio"]
    return {
        "segments": [
            {"index": i, "start": t["start"], "end": t["end"], "question": t["question"],
             "audio": audio}
            for i, t in enumerate(_turns(ctx))
        ]
    }


@registry.register("transcribe", version="2.0.0", models={"asr": "stub-asr-1"})
def transcribe(ctx: Context) -> dict:
    """Production: GPU speech recognition on one audio segment, with word timestamps."""
    seg = ctx.item
    turn = _turns(ctx)[seg["index"]]
    confidence = turn.get("asr_confidence", 0.95)
    transcript = ctx.assets.put_text(
        turn["answer"], name="transcript",
        derived_from=[(seg["audio"], "transcript_of",
                       {"type": "time", "start": seg["start"], "end": seg["end"]})],
        metadata={"language": "en", "asr_confidence": confidence},
    )
    return {
        "index": seg["index"], "start": seg["start"], "end": seg["end"],
        "question": seg["question"], "transcript": ref(transcript), "asr_confidence": confidence,
    }


@registry.register("redact", version="2.0.0", models={"pii": "regex-pii-1"})
def redact(ctx: Context) -> dict:
    """Production: NER-based PII detection; also mute/blur the matching media spans.

    Its outputs declare pii="redacted". The engine allows that only because
    the step's spec lists it in policy_transforms.
    """
    expert_name = (ctx.run_input.get("expert") or {}).get("name")
    turns, total = [], 0
    for t in ctx.deps["transcribe"]["items"]:
        text = ctx.assets.read_text(t["transcript"])
        text, n_email = EMAIL.subn("[EMAIL]", text)
        text, n_phone = PHONE.subn("[PHONE]", text)
        n_name = 0
        if expert_name:
            text, n_name = re.subn(re.escape(expert_name), "[EXPERT]", text, flags=re.IGNORECASE)
        n = n_email + n_phone + n_name
        total += n
        redacted = ctx.assets.put_text(
            text, name=f"redacted_{t['index']}",
            derived_from=[(t["transcript"], "redaction_of")],
            metadata={"redactions": n}, policy={"pii": "redacted"},
        )
        turns.append({**t, "transcript": ref(redacted), "redactions": n})
    return {"turns": turns, "redactions": total}


@registry.register("extract", version="2.0.0", models={"extractor": "stub-extractor-1"})
def extract(ctx: Context) -> dict:
    """Production: an LLM fills the customer's schema for one question/answer pair.

    The record is an asset. Its summary and each claim map back to the exact
    character span of the redacted transcript they came from, which in turn
    maps to a time range of the recording.
    """
    turn = ctx.item
    source_text = ctx.assets.read_text(turn["transcript"])
    spans = [
        (m.start() + len(m.group()) - len(m.group().lstrip()), m.end())
        for m in SENTENCE.finditer(source_text) if m.group().strip()
    ]
    sentences = [source_text[a:b].strip() for a, b in spans]
    lowered = f"{turn['question']} {source_text}".lower()
    vocabulary = ctx.params.get("skills_vocabulary", [])
    claim_idx = [i for i, s in enumerate(sentences)
                 if re.search(r"\d|because|therefore", s, re.IGNORECASE)]
    record = {
        "question": turn["question"],
        "summary": sentences[0] if sentences else "",
        "skills": [s for s in vocabulary if s in lowered],
        "claims": [sentences[i] for i in claim_idx],
        "evidence": {
            "segment_index": turn["index"], "start": turn["start"], "end": turn["end"],
            "source": ctx.run_input["source"]["$asset"],
        },
        "asr_confidence": turn["asr_confidence"],
    }
    derived = [Derivation(parent=turn["transcript"]["$asset"], relation="extracted_from")]
    if sentences:
        derived.append(Derivation(
            parent=turn["transcript"]["$asset"], relation="extracted_from",
            parent_selector=TextSelector(start=spans[0][0], end=spans[0][1]),
            child_selector=PointerSelector(pointer="/summary"),
        ))
    for n, i in enumerate(claim_idx):
        derived.append(Derivation(
            parent=turn["transcript"]["$asset"], relation="extracted_from",
            parent_selector=TextSelector(start=spans[i][0], end=spans[i][1]),
            child_selector=PointerSelector(pointer=f"/claims/{n}"),
        ))
    asset = ctx.assets.put_json(record, name="record", derived_from=derived)
    return {
        "record": ref(asset), "segment_index": turn["index"],
        "asr_confidence": turn["asr_confidence"], "empty": not record["summary"],
    }


@registry.register("quality", version="2.0.0")
def quality(ctx: Context) -> dict:
    """Automated checks, then decide whether a human needs to look.

    Review is triggered by low transcription confidence, an empty extraction,
    or a deterministic random sample (so retries make the same decision).
    """
    records = ctx.deps["extract"]["items"]
    threshold = ctx.params.get("min_asr_confidence", 0.85)
    low = [r["segment_index"] for r in records if r["asr_confidence"] < threshold]
    empty = [r["segment_index"] for r in records if r["empty"]]
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
        "review_proxy": ctx.deps["normalize"]["review_proxy"],
    }


def _provenance(ctx: Context, record_ref: dict) -> dict:
    """Walk the record's recorded lineage back to the uploaded source."""
    graph = ctx.assets.lineage(record_ref, direction="up")
    nodes = {str(n["id"]): n for n in graph["nodes"]}
    spans = [
        {"asset": str(e["parent_id"]), **{k: e["parent_selector"][k] for k in ("start", "end")}}
        for e in graph["edges"]
        if (e["parent_selector"] or {}).get("type") == "time"
    ]
    builds = sorted(
        {(e["activity"]["handler"]["name"], e["activity"]["handler"]["version"])
         for e in graph["edges"] if e["activity"].get("handler")}
    )
    return {
        "sources": [{"asset": i, "sha256": n["sha256"]} for i, n in nodes.items()
                    if n["origin"] == "upload"],
        "media_spans": spans,
        "chain": [{"asset": str(n["id"]), "name": n["name"], "kind": n["kind"], "depth": n["depth"]}
                  for n in graph["nodes"]],
        "handlers": [f"{name}@{version}" for name, version in builds],
    }


@registry.register("package", version="2.0.0")
def package(ctx: Context) -> dict:
    """Assemble the delivery manifest as an asset, with each record's provenance.

    The step's requires_policy gate means the manifest cannot be committed
    unless consent and PII state allow delivery, whatever this code does.
    """
    review = ctx.deps.get("expert_review")  # None when the review step was skipped
    if review and review.get("verdict") == "reject":
        raise PermanentError(f"rejected in expert review: {review.get('notes', 'no notes')}")
    expert_id = (ctx.run_input.get("expert") or {}).get("id", "unknown")
    items = ctx.deps["extract"]["items"]
    records = []
    for item in items:
        meta = ctx.assets.get(item["record"])
        records.append({
            "asset": str(meta["id"]),
            "sha256": meta["sha256"],
            "content": ctx.assets.read_json(item["record"]),
            "provenance": _provenance(ctx, item["record"]),
        })
    manifest = {
        "manifest_version": 2,
        "run_id": ctx.run_id,
        "expert_ref": "exp_" + _sha(f"{ctx.tenant_id}:{expert_id}")[:12],  # pseudonymous, stable per tenant
        "records": records,
        "quality": {k: v for k, v in ctx.deps["quality"].items() if k != "review_proxy"},
        "review": review or {"verdict": "auto_accepted"},
    }
    asset = ctx.assets.put_json(
        manifest, name="manifest",
        derived_from=[(i["record"], "derived_from") for i in items]
        + [(ctx.run_input["source"], "references")],
    )
    return {"manifest": ref(asset), "records": len(records)}
