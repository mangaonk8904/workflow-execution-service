from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

STEP_ID = r"^[a-z][a-z0-9_]{0,62}$"
WORKFLOW_NAME = r"^[a-z][a-z0-9_\-]{0,62}$"

AssetKind = Literal["image", "video", "audio", "document", "text", "structured", "bundle"]
PiiState = Literal["none", "redacted", "unknown", "present"]
# Worse states win when policies are combined.
PII_RANK = {"none": 0, "redacted": 1, "unknown": 2, "present": 3}

# How a child asset relates to a parent. Every relation except `references`
# means the child carries the parent's content, so it inherits its policy.
RELATIONS = frozenset({
    "derived_from", "transcode_of", "rendition_of", "segment_of", "transcript_of",
    "translation_of", "redaction_of", "extracted_from", "summary_of", "part_of",
    "annotates", "aligned_with", "references",
})
NON_INHERITING = frozenset({"references"})


# ---------------------------------------------------------------- selectors
# A selector names a part of an asset. Modelled on W3C Web Annotation
# selectors, cut down to what the pipelines need.


class _Selector(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TimeSelector(_Selector):
    """Seconds into an audio or video asset."""

    type: Literal["time"] = "time"
    start: float = Field(ge=0)
    end: float = Field(ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> "TimeSelector":
        if self.end <= self.start:
            raise ValueError("time selector needs end > start")
        return self


class RegionSelector(_Selector):
    """A box on an image, a video frame (at time t), or a document page."""

    type: Literal["region"] = "region"
    x: float = Field(ge=0)
    y: float = Field(ge=0)
    w: float = Field(gt=0)
    h: float = Field(gt=0)
    unit: Literal["px", "fraction"] = "px"
    t: float | None = Field(None, ge=0)

    @model_validator(mode="after")
    def _fractions(self) -> "RegionSelector":
        if self.unit == "fraction" and (self.x + self.w > 1 or self.y + self.h > 1):
            raise ValueError("fractional region must lie within [0, 1]")
        return self


class PageSelector(_Selector):
    """One page, or an inclusive page range, of a document (1-based)."""

    type: Literal["page"] = "page"
    page: int = Field(ge=1)
    end_page: int | None = Field(None, ge=1)

    @model_validator(mode="after")
    def _ordered(self) -> "PageSelector":
        if self.end_page is not None and self.end_page < self.page:
            raise ValueError("page selector needs end_page >= page")
        return self


class TextSelector(_Selector):
    """Character offsets [start, end) into a text asset."""

    type: Literal["text"] = "text"
    start: int = Field(ge=0)
    end: int = Field(ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> "TextSelector":
        if self.end <= self.start:
            raise ValueError("text selector needs end > start")
        return self


class PointerSelector(_Selector):
    """RFC 6901 JSON pointer into a structured asset."""

    type: Literal["json_pointer"] = "json_pointer"
    pointer: str = Field(pattern=r"^(/.*)?$")


class CompositeSelector(_Selector):
    """All of the parts together, e.g. a page and a region on it."""

    type: Literal["composite"] = "composite"
    items: list["Selector"] = Field(min_length=2, max_length=8)


Selector = Annotated[
    Union[TimeSelector, RegionSelector, PageSelector, TextSelector, PointerSelector,
          CompositeSelector],
    Field(discriminator="type"),
]
CompositeSelector.model_rebuild()

# Which selector types make sense on which kind of asset.
SELECTOR_KINDS: dict[str, frozenset[str]] = {
    "time": frozenset({"video", "audio"}),
    "region": frozenset({"image", "video", "document"}),
    "page": frozenset({"document"}),
    "text": frozenset({"text", "document"}),
    "json_pointer": frozenset({"structured", "bundle"}),
}


def check_selector(selector: Any, kind: str, metadata: dict[str, Any]) -> None:
    """Raise ValueError if `selector` cannot point into an asset of this kind and shape.

    Bounds are checked against technical metadata when it is known:
    duration_seconds, width/height, page_count, char_count.
    """
    if isinstance(selector, CompositeSelector):
        for item in selector.items:
            check_selector(item, kind, metadata)
        return
    allowed = SELECTOR_KINDS[selector.type]
    if kind not in allowed:
        raise ValueError(f"a {selector.type} selector cannot point into a {kind} asset")
    eps = 1e-6
    if isinstance(selector, TimeSelector):
        duration = metadata.get("duration_seconds")
        if duration is not None and selector.end > float(duration) + eps:
            raise ValueError(f"time selector ends at {selector.end}s; asset is {duration}s long")
    elif isinstance(selector, RegionSelector):
        if selector.unit == "px":
            width, height = metadata.get("width"), metadata.get("height")
            if width is not None and selector.x + selector.w > width + eps:
                raise ValueError("region extends past the asset's width")
            if height is not None and selector.y + selector.h > height + eps:
                raise ValueError("region extends past the asset's height")
        duration = metadata.get("duration_seconds")
        if selector.t is not None and duration is not None and selector.t > float(duration) + eps:
            raise ValueError("region frame time is past the end of the asset")
    elif isinstance(selector, PageSelector):
        pages = metadata.get("page_count")
        last = selector.end_page or selector.page
        if pages is not None and last > pages:
            raise ValueError(f"page selector reaches page {last}; asset has {pages} pages")
    elif isinstance(selector, TextSelector):
        chars = metadata.get("char_count")
        if chars is not None and selector.end > chars:
            raise ValueError(f"text selector ends at {selector.end}; asset has {chars} characters")


# ---------------------------------------------------------------- policy


class Policy(BaseModel):
    """What may be done with an asset. Derived assets inherit the strictest of their parents.

    consent       purposes the data subject agreed to, e.g. "share_with_customers"
    pii           none < redacted < unknown < present
    retention_days  delete after this many days (None: no limit)
    restrictions  free-form flags that only accumulate, e.g. "no_model_training"
    """

    model_config = ConfigDict(extra="forbid")

    consent: list[str] = Field(default_factory=list)
    pii: PiiState = "unknown"
    retention_days: int | None = Field(None, ge=1)
    restrictions: list[str] = Field(default_factory=list)

    @field_validator("consent", "restrictions")
    @classmethod
    def _sorted_unique(cls, v: list[str]) -> list[str]:
        return sorted(set(v))

    @classmethod
    def inherit(cls, parents: list["Policy"]) -> "Policy":
        if not parents:
            raise ValueError("cannot inherit a policy from no parents")
        consent = set(parents[0].consent)
        for p in parents[1:]:
            consent &= set(p.consent)
        retention = [p.retention_days for p in parents if p.retention_days is not None]
        return cls(
            consent=sorted(consent),
            pii=max((p.pii for p in parents), key=PII_RANK.__getitem__),
            retention_days=min(retention) if retention else None,
            restrictions=sorted({r for p in parents for r in p.restrictions}),
        )

    def apply(self, requested: dict[str, Any] | None, allowed: dict[str, str]) -> tuple["Policy", list[dict]]:
        """Apply a step's requested changes to this inherited policy.

        Tightening is always allowed. Loosening is refused, except the PII
        state a step's spec explicitly grants in `policy_transforms` (a
        redaction step may declare its output pii="redacted").
        Returns the new policy and the list of loosening changes made.
        """
        if not requested:
            return self, []
        unknown = set(requested) - set(Policy.model_fields)
        if unknown:
            raise ValueError(f"unknown policy fields: {sorted(unknown)}")
        merged = self.model_dump()
        changes: list[dict] = []
        if "consent" in requested:
            wanted = set(requested["consent"])
            if not wanted <= set(self.consent):
                raise ValueError(
                    f"cannot widen consent beyond {self.consent}: {sorted(wanted - set(self.consent))}"
                )
            merged["consent"] = sorted(wanted)
        if "pii" in requested:
            wanted = requested["pii"]
            if wanted not in PII_RANK:
                raise ValueError(f"unknown pii state {wanted!r}")
            if PII_RANK[wanted] < PII_RANK[self.pii]:
                if allowed.get("pii") != wanted:
                    raise ValueError(
                        f"step may not lower pii from {self.pii!r} to {wanted!r}; "
                        "declare it in the step's policy_transforms"
                    )
                changes.append({"field": "pii", "from": self.pii, "to": wanted})
            merged["pii"] = wanted
        if "retention_days" in requested:
            wanted = requested["retention_days"]
            if self.retention_days is not None and (wanted is None or wanted > self.retention_days):
                raise ValueError(f"cannot extend retention beyond {self.retention_days} days")
            merged["retention_days"] = wanted
        if "restrictions" in requested:
            wanted = set(requested["restrictions"])
            if not wanted >= set(self.restrictions):
                raise ValueError(
                    f"cannot drop restrictions: {sorted(set(self.restrictions) - wanted)}"
                )
            merged["restrictions"] = sorted(wanted)
        return Policy.model_validate(merged), changes


class PolicyRequirement(BaseModel):
    """A gate: every asset a step commits or references must satisfy it."""

    model_config = ConfigDict(extra="forbid")

    consent: list[str] = Field(default_factory=list)
    pii: list[PiiState] | None = None

    def violations(self, policy: Policy) -> list[str]:
        problems = []
        missing = sorted(set(self.consent) - set(policy.consent))
        if missing:
            problems.append(f"missing consent {missing}")
        if self.pii is not None and policy.pii not in self.pii:
            problems.append(f"pii is {policy.pii!r}, allowed {self.pii}")
        return problems


class Derivation(BaseModel):
    """One lineage edge from a new asset to an existing one."""

    model_config = ConfigDict(extra="forbid")

    parent: str
    relation: str = "derived_from"
    parent_selector: Selector | None = None
    child_selector: Selector | None = None

    @field_validator("relation")
    @classmethod
    def _known(cls, v: str) -> str:
        if v not in RELATIONS:
            raise ValueError(f"unknown relation {v!r}; expected one of {sorted(RELATIONS)}")
        return v


class RetryPolicy(BaseModel):
    max_attempts: int = Field(3, ge=1, le=20)
    backoff_seconds: float = Field(2.0, ge=0)
    max_backoff_seconds: float = Field(60.0, ge=0)


class StepSpec(BaseModel):
    id: str = Field(pattern=STEP_ID)
    # Handler name. Workers subscribe by type; defaults to the step id.
    type: str | None = None
    kind: Literal["task", "human"] = "task"
    depends_on: list[str] = Field(default_factory=list)
    # "<dep>.<key>" pointing at a list in a dependency's output: one child per item.
    foreach: str | None = None
    # "<dep>.<key>": the step is skipped when the value is falsy.
    when: str | None = None
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout_seconds: int | None = Field(None, gt=0)
    # Human steps: emit step.sla_breached once this much time has passed.
    sla_seconds: int | None = Field(None, gt=0)
    params: dict[str, Any] = Field(default_factory=dict)
    # Policy loosenings this step's outputs may declare, e.g. {"pii": "redacted"}
    # on a redaction step. Kept in the spec so they are versioned and reviewed.
    policy_transforms: dict[str, str] = Field(default_factory=dict)
    # Gate checked when the step completes: every asset it commits or
    # references in its output must satisfy this, or the step fails.
    requires_policy: PolicyRequirement | None = None

    @field_validator("policy_transforms")
    @classmethod
    def _transforms(cls, v: dict[str, str]) -> dict[str, str]:
        for key, value in v.items():
            if key != "pii" or value not in ("redacted", "none"):
                raise ValueError(
                    "policy_transforms may only grant pii='redacted' or pii='none'; "
                    "consent, retention and restrictions can never be loosened by a step"
                )
        return v

    @property
    def handler(self) -> str:
        return self.type or self.id


class WorkflowSpec(BaseModel):
    name: str = Field(pattern=WORKFLOW_NAME)
    description: str | None = None
    steps: list[StepSpec] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def _validate_dag(self) -> "WorkflowSpec":
        ids = [s.id for s in self.steps]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(f"duplicate step ids: {dupes}")
        known = set(ids)
        for s in self.steps:
            missing = [d for d in s.depends_on if d not in known]
            if missing:
                raise ValueError(f"step '{s.id}' depends on unknown steps: {missing}")
            if s.id in s.depends_on:
                raise ValueError(f"step '{s.id}' depends on itself")
            for field in ("foreach", "when"):
                ref = getattr(s, field)
                if ref and ref.split(".")[0] not in s.depends_on:
                    raise ValueError(
                        f"step '{s.id}': {field} '{ref}' must reference a step listed in depends_on"
                    )
            if s.kind == "human" and s.foreach:
                raise ValueError(f"step '{s.id}': human steps cannot use foreach")
            if s.kind == "human" and s.policy_transforms:
                raise ValueError(f"step '{s.id}': human steps cannot declare policy_transforms")
        topological_order(self.steps)  # raises on cycles
        return self

    def step(self, step_id: str) -> StepSpec:
        for s in self.steps:
            if s.id == step_id:
                return s
        raise KeyError(step_id)


def topological_order(steps: list[StepSpec]) -> list[str]:
    """Kahn's algorithm. Raises ValueError naming the steps on a cycle."""
    indegree = {s.id: len(set(s.depends_on)) for s in steps}
    dependents: dict[str, list[str]] = {s.id: [] for s in steps}
    for s in steps:
        for d in set(s.depends_on):
            dependents[d].append(s.id)
    queue = [i for i, n in indegree.items() if n == 0]
    order: list[str] = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for nxt in dependents[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if len(order) != len(steps):
        raise ValueError(f"workflow has a cycle involving: {sorted(set(indegree) - set(order))}")
    return order
