from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

STEP_ID = r"^[a-z][a-z0-9_]{0,62}$"
WORKFLOW_NAME = r"^[a-z][a-z0-9_\-]{0,62}$"


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
