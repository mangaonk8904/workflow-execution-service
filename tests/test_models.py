import pytest
from pydantic import ValidationError

from wes import WorkflowSpec
from wes.models import topological_order


def spec(steps):
    return WorkflowSpec.model_validate({"name": "wf", "steps": steps})


def test_valid_dag_orders_dependencies_first():
    s = spec([
        {"id": "c", "depends_on": ["a", "b"]},
        {"id": "a"},
        {"id": "b", "depends_on": ["a"]},
    ])
    order = topological_order(s.steps)
    assert order.index("a") < order.index("b") < order.index("c")


def test_cycle_rejected():
    with pytest.raises(ValidationError, match="cycle"):
        spec([{"id": "a", "depends_on": ["b"]}, {"id": "b", "depends_on": ["a"]}])


def test_unknown_dependency_rejected():
    with pytest.raises(ValidationError, match="unknown steps"):
        spec([{"id": "a", "depends_on": ["ghost"]}])


def test_duplicate_ids_rejected():
    with pytest.raises(ValidationError, match="duplicate"):
        spec([{"id": "a"}, {"id": "a"}])


def test_foreach_must_reference_a_dependency():
    with pytest.raises(ValidationError, match="depends_on"):
        spec([{"id": "a"}, {"id": "b", "foreach": "a.items"}])


def test_step_ids_cannot_contain_fanout_separator():
    with pytest.raises(ValidationError):
        spec([{"id": "a#1"}])
