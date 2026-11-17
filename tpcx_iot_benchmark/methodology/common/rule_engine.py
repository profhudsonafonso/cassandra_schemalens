"""Generic declarative rule engine used by both Cassandra and TSDB families.

The engine deliberately knows nothing about SmartRetail query IDs and nothing
about the semantic meaning of C0..C9 or TS0..TS8.  It only evaluates conditions
stored in JSON configuration files.

This separation is important for a supervisor demonstration:

    evidence -> generic comparisons -> activated families

Changing a family policy therefore means changing a versioned JSON rule, not
adding query-specific conditional branches.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class ConditionResult:
    group: str
    passed: bool
    detail: str


@dataclass
class RuleResult:
    rule_id: str
    family: str
    passed: bool
    reason: str
    conditions: list[ConditionResult]


OPERATORS = {
    "eq": lambda actual, expected: actual == expected,
    "ne": lambda actual, expected: actual != expected,
    "gte": lambda actual, expected: actual is not None and actual >= expected,
    "lte": lambda actual, expected: actual is not None and actual <= expected,
    "gt": lambda actual, expected: actual is not None and actual > expected,
    "lt": lambda actual, expected: actual is not None and actual < expected,
    "contains": lambda actual, expected: expected in actual if actual is not None else False,
    "in": lambda actual, expected: actual in expected,
}


def _eval_condition(values: dict[str, Any], condition: dict[str, Any], group: str) -> ConditionResult:
    field = condition["field"]
    op = condition["op"]
    expected = condition.get("value")
    if op not in OPERATORS:
        raise ValueError(f"Unsupported rule operator: {op}")
    actual = values.get(field)
    passed = bool(OPERATORS[op](actual, expected))
    return ConditionResult(group=group, passed=passed, detail=f"{field}={actual!r} {op} {expected!r}")


def evaluate_rule(values: dict[str, Any], rule: dict[str, Any], active_families: list[str]) -> RuleResult:
    """Evaluate one rule against one evidence vector.

    Supported declarative groups:
    - ``all``: every condition must pass;
    - ``any``: at least one condition must pass;
    - ``requires_families``: all listed families must already be active;
    - ``requires_any_families``: at least one listed family must be active.
    """
    results: list[ConditionResult] = []

    all_conditions = rule.get("all", [])
    any_conditions = rule.get("any", [])
    required_families = rule.get("requires_families", [])
    required_any_families = rule.get("requires_any_families", [])

    all_passed = True
    if all_conditions:
        evaluated = [_eval_condition(values, condition, "all") for condition in all_conditions]
        results.extend(evaluated)
        all_passed = all(item.passed for item in evaluated)

    any_passed = True
    if any_conditions:
        evaluated = [_eval_condition(values, condition, "any") for condition in any_conditions]
        results.extend(evaluated)
        any_passed = any(item.passed for item in evaluated)

    required_passed = True
    if required_families:
        required_passed = all(family in active_families for family in required_families)
        results.append(
            ConditionResult(
                group="requires_families",
                passed=required_passed,
                detail=f"required_all={required_families!r}, active={active_families!r}",
            )
        )

    required_any_passed = True
    if required_any_families:
        required_any_passed = any(family in active_families for family in required_any_families)
        results.append(
            ConditionResult(
                group="requires_any_families",
                passed=required_any_passed,
                detail=f"required_any={required_any_families!r}, active={active_families!r}",
            )
        )

    return RuleResult(
        rule_id=rule["rule_id"],
        family=rule["family"],
        passed=all_passed and any_passed and required_passed and required_any_passed,
        reason=rule["reason"],
        conditions=results,
    )


def activate(values: dict[str, Any], rules_config: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    """Return active families and a full pass/fail audit trace."""
    active: list[str] = []
    trace: list[dict[str, Any]] = []
    for rule in rules_config["rules"]:
        result = evaluate_rule(values, rule, active)
        if result.passed:
            active.append(result.family)
        trace.append(asdict(result))
    return active, trace
