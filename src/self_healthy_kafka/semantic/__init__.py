"""Backend-owned semantic contracts for the chat analytics service.

The package deliberately describes business concepts rather than accepting
database objects from a model.  It is shared by planning, compilation,
evidence construction, and conversation context.
"""

from self_healthy_kafka.semantic.catalog import CATALOG_VERSION, SEMANTIC_CATALOG
from self_healthy_kafka.semantic.jev import (
    HttpJEVAdapter,
    JEVDecision,
    JEVResponseError,
    MockJEVProvider,
    parse_jev_response,
    parse_typesafe_response,
)
from self_healthy_kafka.semantic.planner import (
    SemanticPlan,
    SemanticPlanError,
    SemanticPlanner,
    compile_analytics_request,
)

__all__ = [
    "CATALOG_VERSION",
    "SEMANTIC_CATALOG",
    "SemanticPlan",
    "SemanticPlanError",
    "SemanticPlanner",
    "compile_analytics_request",
    "HttpJEVAdapter",
    "JEVDecision",
    "JEVResponseError",
    "MockJEVProvider",
    "parse_jev_response",
    "parse_typesafe_response",
]
