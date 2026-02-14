"""ADO prompt builders for canonical modes: oneshot and swarm."""

from .swarm_fan_out import build_fan_out_prompt
from .swarm_snipe import (
    build_retry_worker_prompt,
    build_snipe_analyst_prompt,   # DEPRECATED — kept for reference
    build_sniper_prompt,          # DEPRECATED — kept for reference
)
from .swarm_common import build_worker_strategy_header
# Canonical prompt builders
from .analyst_briefing import build_analyst_briefing_prompt
from .swarm_parsers import (
    parse_briefing_response,
    BriefingShared,
    BriefingWorker,
    ParsedBriefing,
)
from .worker import build_worker_prompt
from .briefing_checks import (
    validate_parsed_briefing,
    build_worker_rewrite_checklist,
)
from .worker_shared_prefix import build_shared_worker_prefix, build_worker_assignment
from .coach import build_coach_prompt, build_coach_refinement_prefix

__all__ = [
    "build_fan_out_prompt",
    "build_worker_strategy_header",
    # Canonical prompt builders
    "build_analyst_briefing_prompt",
    "build_worker_prompt",
    "parse_briefing_response",
    "validate_parsed_briefing",
    # Briefing types
    "BriefingShared",
    "BriefingWorker",
    "ParsedBriefing",
    # Snipe / Retry
    "build_retry_worker_prompt",
    # Shared-prefix + Coach
    "build_shared_worker_prefix",
    "build_worker_assignment",
    "build_coach_prompt",
    "build_coach_refinement_prefix",
    # Checklists
    "build_worker_rewrite_checklist",
]
