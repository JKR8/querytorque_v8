"""ADO prompt builders for swarm mode."""

from .swarm_fan_out import build_fan_out_prompt
from .swarm_snipe import (
    build_retry_worker_prompt,
    build_snipe_analyst_prompt,   # DEPRECATED — kept for reference
    build_sniper_prompt,          # DEPRECATED — kept for reference
)
from .swarm_common import build_worker_strategy_header
from .swarm_parsers import (
    WorkerAssignment,
    parse_fan_out_response,
    # Briefing types
    BriefingShared,
    BriefingWorker,
    ParsedBriefing,
    parse_briefing_response,
    # Oneshot types
    OneshotResult,
    parse_oneshot_response,
    # Snipe types
    SnipeAnalysis,
    parse_snipe_response,
)
from .analyst_briefing import build_analyst_briefing_prompt
from .worker import build_worker_prompt
from .worker_shared_prefix import build_shared_worker_prefix, build_worker_assignment
from .coach import build_coach_prompt, build_coach_refinement_prefix
from .briefing_checks import (
    build_analyst_section_checklist,
    build_expert_section_checklist,
    build_oneshot_section_checklist,
    build_worker_rewrite_checklist,
    validate_parsed_briefing,
)

__all__ = [
    "build_fan_out_prompt",
    "build_worker_strategy_header",
    "WorkerAssignment",
    "parse_fan_out_response",
    # Briefing
    "BriefingShared",
    "BriefingWorker",
    "ParsedBriefing",
    "parse_briefing_response",
    # Oneshot
    "OneshotResult",
    "parse_oneshot_response",
    "build_analyst_briefing_prompt",
    "build_worker_prompt",
    "build_analyst_section_checklist",
    "build_expert_section_checklist",
    "build_oneshot_section_checklist",
    "build_worker_rewrite_checklist",
    "validate_parsed_briefing",
    # Snipe / Retry
    "build_retry_worker_prompt",
    "build_snipe_analyst_prompt",   # DEPRECATED
    "build_sniper_prompt",          # DEPRECATED
    "SnipeAnalysis",
    "parse_snipe_response",
    # Shared-prefix + Coach
    "build_shared_worker_prefix",
    "build_worker_assignment",
    "build_coach_prompt",
    "build_coach_refinement_prefix",
]
