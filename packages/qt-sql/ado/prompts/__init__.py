"""ADO prompt builders for swarm mode."""

from .swarm_fan_out import build_fan_out_prompt
from .swarm_snipe import (
    build_snipe_prompt,
    build_snipe_worker_context,
    # V2 snipe architecture
    build_snipe_analyst_prompt,
    build_sniper_prompt,
)
from .swarm_common import build_worker_strategy_header
from .swarm_parsers import (
    WorkerAssignment,
    SnipeAnalysis,
    parse_fan_out_response,
    parse_snipe_response,
    # V2 briefing types
    BriefingShared,
    BriefingWorker,
    ParsedBriefing,
    parse_briefing_response,
    # V2 snipe types
    SnipeAnalysisV2,
    parse_snipe_analyst_response,
)
from .analyst_briefing import build_analyst_briefing_prompt
from .worker_v2 import build_worker_v2_prompt
from .briefing_checks import (
    build_analyst_section_checklist,
    build_worker_rewrite_checklist,
    validate_parsed_briefing,
)

__all__ = [
    "build_fan_out_prompt",
    "build_snipe_prompt",
    "build_snipe_worker_context",
    "build_worker_strategy_header",
    "WorkerAssignment",
    "SnipeAnalysis",
    "parse_fan_out_response",
    "parse_snipe_response",
    # V2
    "BriefingShared",
    "BriefingWorker",
    "ParsedBriefing",
    "parse_briefing_response",
    "build_analyst_briefing_prompt",
    "build_worker_v2_prompt",
    "build_analyst_section_checklist",
    "build_worker_rewrite_checklist",
    "validate_parsed_briefing",
    # V2 snipe
    "build_snipe_analyst_prompt",
    "build_sniper_prompt",
    "SnipeAnalysisV2",
    "parse_snipe_analyst_response",
]
