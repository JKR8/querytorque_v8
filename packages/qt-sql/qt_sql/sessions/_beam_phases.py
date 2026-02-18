"""Beam phase methods and BeamContext — extracted for wave mode.

These are imported into beam_session.py and added as methods on BeamSession.
The BeamContext dataclass and all phase methods live here to keep beam_session.py
manageable.
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .beam_session import AppliedPatch, PatchIterationResult, BeamSession
    from ..schemas import SessionResult

logger = logging.getLogger(__name__)


@dataclass
class BeamContext:
    """All shared state needed by beam phase methods.

    Created once by prepare_context(), consumed by all subsequent phases.
    """

    session_dir: Path
    db_path: str
    script_ir: Any
    ir_node_map: str
    base_tree: dict
    base_tree_prompt: str
    tree_mode: bool
    original_explain: str
    baseline_ms: Optional[float]
    importance_stars: int
    schema_context: str
    engine_knowledge: str
    gold_examples: list
    intelligence_brief: str
    analyst_call_fn: Any  # Callable[[str], str]
    worker_call_fn: Any   # Callable[[str], str]
    worker_call_fn_by_patch_id: Dict[str, Any] = field(default_factory=dict)
    max_probes: int = 16
    beam_provider_override: Optional[str] = None
    beam_model_override: Optional[str] = None
    worker_provider_override: Optional[str] = None
    worker_model_override: Optional[str] = None
    dialect_enum: Any = None
    qerror_analysis: Optional[Any] = None
    iteration_history: Optional[Any] = None
    target_speedup: float = 10.0
    worker_slots: int = 8
    launch_interval_s: float = 0.0
