"""ADO optimization sessions — Beam, Swarm (legacy), and Oneshot (legacy) modes."""

from .base_session import OptimizationSession
from .beam_session import BeamSession
from .oneshot_session import OneshotSession
from .swarm_session import SwarmSession

# Backwards compat alias
OneshotPatchSession = BeamSession

__all__ = [
    "OptimizationSession",
    "BeamSession",
    "OneshotSession",
    "OneshotPatchSession",  # deprecated alias
    "SwarmSession",
]
