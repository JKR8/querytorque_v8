"""Reward function for MCTS SQL optimizer.

The reward function guides MCTS towards finding optimizations that are:
1. Semantically correct (pass validation)
2. Actually faster (have speedup > 1)

Reward design:
- Invalid optimizations: 0 reward (no incentive)
- Valid but slower: small positive (correct is better than nothing)
- Valid and faster: proportional to speedup (capped)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class RewardConfig:
    """Configuration for reward computation.

    Attributes:
        max_reward: Maximum reward to prevent outliers dominating.
        min_valid_reward: Minimum reward for valid but slower queries.
        speedup_threshold: Speedup threshold for "good" optimization.
        great_speedup: Speedup threshold for "great" optimization.
        penalty_slower: Reward for valid but slower queries.
    """

    max_reward: float = 5.0
    min_valid_reward: float = 0.2
    speedup_threshold: float = 1.1
    great_speedup: float = 2.0
    penalty_slower: float = 0.2


def compute_reward(
    validation_result: Any,
    config: Optional[RewardConfig] = None,
) -> float:
    """Compute reward from a validation result.

    Reward structure:
    - status != "pass": 0.0 (semantic error = no reward)
    - speedup >= 2.0: min(speedup, max_reward) (great improvement)
    - speedup >= 1.1: speedup (good improvement)
    - speedup >= 1.0: 0.5 (correct but no improvement)
    - speedup < 1.0: penalty_slower (correct but slower)

    Args:
        validation_result: Result from SQLValidator.validate().
        config: Reward configuration.

    Returns:
        Reward value.
    """
    if config is None:
        config = RewardConfig()

    # Check validation status
    status = getattr(validation_result, "status", None)

    # Handle ValidationStatus enum or string
    if hasattr(status, "value"):
        status_str = status.value
    else:
        status_str = str(status).lower() if status else ""

    if status_str != "pass":
        # Semantic error = no reward
        # This strongly discourages invalid transformations
        return 0.0

    # Get speedup
    speedup = getattr(validation_result, "speedup", 1.0)

    # Handle edge cases
    if speedup == float("inf"):
        speedup = config.max_reward
    if speedup <= 0:
        speedup = 1.0

    # Compute reward based on speedup
    if speedup >= config.great_speedup:
        # Great improvement: use speedup but cap it
        return min(speedup, config.max_reward)

    elif speedup >= config.speedup_threshold:
        # Good improvement: linear reward
        return speedup

    elif speedup >= 1.0:
        # No improvement but correct
        # Small positive reward to prefer valid over invalid
        return 0.5

    else:
        # Slower but correct
        # Small reward - correct is still better than nothing
        return config.penalty_slower


def compute_reward_with_details(
    validation_result: Any,
    config: Optional[RewardConfig] = None,
) -> tuple[float, dict]:
    """Compute reward with detailed breakdown.

    Args:
        validation_result: Result from SQLValidator.validate().
        config: Reward configuration.

    Returns:
        Tuple of (reward, details_dict).
    """
    if config is None:
        config = RewardConfig()

    reward = compute_reward(validation_result, config)

    # Get status
    status = getattr(validation_result, "status", None)
    if hasattr(status, "value"):
        status_str = status.value
    else:
        status_str = str(status).lower() if status else "unknown"

    # Get speedup
    speedup = getattr(validation_result, "speedup", 1.0)

    # Categorize
    if status_str != "pass":
        category = "invalid"
    elif speedup >= config.great_speedup:
        category = "great"
    elif speedup >= config.speedup_threshold:
        category = "good"
    elif speedup >= 1.0:
        category = "no_improvement"
    else:
        category = "slower"

    details = {
        "status": status_str,
        "speedup": speedup,
        "reward": reward,
        "category": category,
        "config": {
            "max_reward": config.max_reward,
            "great_speedup": config.great_speedup,
            "speedup_threshold": config.speedup_threshold,
        },
    }

    return reward, details


class AdaptiveRewardConfig(RewardConfig):
    """Reward config that adapts based on observed speedups.

    Can be used to normalize rewards if most queries have small speedups.
    """

    def __init__(
        self,
        observed_speedups: Optional[list[float]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.observed_speedups = observed_speedups or []

    def update(self, speedup: float) -> None:
        """Update with newly observed speedup."""
        self.observed_speedups.append(speedup)

    def get_normalized_reward(self, speedup: float) -> float:
        """Get reward normalized by observed distribution.

        Uses percentile-based normalization if enough data.
        """
        if len(self.observed_speedups) < 10:
            # Not enough data, use default
            return min(speedup, self.max_reward) if speedup >= 1.0 else self.penalty_slower

        # Sort observed speedups
        sorted_speedups = sorted(self.observed_speedups)

        # Find percentile of this speedup
        count_below = sum(1 for s in sorted_speedups if s < speedup)
        percentile = count_below / len(sorted_speedups)

        # Map percentile to reward [0, max_reward]
        return percentile * self.max_reward
