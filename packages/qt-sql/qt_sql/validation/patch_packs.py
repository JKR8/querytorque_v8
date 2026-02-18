"""Patch-pack loader for benchmark-specific synthetic witness recipes.

Core synthetic validation should remain AST-driven and generic. Any benchmark
or dataset-specific witness patches are exposed through this optional module
boundary and must be explicitly enabled by name.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, Callable, Dict, Optional


ApplyRecipeFn = Callable[[Any, Dict[str, Any], Dict[str, Dict[str, Any]]], bool]


@dataclass(frozen=True)
class WitnessPatchPack:
    name: str
    description: str
    apply_recipe: ApplyRecipeFn


_PATCH_PACK_MODULES: Dict[str, tuple[str, ...]] = {
    "dsb_mvrows": (
        "qt_sql.validation.patches.dsb_mvrows",
        "patches.dsb_mvrows",
    ),
}


def available_patch_packs() -> Dict[str, str]:
    """Return supported patch-pack names and import paths."""
    return {name: module_paths[0] for name, module_paths in _PATCH_PACK_MODULES.items()}


def load_witness_patch_pack(name: str) -> Optional[WitnessPatchPack]:
    """Load an optional witness patch pack by name.

    Returns None for "none" / empty names.
    """
    normalized = (name or "").strip().lower()
    if normalized in {"", "none"}:
        return None

    module_paths = _PATCH_PACK_MODULES.get(normalized)
    if not module_paths:
        known = ", ".join(sorted(_PATCH_PACK_MODULES))
        raise ValueError(f"Unknown patch pack '{name}'. Known values: none, {known}")

    module = None
    last_err: Optional[Exception] = None
    for module_path in module_paths:
        try:
            module = import_module(module_path)
            break
        except ModuleNotFoundError as exc:
            last_err = exc
    if module is None:
        raise ModuleNotFoundError(
            f"Unable to import patch pack '{normalized}' from any known paths: {module_paths}"
        ) from last_err

    apply_recipe = getattr(module, "apply_recipe", None)
    if not callable(apply_recipe):
        raise ValueError(f"Patch pack '{normalized}' missing callable apply_recipe()")

    description = str(getattr(module, "DESCRIPTION", "")).strip()
    if not description:
        description = f"{normalized} witness recipe patch pack"

    return WitnessPatchPack(
        name=normalized,
        description=description,
        apply_recipe=apply_recipe,
    )
