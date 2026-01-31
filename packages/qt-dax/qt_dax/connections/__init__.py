"""QueryTorque DAX Connections.

Provides Power BI Desktop connection via XMLA endpoint.
"""

from .pbi_desktop import (
    PBIDesktopConnection,
    PBIInstance,
    find_pbi_instances,
    validate_dax_against_desktop,
)

__all__ = [
    "PBIDesktopConnection",
    "PBIInstance",
    "find_pbi_instances",
    "validate_dax_against_desktop",
]
