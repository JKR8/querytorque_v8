"""
Power BI Desktop local connection.

Connects to running Power BI Desktop instances via localhost XMLA endpoint.
Same approach as DAX Studio - zero auth required for local connections.

Requirements:
- Windows (PBI Desktop is Windows-only)
- pyadomd package: pip install pyadomd
- Power BI Desktop running with a model loaded

Usage:
    from qt_dax.connections import PBIDesktopConnection, find_pbi_instances

    # Find running instances
    instances = find_pbi_instances()
    for inst in instances:
        print(f"{inst.name} on port {inst.port}")

    # Connect to first instance
    conn = PBIDesktopConnection(instances[0].port)

    # Execute DAX
    result = conn.execute_dax("EVALUATE ROW('Test', 1+1)")
    print(result)

    # Get model info
    tables = conn.get_tables()
    measures = conn.get_measures()
"""

import glob
import os
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Lazy import - pyadomd is Windows-only and optional
_pyadomd = None


def _get_pyadomd():
    """Lazy load pyadomd, with helpful error if not available."""
    global _pyadomd
    if _pyadomd is None:
        try:
            import pyadomd
            _pyadomd = pyadomd
        except ImportError as e:
            error_msg = str(e)
            if "AdomdClient" in error_msg:
                raise ImportError(
                    "pyadomd found but ADOMD client libraries are missing.\n\n"
                    "Install the Analysis Services client libraries:\n"
                    "  1. Download from: https://learn.microsoft.com/en-us/analysis-services/client-libraries\n"
                    "  2. Run the MSOLAP (amd64) installer\n"
                    "  3. Restart Python\n\n"
                    "The DLL should install to:\n"
                    "  C:\\Windows\\Microsoft.NET\\assembly\\GAC_MSIL\\Microsoft.AnalysisServices.AdomdClient"
                )
            raise ImportError(
                "pyadomd is required for Power BI Desktop connection.\n"
                "Install with: pip install pyadomd\n"
                "Note: Windows only, requires .NET Framework."
            )
    return _pyadomd


@dataclass
class PBIInstance:
    """A running Power BI Desktop instance."""
    port: int
    name: str
    workspace_path: str


def find_pbi_instances() -> list[PBIInstance]:
    """
    Find all running Power BI Desktop instances.

    Scans for msmdsrv.port.txt files created by PBI Desktop's
    local Analysis Services instance. Supports both:
    - Classic installer: AppData/Local/Microsoft/Power BI Desktop/
    - Microsoft Store: [User]/Microsoft/Power BI Desktop Store App/

    Returns:
        List of PBIInstance objects for each running instance.
    """
    if platform.system() != "Windows":
        raise OSError("Power BI Desktop connection only supported on Windows")

    instances = []
    user_home = os.path.expanduser("~")
    local_app_data = os.environ.get("LOCALAPPDATA", "")

    # Paths to check for port files
    search_patterns = []

    # Classic installer path
    if local_app_data:
        search_patterns.append(os.path.join(
            local_app_data,
            "Microsoft",
            "Power BI Desktop",
            "AnalysisServicesWorkspaces",
            "*",
            "Data",
            "msmdsrv.port.txt"
        ))

    # Microsoft Store version path
    search_patterns.append(os.path.join(
        user_home,
        "Microsoft",
        "Power BI Desktop Store App",
        "AnalysisServicesWorkspaces",
        "*",
        "Data",
        "msmdsrv.port.txt"
    ))

    for pattern in search_patterns:
        for port_file in glob.glob(pattern):
            try:
                # Port file may be UTF-16 encoded (has null bytes)
                with open(port_file, "rb") as f:
                    content = f.read()

                # Try UTF-16 first (Windows often uses this)
                try:
                    port_str = content.decode("utf-16").strip()
                except UnicodeDecodeError:
                    port_str = content.decode("utf-8").strip()

                # Remove any spaces or null chars
                port_str = port_str.replace(" ", "").replace("\x00", "")
                port = int(port_str)

                # Extract workspace name from path
                workspace_path = Path(port_file).parent.parent
                workspace_name = workspace_path.name

                instances.append(PBIInstance(
                    port=port,
                    name=workspace_name,
                    workspace_path=str(workspace_path)
                ))
            except (ValueError, IOError):
                # Skip invalid port files
                continue

    return instances


class PBIDesktopConnection:
    """
    Connection to a local Power BI Desktop instance.

    Provides DAX execution and model metadata queries via XMLA.
    """

    def __init__(self, port: int):
        """
        Initialize connection to PBI Desktop on specified port.

        Args:
            port: Local port number (from find_pbi_instances)
        """
        self.port = port
        self._conn_str = f"Provider=MSOLAP;Data Source=localhost:{port}"
        self._connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Open connection to PBI Desktop."""
        pyadomd = _get_pyadomd()
        self._connection = pyadomd.Pyadomd(self._conn_str)
        self._connection.open()

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute_dax(self, dax: str) -> list[dict]:
        """
        Execute a DAX query and return results.

        Args:
            dax: DAX query (should start with EVALUATE for table results)

        Returns:
            List of dicts, one per row.

        Raises:
            Exception: If DAX is invalid or execution fails.
        """
        if not self._connection:
            raise RuntimeError("Not connected. Call connect() first or use context manager.")

        with self._connection.cursor() as cursor:
            cursor.execute(dax)
            columns = [col[0] for col in cursor.description]
            # Use fetchall() instead of iterating cursor directly
            rows = []
            for row in cursor.fetchall():
                rows.append(dict(zip(columns, row)))
            return rows

    def validate_dax(self, dax: str) -> tuple[bool, Optional[str]]:
        """
        Check if DAX query is valid by attempting execution.

        Args:
            dax: DAX query to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Wrap in EVALUATE if not present (for measure expressions)
            if not dax.strip().upper().startswith("EVALUATE"):
                dax = f"EVALUATE ROW('Result', {dax})"

            self.execute_dax(dax)
            return True, None
        except Exception as e:
            return False, str(e)

    def get_tables(self) -> list[dict]:
        """
        Get all tables in the model.

        Returns:
            List of dicts with table metadata.
        """
        dax = """
        SELECT
            [DIMENSION_UNIQUE_NAME] AS [Table],
            [DIMENSION_CARDINALITY] AS [RowCount]
        FROM $SYSTEM.MDSCHEMA_DIMENSIONS
        WHERE [DIMENSION_TYPE] = 3
        """
        return self._execute_dmv(dax)

    def get_columns(self, table_name: Optional[str] = None) -> list[dict]:
        """
        Get columns, optionally filtered by table.

        Args:
            table_name: Optional table name filter

        Returns:
            List of dicts with column metadata.
        """
        dax = """
        SELECT
            [DIMENSION_UNIQUE_NAME] AS [Table],
            [HIERARCHY_UNIQUE_NAME] AS [Column],
            [HIERARCHY_CARDINALITY] AS [Cardinality]
        FROM $SYSTEM.MDSCHEMA_HIERARCHIES
        WHERE [HIERARCHY_ORIGIN] = 2
        """

        results = self._execute_dmv(dax)

        if table_name:
            results = [r for r in results if r.get("Table") == f"[{table_name}]"]

        return results

    def get_measures(self) -> list[dict]:
        """
        Get all measures in the model.

        Returns:
            List of dicts with measure name, expression, table.
        """
        dax = """
        SELECT
            [MEASUREGROUP_NAME] AS [Table],
            [MEASURE_NAME] AS [Measure],
            [EXPRESSION] AS [Expression]
        FROM $SYSTEM.MDSCHEMA_MEASURES
        WHERE [MEASURE_IS_VISIBLE]
        """
        return self._execute_dmv(dax)

    def get_relationships(self) -> list[dict]:
        """
        Get model relationships.

        Returns:
            List of dicts with relationship metadata.
        """
        # TMSCHEMA_RELATIONSHIPS requires TOM, use DAX INFO functions instead
        dax = """
        EVALUATE
        SELECTCOLUMNS(
            INFO.RELATIONSHIPS(),
            "FromTable", [FromTableID],
            "FromColumn", [FromColumnID],
            "ToTable", [ToTableID],
            "ToColumn", [ToColumnID],
            "IsActive", [IsActive],
            "CrossFilterDirection", [CrossFilteringBehavior]
        )
        """
        try:
            return self.execute_dax(dax)
        except Exception:
            # INFO functions might not be available in older versions
            return []

    def get_model_summary(self) -> dict:
        """
        Get a summary of the model for context injection.

        Returns:
            Dict with tables, measures, relationships counts and names.
        """
        tables = self.get_tables()
        measures = self.get_measures()

        return {
            "table_count": len(tables),
            "tables": [t.get("Table", "").strip("[]") for t in tables],
            "measure_count": len(measures),
            "measures": [m.get("Measure", "") for m in measures],
        }

    def _execute_dmv(self, query: str) -> list[dict]:
        """Execute a DMV query against $SYSTEM tables."""
        if not self._connection:
            raise RuntimeError("Not connected. Call connect() first or use context manager.")

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = []
            for row in cursor.fetchall():
                rows.append(dict(zip(columns, row)))
            return rows


# Convenience function for quick validation
def validate_dax_against_desktop(dax: str) -> tuple[bool, Optional[str], Optional[int]]:
    """
    Quick validation of DAX against first available PBI Desktop instance.

    Args:
        dax: DAX expression or query to validate

    Returns:
        Tuple of (is_valid, error_message, port_used)
    """
    instances = find_pbi_instances()

    if not instances:
        return False, "No Power BI Desktop instances found. Open a PBIX file first.", None

    instance = instances[0]

    with PBIDesktopConnection(instance.port) as conn:
        valid, error = conn.validate_dax(dax)
        return valid, error, instance.port
