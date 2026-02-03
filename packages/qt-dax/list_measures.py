"""List measures in open PBI model."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

adomd_path = r"C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.AnalysisServices.AdomdClient\v4.0_15.0.0.0__89845dcd8080cc91"
if os.path.exists(adomd_path):
    os.environ["PATH"] = adomd_path + os.pathsep + os.environ.get("PATH", "")
    sys.path.insert(0, adomd_path)
    import clr
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")

from qt_dax.connections import PBIDesktopConnection, find_pbi_instances

inst = find_pbi_instances()[0]
print(f"Port: {inst.port}")

with PBIDesktopConnection(inst.port) as conn:
    measures = conn.get_measures()
    print(f"Total measures: {len(measures)}\n")

    # Show first 20 measures
    print("Sample measures:")
    for m in measures[:20]:
        print(f"  - {m.get('Measure')}")
