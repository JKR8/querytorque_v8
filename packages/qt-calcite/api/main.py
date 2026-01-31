#!/usr/bin/env python3
"""QTCalcite REST API - LLM-powered SQL Query Optimizer.

This API wraps the Java Calcite optimizer with authentication from qt-shared.
"""
import asyncio
import re
import os
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import uvicorn

# Import auth from qt-shared (optional - works without it)
try:
    from qt_shared.auth import OptionalUser, UserContext
    from qt_shared.config import get_settings
    AUTH_AVAILABLE = True
except ImportError:
    AUTH_AVAILABLE = False
    OptionalUser = None
    UserContext = None

# Config
JAR_PATH = Path(__file__).parent.parent / "build/libs/qt-calcite-1.0.0-all.jar"
# Also check for container path
CONTAINER_JAR_PATH = Path("/app/qt-calcite.jar")
DEFAULT_CONNECTION = os.getenv("QTCALCITE_DB", ":memory:")


def get_jar_path() -> Path:
    """Get the actual JAR path."""
    if JAR_PATH.exists():
        return JAR_PATH
    if CONTAINER_JAR_PATH.exists():
        return CONTAINER_JAR_PATH
    return JAR_PATH


# Request/Response models
class OptimizeRequest(BaseModel):
    sql: str
    connection_string: str = ":memory:"
    deepseek_api_key: Optional[str] = None
    mode: str = "hep"  # "hep" or "volcano"
    dry_run: bool = False
    compare: bool = True
    timeout_seconds: int = 300


class OptimizeResponse(BaseModel):
    success: bool
    original_sql: str
    optimized_sql: Optional[str] = None
    query_changed: bool = False
    rules_applied: list[str] = []
    original_time_ms: Optional[int] = None
    optimized_time_ms: Optional[int] = None
    improvement_percent: Optional[float] = None
    validation_passed: Optional[bool] = None
    row_count: Optional[int] = None
    original_cost: Optional[float] = None
    optimized_cost: Optional[float] = None
    llm_reasoning: Optional[str] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    jar_found: bool
    default_connection: str
    auth_enabled: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    jar_path = get_jar_path()
    if not jar_path.exists():
        print(f"WARNING: JAR not found at {jar_path}")
        print("Run: ./gradlew fatJar")
    yield


app = FastAPI(
    title="QTCalcite API",
    description="LLM-R2 SQL Query Optimizer using Apache Calcite and DuckDB",
    version="1.0.0",
    lifespan=lifespan
)


def parse_output(output: str) -> dict:
    """Parse JAR output into structured data."""
    result = {
        "rules_applied": [],
        "original_sql": None,
        "optimized_sql": None,
        "query_changed": False,
        "original_time_ms": None,
        "optimized_time_ms": None,
        "improvement_percent": None,
        "validation_passed": None,
        "row_count": None,
        "original_cost": None,
        "optimized_cost": None,
        "llm_reasoning": None,
    }

    # Extract rules
    rules_match = re.search(r"Selected rules: (.+)", output)
    if rules_match:
        result["rules_applied"] = [r.strip() for r in rules_match.group(1).split(",")]

    # Extract query changed
    if "Query changed: Yes" in output:
        result["query_changed"] = True

    # Extract original SQL
    orig_match = re.search(r"Original SQL:\s*\n?\s*(.+?)(?=\n\s*\n|Optimized SQL:)", output, re.DOTALL)
    if orig_match:
        result["original_sql"] = orig_match.group(1).strip()

    # Extract optimized SQL
    opt_match = re.search(r"Optimized SQL:\s*\n?\s*(.+?)(?=\n\s*-{20,}|\n\s*\n\s*\()", output, re.DOTALL)
    if opt_match:
        result["optimized_sql"] = opt_match.group(1).strip()

    # Extract timing
    orig_time = re.search(r"Original time:\s*(\d+)\s*ms", output)
    if orig_time:
        result["original_time_ms"] = int(orig_time.group(1))

    opt_time = re.search(r"Optimized time:\s*(\d+)\s*ms", output)
    if opt_time:
        result["optimized_time_ms"] = int(opt_time.group(1))

    improvement = re.search(r"Improvement:\s*([-\d.]+)%", output)
    if improvement:
        result["improvement_percent"] = float(improvement.group(1))

    # Extract validation
    if "VALIDATION: PASSED" in output:
        result["validation_passed"] = True
    elif "VALIDATION: FAILED" in output:
        result["validation_passed"] = False

    # Extract row count
    row_match = re.search(r"Original rows:\s*(\d+)", output)
    if row_match:
        result["row_count"] = int(row_match.group(1))

    # Extract Volcano costs
    orig_cost = re.search(r"Original cost:\s*([\d.]+)", output)
    if orig_cost:
        result["original_cost"] = float(orig_cost.group(1))

    opt_cost = re.search(r"Optimized cost:\s*([\d.]+)", output)
    if opt_cost:
        result["optimized_cost"] = float(opt_cost.group(1))

    # Extract LLM reasoning
    reasoning_match = re.search(
        r"(?:Reasoning|LLM Analysis):\s*(.+?)(?=\n\s*-{20,}|\n\s*Selected rules:|\Z)",
        output, re.DOTALL
    )
    if reasoning_match:
        result["llm_reasoning"] = reasoning_match.group(1).strip()

    return result


async def run_optimizer(
    sql: str,
    connection_string: str,
    mode: str = "hep",
    dry_run: bool = False,
    compare: bool = True,
    timeout_seconds: int = 300,
    deepseek_api_key: Optional[str] = None
) -> tuple[bool, str, dict]:
    """Run the Java optimizer asynchronously and return parsed results."""
    jar_path = get_jar_path()
    if not jar_path.exists():
        return False, f"JAR not found: {jar_path}", {}

    args = ["java", "-jar", str(jar_path), "-d", connection_string]

    if mode == "volcano":
        args.append("--volcano")

    args.append("auto")

    if dry_run:
        args.append("--dry-run")
    elif compare:
        args.append("--compare")

    args.append(sql)

    env = os.environ.copy()
    if deepseek_api_key:
        env["DEEPSEEK_API_KEY"] = deepseek_api_key

    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout_seconds
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return False, f"Query timeout ({timeout_seconds}s)", {}

        output = stdout.decode() + stderr.decode()

        if process.returncode != 0:
            if "API Error" in output or "Network Error" in output:
                return False, "DeepSeek API error", {"raw_output": output}
            if "Database error" in output:
                return False, "Database error", {"raw_output": output}
            return False, f"Optimization failed (exit {process.returncode})", {"raw_output": output}

        parsed = parse_output(output)
        parsed["raw_output"] = output
        return True, "OK", parsed

    except Exception as e:
        return False, str(e), {}


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    jar_path = get_jar_path()
    auth_enabled = False
    if AUTH_AVAILABLE:
        try:
            settings = get_settings()
            auth_enabled = settings.auth_enabled
        except Exception:
            pass

    return HealthResponse(
        status="ok" if jar_path.exists() else "degraded",
        jar_found=jar_path.exists(),
        default_connection=DEFAULT_CONNECTION,
        auth_enabled=auth_enabled
    )


@app.post("/optimize", response_model=OptimizeResponse)
async def optimize_query(request: OptimizeRequest):
    """Optimize a SQL query using LLM-selected Calcite rules.

    - Sends query to DeepSeek API for rule selection
    - Applies selected rules via Apache Calcite
    - Optionally compares original vs optimized performance

    Connection string accepts:
    - Local file path: /path/to/database.duckdb
    - In-memory: :memory:
    - MotherDuck: md:database_name?motherduck_token=YOUR_TOKEN

    Mode options:
    - hep: Heuristic optimizer (default, faster)
    - volcano: Cost-based Volcano optimizer (more thorough)
    """
    if request.mode not in ("hep", "volcano"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid mode: {request.mode}. Must be 'hep' or 'volcano'"
        )

    success, message, data = await run_optimizer(
        sql=request.sql,
        connection_string=request.connection_string,
        mode=request.mode,
        dry_run=request.dry_run,
        compare=request.compare,
        timeout_seconds=request.timeout_seconds,
        deepseek_api_key=request.deepseek_api_key
    )

    if not success:
        return OptimizeResponse(
            success=False,
            original_sql=request.sql,
            error=message
        )

    return OptimizeResponse(
        success=True,
        original_sql=request.sql,
        optimized_sql=data.get("optimized_sql"),
        query_changed=data.get("query_changed", False),
        rules_applied=data.get("rules_applied", []),
        original_time_ms=data.get("original_time_ms"),
        optimized_time_ms=data.get("optimized_time_ms"),
        improvement_percent=data.get("improvement_percent"),
        validation_passed=data.get("validation_passed"),
        row_count=data.get("row_count"),
        original_cost=data.get("original_cost"),
        optimized_cost=data.get("optimized_cost"),
        llm_reasoning=data.get("llm_reasoning")
    )


@app.get("/rules")
async def list_rules():
    """List available Calcite optimization rules."""
    rules = {
        "filter": [
            "FILTER_INTO_JOIN", "FILTER_PROJECT_TRANSPOSE", "FILTER_MERGE",
            "FILTER_SCAN", "FILTER_AGGREGATE_TRANSPOSE", "FILTER_CORRELATE",
            "FILTER_SET_OP_TRANSPOSE", "FILTER_REDUCE_EXPRESSIONS"
        ],
        "project": [
            "PROJECT_MERGE", "PROJECT_REMOVE", "PROJECT_JOIN_TRANSPOSE",
            "PROJECT_FILTER_TRANSPOSE", "PROJECT_AGGREGATE_MERGE",
            "PROJECT_TABLE_SCAN", "PROJECT_REDUCE_EXPRESSIONS"
        ],
        "join": [
            "JOIN_COMMUTE", "JOIN_ASSOCIATE", "JOIN_CONDITION_PUSH",
            "JOIN_PUSH_TRANSITIVE_PREDICATES", "JOIN_EXTRACT_FILTER",
            "JOIN_PROJECT_BOTH_TRANSPOSE", "JOIN_TO_MULTI_JOIN"
        ],
        "aggregate": [
            "AGGREGATE_PROJECT_MERGE", "AGGREGATE_JOIN_TRANSPOSE",
            "AGGREGATE_REMOVE", "AGGREGATE_REDUCE_FUNCTIONS",
            "AGGREGATE_EXPAND_DISTINCT_AGGREGATES"
        ],
        "sort": [
            "SORT_PROJECT_TRANSPOSE", "SORT_REMOVE", "SORT_REMOVE_CONSTANT_KEYS",
            "SORT_JOIN_TRANSPOSE", "LIMIT_MERGE"
        ],
        "set_ops": [
            "UNION_MERGE", "UNION_REMOVE", "INTERSECT_MERGE", "MINUS_MERGE"
        ]
    }
    return {"rules": rules, "total": sum(len(v) for v in rules.values())}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    print(f"Starting QTCalcite API on {args.host}:{args.port}")
    print(f"JAR: {get_jar_path()}")
    print(f"Default connection: {DEFAULT_CONNECTION}")
    print(f"Docs: http://{args.host}:{args.port}/docs")

    uvicorn.run(app, host=args.host, port=args.port)
