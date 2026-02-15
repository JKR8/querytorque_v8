/**
 * API client for qt-sql backend
 *
 * Typed to match real backend schemas in api/main.py.
 */
import axios, { AxiosInstance, AxiosError } from 'axios'
import { config } from '@/config'

// Create axios instance with defaults
const api: AxiosInstance = axios.create({
  baseURL: config.api.baseUrl,
  timeout: 120_000, // optimizations can take 60s+
  headers: {
    'Content-Type': 'application/json',
  },
})

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response) {
      const data = error.response.data as { detail?: string }
      throw new Error(data.detail || `HTTP ${error.response.status}`)
    }
    throw new Error(error.message || 'Network error')
  }
)

// ============================================
// Types — Health
// ============================================

export interface HealthResponse {
  status: string
  version: string
  llm_configured: boolean
  llm_provider: string | null
}

// ============================================
// Types — Database Connection
// ============================================

export interface DatabaseConnectionResponse {
  session_id: string
  connected: boolean
  type: string
  details?: string
  error?: string
}

export interface DatabaseConnectionStatus {
  connected: boolean
  type?: string
  details?: string
}

export interface SchemaColumn {
  name: string
  type: string
  nullable?: boolean
}

export interface SchemaResponse {
  session_id: string
  tables: Record<string, SchemaColumn[]>
  error?: string
}

// ============================================
// Types — Query Execution
// ============================================

export interface QueryResult {
  columns: string[]
  column_types: string[]
  rows: unknown[][]
  row_count: number
  execution_time_ms: number
  truncated?: boolean
  error?: string
}

// ============================================
// Types — Execution Plan
// ============================================

export interface PlanTreeNode {
  indent: number
  operator: string
  details?: string
  meta?: string
  cost_pct: number
  rows: number
  estimated_rows?: number
  timing_ms?: number
  is_bottleneck?: boolean
  problem?: boolean
  spill?: boolean
  pruning_ratio?: number | null
}

export interface ExecutionPlanResponse {
  success: boolean
  plan_text?: string
  plan_json?: object
  plan_tree?: PlanTreeNode[]
  execution_time_ms?: number
  total_cost?: number
  bottleneck?: {
    operator: string
    cost_pct: number
    rows: number
    details?: string
    suggestion?: string
    title?: string
    detail?: string
  }
  warnings?: string[]
  error?: string
}

// ============================================
// Types — Audit
// ============================================

export interface AuditResponse {
  success: boolean
  plan_tree?: PlanTreeNode[]
  bottleneck?: {
    operator: string
    cost_pct: number
    rows: number
    details?: string
    suggestion?: string
  }
  pathology_name?: string
  execution_time_ms?: number
  total_cost?: number
  warnings: string[]
  error?: string
}

// ============================================
// Types — Optimization
// ============================================

export interface OptimizeRequest {
  sql: string
  dsn: string
  mode?: 'beam'
  query_id?: string
  session_id?: string
  max_iterations?: number
  target_speedup?: number
}

export interface WorkerResult {
  worker_id: number
  strategy: string
  examples_used: string[]
  optimized_sql: string
  speedup: number
  status: string
  transforms: string[]
  error_message?: string
}

export interface OptimizeResponse {
  status: 'WIN' | 'IMPROVED' | 'NEUTRAL' | 'REGRESSION' | 'ERROR'
  speedup: number
  speedup_type: string
  validation_confidence: string
  optimized_sql?: string
  original_sql: string
  transforms: string[]
  workers: WorkerResult[]
  query_id: string
  error?: string
  n_iterations: number
  n_api_calls: number
}

// ============================================
// Types — SQL Validation
// ============================================

export interface ValidateSQLRequest {
  original_sql: string
  optimized_sql: string
  mode?: 'sample' | 'full'
  schema_sql?: string
  session_id?: string
}

export interface ValidateSQLResponse {
  status: 'pass' | 'fail' | 'warn' | 'error'
  mode: string
  row_counts: { original: number; optimized: number }
  row_counts_match: boolean
  timing: { original_ms: number; optimized_ms: number }
  speedup: number
  cost: { original: number; optimized: number }
  cost_reduction_pct: number
  values_match: boolean
  checksum_match?: boolean
  errors: string[]
  warnings: string[]
}

// ============================================
// API Functions — Health
// ============================================

export async function getHealth(): Promise<HealthResponse> {
  const response = await api.get('/health')
  return response.data
}

// ============================================
// API Functions — Database Connection
// ============================================

export async function connectDuckDB(fixtureFile: File): Promise<DatabaseConnectionResponse> {
  const formData = new FormData()
  formData.append('fixture_file', fixtureFile)

  const response = await api.post('/database/connect/duckdb', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return response.data
}

export async function connectDuckDBQuick(fixturePath: string): Promise<DatabaseConnectionResponse> {
  const formData = new FormData()
  formData.append('fixture_path', fixturePath)

  const response = await api.post('/database/connect/duckdb/quick', formData)
  return response.data
}

export async function connectPostgres(connectionString: string): Promise<DatabaseConnectionResponse> {
  const response = await api.post('/database/connect/postgres', {
    connection_string: connectionString,
  })
  return response.data
}

export async function getDatabaseStatus(sessionId: string): Promise<DatabaseConnectionStatus> {
  const response = await api.get(`/database/status/${sessionId}`)
  return response.data
}

export async function disconnectDatabase(sessionId: string): Promise<void> {
  await api.delete(`/database/disconnect/${sessionId}`)
}

// ============================================
// API Functions — Query Execution
// ============================================

export async function executeQuery(sessionId: string, sql: string, limit: number = 100): Promise<QueryResult> {
  const response = await api.post(`/database/execute/${sessionId}`, { sql, limit })
  return response.data
}

export async function getExecutionPlan(sessionId: string, sql: string, analyze: boolean = true): Promise<ExecutionPlanResponse> {
  const response = await api.post(`/database/explain/${sessionId}`, { sql, analyze })
  return response.data
}

export async function getDatabaseSchema(sessionId: string): Promise<SchemaResponse> {
  const response = await api.get(`/database/schema/${sessionId}`)
  return response.data
}

// ============================================
// API Functions — Audit (free tier, no LLM)
// ============================================

export async function auditQuery(sessionId: string, sql: string): Promise<AuditResponse> {
  const response = await api.post(`/database/audit/${sessionId}`, { sql, analyze: true })
  return response.data
}

// ============================================
// API Functions — Optimization (LLM-backed)
// ============================================

export async function optimizeQuery(request: OptimizeRequest): Promise<OptimizeResponse> {
  const response = await api.post('/sql/optimize', request, {
    timeout: 120_000, // optimization can take 60s+
  })
  return response.data
}

// ============================================
// API Functions — SQL Validation
// ============================================

export async function validateSql(request: ValidateSQLRequest): Promise<ValidateSQLResponse> {
  const response = await api.post('/sql/validate', request)
  return response.data
}

export default api
