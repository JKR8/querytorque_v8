/**
 * API client for qt-sql backend
 */
import axios, { AxiosInstance, AxiosError } from 'axios'
import { config } from '@/config'

// Create axios instance with defaults
const api: AxiosInstance = axios.create({
  baseURL: config.api.baseUrl,
  timeout: 60000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor to add auth token
api.interceptors.request.use(
  async (config) => {
    // Token will be added by AuthContext if available
    return config
  },
  (error) => Promise.reject(error)
)

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
// Types
// ============================================

export interface HealthResponse {
  status: string
  mode: 'auto' | 'manual'
  auth_enabled: boolean
  llm_configured: boolean
  llm_provider: string | null
}

export interface AnalysisResult {
  html: string
  score: number
  status: 'pass' | 'warn' | 'fail' | 'deny'
  summary: string
  file_name: string
  original_sql: string
  issues_count: number
}

export interface ValidationResult {
  success: boolean
  syntax_valid: boolean
  syntax_errors: string[]
  schema_valid: boolean
  schema_violations: string[]
  regression_passed: boolean
  issues_fixed: string[]
  new_issues: string[]
  equivalence_status: 'pass' | 'fail' | 'skip'
  equivalence_details?: {
    original_row_count: number
    optimized_row_count: number
    speedup_ratio: number
  }
  optimized_code: string
  diff_html: string
}

export interface OptimizationSession {
  session_id: string
  state: 'pending' | 'optimizing' | 'awaiting_input' | 'validating' | 'complete' | 'failed'
  mode: 'auto' | 'manual'
  original_code: string
  optimized_code?: string
  validation?: ValidationResult
  prompt_markdown?: string
  errors: string[]
  warnings: string[]
  can_retry: boolean
  retry_count: number
  max_retries: number
}

export interface Report {
  id: string
  file_name: string
  score: number
  status: string
  created_at: string
  issues_count: number
}

// ============================================
// Health
// ============================================

export async function getHealth(): Promise<HealthResponse> {
  // Health endpoint is at root, not under /api
  // Use api instance to get the correct baseURL
  const response = await api.get('/health')
  return response.data
}

// ============================================
// Analysis
// ============================================

export async function analyzeSql(
  code: string,
  _fileName: string = 'query.sql'
): Promise<AnalysisResult> {
  // Send SQL for analysis - endpoint at /api/sql/analyze
  const response = await api.post('/sql/analyze', {
    sql: code,  // Backend expects 'sql' not 'code'
  })
  return response.data
}

export async function analyzeSqlFile(file: File): Promise<AnalysisResult> {
  const formData = new FormData()
  formData.append('file', file)

  const response = await api.post('/sql/analyze/file', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return response.data
}

// ============================================
// Optimization
// ============================================

export async function startOptimization(
  code: string,
  mode: 'auto' | 'manual' = 'auto',
  fileName: string = 'query.sql'
): Promise<OptimizationSession> {
  const response = await api.post('/v2/optimize/start', {
    code,
    query_type: 'sql',
    mode,
    file_name: fileName,
  })
  return response.data
}

export async function submitOptimizationResponse(
  sessionId: string,
  rawResponse: string
): Promise<OptimizationSession> {
  const response = await api.post(`/v2/optimize/submit/${sessionId}`, {
    raw_response: rawResponse,
    response_format: 'json',
  })
  return response.data
}

export async function acceptOptimization(
  sessionId: string
): Promise<OptimizationSession> {
  const response = await api.post(`/v2/optimize/accept/${sessionId}`)
  return response.data
}

export async function retryOptimization(
  sessionId: string,
  feedback?: string
): Promise<OptimizationSession> {
  const response = await api.post(`/v2/optimize/retry/${sessionId}`, {
    feedback,
  })
  return response.data
}

export async function getOptimizationSession(
  sessionId: string
): Promise<OptimizationSession> {
  const response = await api.get(`/v2/optimize/session/${sessionId}`)
  return response.data
}

// ============================================
// Reports
// ============================================

export async function listReports(
  limit: number = 50,
  offset: number = 0
): Promise<{ reports: Report[]; total: number }> {
  const response = await api.get('/reports', {
    params: { limit, offset },
  })
  return response.data
}

export async function getReport(reportId: string): Promise<AnalysisResult> {
  const response = await api.get(`/reports/${reportId}`)
  return response.data
}

export async function deleteReport(reportId: string): Promise<void> {
  await api.delete(`/reports/${reportId}`)
}

// ============================================
// Validation Types
// ============================================

export type ValidationStatus = 'pass' | 'fail' | 'skip' | 'warn' | 'error'

export interface IssueDetail {
  rule_id?: string
  rule?: string
  severity?: string
  title?: string
  description?: string
  line?: number
}

export interface EquivalenceDetails {
  status: 'pass' | 'fail' | 'warn' | 'skip' | 'error'
  equivalent: boolean
  row_count_match: boolean
  original_row_count: number
  optimized_row_count: number
  original_execution_time_ms: number
  optimized_execution_time_ms: number
  speedup_ratio: number
  original_checksum?: string
  optimized_checksum?: string
  sample_mismatches: object[]
  errors: string[]
  warnings: string[]
}

export interface SandboxTable {
  name: string
  rows: number
}

export interface SandboxPlan {
  original_plan?: string
  optimized_plan?: string
}

export interface ExecutionPlanComparison {
  original_execution_time_ms: number
  optimized_execution_time_ms: number
  time_improvement_pct?: number
  cost_improvement_pct?: number
  original_bottleneck?: string
  optimized_bottleneck?: string
  original_plan_summary?: {
    plan_tree?: object[]
    warnings?: string[]
  }
  optimized_plan_summary?: {
    plan_tree?: object[]
    warnings?: string[]
  }
  original_total_cost?: number
  optimized_total_cost?: number
}

export interface PatchResult {
  issue_id: string
  status: 'applied' | 'skipped' | 'failed'
  description?: string
  line_matched?: number
  error?: string
}

export interface PatchResultSummary {
  patch_results: PatchResult[]
  applied_count: number
  total_patches: number
  success_rate?: number
}

export interface ValidationPreviewResponse {
  session_id: string
  success: boolean
  optimization_mode?: 'auto' | 'manual'
  syntax_status: ValidationStatus
  syntax_errors: string[]
  schema_status: ValidationStatus
  schema_violations: string[]
  regression_status: ValidationStatus
  issues_fixed: IssueDetail[]
  new_issues: IssueDetail[]
  equivalence_status: ValidationStatus
  equivalence_details?: EquivalenceDetails
  sandbox_tables?: SandboxTable[]
  sandbox_plans?: SandboxPlan
  plan_comparison?: ExecutionPlanComparison
  original_code: string
  optimized_code: string
  diff_html: string
  llm_confidence: number
  llm_explanation: string
  all_passed: boolean
  errors: string[]
  warnings: string[]
  can_retry?: boolean
  retry_count?: number
  max_retries?: number
  patch_mode?: boolean
  patch_result?: PatchResultSummary
  chunking_status?: 'pass' | 'fail' | 'warn' | 'skip'
  chunking_violations?: string[]
  skipped_layers?: string[]
  early_failure?: boolean
}

// ============================================
// Database Connection
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

export interface SchemaResponse {
  session_id: string
  tables: Record<string, { name: string; type: string; nullable?: boolean }[]>
  error?: string
}

export interface QueryResult {
  columns: string[]
  column_types: string[]
  rows: unknown[][]
  row_count: number
  execution_time_ms: number
  truncated?: boolean
  error?: string
}

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

export async function connectDuckDB(fixtureFile: File): Promise<DatabaseConnectionResponse> {
  const formData = new FormData()
  formData.append('fixture_file', fixtureFile)

  const response = await api.post('/database/connect/duckdb', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return response.data
}

export async function connectDuckDBQuick(fixturePath: string): Promise<DatabaseConnectionResponse> {
  const response = await api.post('/database/connect/duckdb/quick', {
    fixture_path: fixturePath,
  })
  return response.data
}

export async function connectPostgres(connectionString: string): Promise<DatabaseConnectionResponse> {
  const formData = new FormData()
  formData.append('connection_string', connectionString)

  const response = await api.post('/database/connect/postgres', formData)
  return response.data
}

export async function getDatabaseStatus(sessionId: string): Promise<DatabaseConnectionStatus> {
  const response = await api.get(`/database/status/${sessionId}`)
  return response.data
}

export async function disconnectDatabase(sessionId: string): Promise<void> {
  await api.delete(`/database/disconnect/${sessionId}`)
}

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
// Manual Validation
// ============================================

export async function validateManualResponse(
  originalSql: string,
  llmResponse: string,
  dialect: string = 'generic'
): Promise<ValidationPreviewResponse> {
  const response = await api.post('/optimize/manual/validate', {
    original_sql: originalSql,
    llm_response: llmResponse,
    dialect,
  })
  return response.data
}

// ============================================
// Utilities
// ============================================

/**
 * Set the auth token for API requests
 */
export function setAuthToken(token: string | null): void {
  if (token) {
    api.defaults.headers.common['Authorization'] = `Bearer ${token}`
  } else {
    delete api.defaults.headers.common['Authorization']
  }
}

export default api
