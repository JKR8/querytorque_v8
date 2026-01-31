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
  calcite_available: boolean
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
  const response = await api.get('/health')
  return response.data
}

// ============================================
// Analysis
// ============================================

export async function analyzeSql(
  code: string,
  fileName: string = 'query.sql'
): Promise<AnalysisResult> {
  const response = await api.post('/sql/analyze', {
    code,
    file_name: fileName,
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
