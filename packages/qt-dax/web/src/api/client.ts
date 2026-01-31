/**
 * API client for Query Torque DAX backend
 */

import axios, { AxiosInstance, AxiosError } from 'axios'
import { config } from '@/config'

// Create axios instance with defaults
const apiClient: AxiosInstance = axios.create({
  baseURL: config.apiBaseUrl,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Error handler
function handleApiError(error: AxiosError): never {
  if (error.response?.data && typeof error.response.data === 'object') {
    const data = error.response.data as { detail?: string }
    throw new Error(data.detail || 'API request failed')
  }
  throw new Error(error.message || 'Network error')
}

// ============================================
// Types
// ============================================

export interface HealthResponse {
  status: string
  mode: 'auto' | 'manual'
  auth_enabled: boolean
  version: string
}

export interface AnalysisResult {
  html: string
  score: number
  status: 'pass' | 'warn' | 'fail' | 'deny'
  summary: string
  file_name: string
  model_summary?: ModelSummary
}

export interface ModelSummary {
  name: string
  tables_count: number
  measures_count: number
  columns_count: number
  relationships_count: number
  total_size_bytes: number
  data_size_bytes: number
  dictionary_size_bytes: number
}

export interface Table {
  name: string
  row_count: number
  column_count: number
  size_bytes: number
  is_calculated: boolean
  is_hidden: boolean
}

export interface Measure {
  name: string
  table_name: string
  expression: string
  format_string?: string
  is_hidden: boolean
  dependencies: string[]
}

export interface Relationship {
  from_table: string
  from_column: string
  to_table: string
  to_column: string
  cardinality: string
  cross_filter_behavior: string
  is_active: boolean
}

export interface ModelBrowserData {
  tables: Table[]
  measures: Measure[]
  relationships: Relationship[]
  model_summary: ModelSummary
}

export interface OptimizationPayload {
  session_id: string
  prompt_markdown: string
  prompt_json: object
  issues_summary: Issue[]
  original_code: string
  estimated_tokens?: number
}

export interface Issue {
  rule_id: string
  severity: 'critical' | 'high' | 'medium' | 'low' | 'info'
  title: string
  description?: string
  line?: number
  measure_name?: string
}

export interface ValidationResult {
  session_id: string
  success: boolean
  syntax_status: 'pass' | 'fail' | 'skip'
  syntax_errors: string[]
  regression_status: 'pass' | 'fail' | 'skip'
  issues_fixed: Issue[]
  new_issues: Issue[]
  original_code: string
  optimized_code: string
  diff_html: string
  llm_confidence: number
  llm_explanation: string
  all_passed: boolean
  can_retry: boolean
  retry_count: number
  max_retries: number
}

export interface Report {
  id: string
  name: string
  created_at: string
  score: number
  status: string
  model_name?: string
}

// ============================================
// Health Check
// ============================================

export async function getHealth(): Promise<HealthResponse> {
  try {
    const response = await apiClient.get('/health')
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// VPAX Analysis
// ============================================

export async function analyzeVpax(file: File): Promise<AnalysisResult> {
  try {
    const formData = new FormData()
    formData.append('file', file)

    const response = await apiClient.post('/dax/analyze', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function analyzeVpaxStatic(file: File): Promise<AnalysisResult> {
  try {
    const formData = new FormData()
    formData.append('file', file)

    const response = await apiClient.post('/dax/analyze-model-static', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// Model Browser
// ============================================

export async function getModelBrowserData(file: File): Promise<ModelBrowserData> {
  try {
    const formData = new FormData()
    formData.append('file', file)

    const response = await apiClient.post('/dax/model-browser', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// DAX Text Analysis
// ============================================

export async function analyzeDaxText(code: string, name?: string): Promise<AnalysisResult> {
  try {
    const response = await apiClient.post('/dax/analyze-text', {
      code,
      name: name || 'DAX Query',
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// Optimization Flow
// ============================================

export async function startOptimization(
  code: string,
  measureName?: string,
  mode: 'auto' | 'manual' = 'manual'
): Promise<OptimizationPayload> {
  try {
    const response = await apiClient.post('/v2/optimize/start', {
      code,
      query_type: 'dax',
      mode,
      file_name: measureName || 'measure.dax',
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function submitOptimizationResponse(
  sessionId: string,
  rawResponse: string,
  responseFormat: 'json' | 'markdown' | 'code_only' = 'json'
): Promise<ValidationResult> {
  try {
    const response = await apiClient.post(`/v2/optimize/submit/${sessionId}`, {
      raw_response: rawResponse,
      response_format: responseFormat,
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function acceptOptimization(sessionId: string): Promise<{ success: boolean; optimized_code: string }> {
  try {
    const response = await apiClient.post(`/v2/optimize/accept/${sessionId}`)
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function retryOptimization(sessionId: string, feedback?: string): Promise<OptimizationPayload> {
  try {
    const response = await apiClient.post(`/v2/optimize/retry/${sessionId}`, {
      feedback,
    })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function cancelOptimization(sessionId: string): Promise<void> {
  try {
    await apiClient.delete(`/v2/optimize/session/${sessionId}`)
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// Reports
// ============================================

export async function getReports(): Promise<Report[]> {
  try {
    const response = await apiClient.get('/dax/reports')
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function getReport(reportId: string): Promise<AnalysisResult> {
  try {
    const response = await apiClient.get(`/dax/reports/${reportId}`)
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function deleteReport(reportId: string): Promise<void> {
  try {
    await apiClient.delete(`/dax/reports/${reportId}`)
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// Power BI Desktop Connection (Live Model)
// ============================================

export interface PBIInstance {
  port: number
  name: string
  workspace_path: string
}

export interface PBISession {
  session_id: string
  port: number
  name: string
  status: string
  model_summary?: ModelSummary
}

export async function listPBIInstances(): Promise<{ instances: PBIInstance[]; available: boolean }> {
  try {
    const response = await apiClient.get('/pbi/instances')
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function connectPBI(port: number): Promise<PBISession> {
  try {
    const response = await apiClient.post('/pbi/connect', { port })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function disconnectPBI(sessionId: string): Promise<void> {
  try {
    await apiClient.delete(`/pbi/session/${sessionId}`)
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

export async function executeDAX(sessionId: string, dax: string): Promise<{
  success: boolean
  columns: string[]
  rows: unknown[][]
  row_count: number
  execution_time_ms: number
  error?: string
}> {
  try {
    const response = await apiClient.post(`/pbi/session/${sessionId}/execute`, { dax })
    return response.data
  } catch (error) {
    return handleApiError(error as AxiosError)
  }
}

// ============================================
// Auth Token Interceptor
// ============================================

let getAuthToken: (() => Promise<string | null>) | null = null

export function setAuthTokenProvider(provider: () => Promise<string | null>) {
  getAuthToken = provider

  // Add request interceptor
  apiClient.interceptors.request.use(async (config) => {
    if (getAuthToken) {
      const token = await getAuthToken()
      if (token) {
        config.headers.Authorization = `Bearer ${token}`
      }
    }
    return config
  })
}

export default apiClient
