/**
 * @querytorque/ui-shared
 * Shared React component library for QueryTorque web apps
 */

// Components
export { CodeEditor } from './components/CodeEditor'
export type { CodeEditorProps, EditorLanguage } from './components/CodeEditor'

export { DropZone } from './components/DropZone'
export type { DropZoneProps } from './components/DropZone'

export { ReportViewer } from './components/ReportViewer'
export type { ReportViewerProps } from './components/ReportViewer'

export { ValidationReport } from './components/ValidationReport'
export type {
  ValidationReportProps,
  ValidationResult,
  ValidationIssue,
  PatchResult,
  EquivalenceDetails,
  PlanComparison,
  PlanNode,
} from './components/ValidationReport'

// Contexts
export { AuthProvider, useAuth, isAuthConfigured } from './contexts/AuthContext'
export type { AuthContextType, AuthConfig, User } from './contexts/AuthContext'

// Theme
// Note: Import tokens.css directly in your app if using CSS variables
// import '@querytorque/ui-shared/src/theme/tokens.css'
