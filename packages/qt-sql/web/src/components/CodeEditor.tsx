import { useRef } from 'react'
import './CodeEditor.css'

interface CodeEditorProps {
  value: string
  onChange: (value: string) => void
  language: 'sql' | 'dax'
  disabled?: boolean
  placeholder?: string
}

// SQL keywords for highlighting
const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL',
  'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL',
  'ORDER', 'BY', 'ASC', 'DESC', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
  'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE', 'DROP',
  'TABLE', 'INDEX', 'VIEW', 'DATABASE', 'ALTER', 'ADD', 'COLUMN',
  'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
  'UNION', 'ALL', 'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'NULLIF', 'CAST',
  'WITH', 'RECURSIVE', 'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK', 'DENSE_RANK',
]

// DAX keywords for highlighting
const DAX_KEYWORDS = [
  'CALCULATE', 'CALCULATETABLE', 'FILTER', 'ALL', 'ALLEXCEPT', 'ALLSELECTED',
  'VALUES', 'DISTINCT', 'SUMMARIZE', 'SUMMARIZECOLUMNS', 'ADDCOLUMNS',
  'SELECTCOLUMNS', 'TOPN', 'GENERATE', 'GENERATEALL', 'CROSSJOIN',
  'UNION', 'INTERSECT', 'EXCEPT', 'NATURALINNERJOIN', 'NATURALLEFTOUTERJOIN',
  'VAR', 'RETURN', 'IF', 'SWITCH', 'TRUE', 'FALSE', 'BLANK', 'ERROR',
  'SUM', 'SUMX', 'COUNT', 'COUNTX', 'COUNTA', 'COUNTAX', 'COUNTROWS',
  'AVERAGE', 'AVERAGEX', 'MIN', 'MINX', 'MAX', 'MAXX', 'DIVIDE',
  'RELATED', 'RELATEDTABLE', 'LOOKUPVALUE', 'EARLIER', 'EARLIEST',
  'USERELATIONSHIP', 'CROSSFILTER', 'TREATAS',
  'DATEADD', 'DATESYTD', 'DATESMTD', 'DATESQTD', 'SAMEPERIODLASTYEAR',
  'TOTALYTD', 'TOTALMTD', 'TOTALQTD', 'PREVIOUSYEAR', 'PREVIOUSMONTH',
  'FORMAT', 'CONCATENATE', 'CONCATENATEX', 'LEFT', 'RIGHT', 'MID', 'LEN',
  'ISBLANK', 'ISERROR', 'ISLOGICAL', 'ISNUMBER', 'ISTEXT',
]

function highlightCode(code: string, language: 'sql' | 'dax'): string {
  const keywords = language === 'sql' ? SQL_KEYWORDS : DAX_KEYWORDS
  let result = code
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')

  // Highlight strings (single and double quotes)
  result = result.replace(
    /('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")/g,
    '<span class="hl-string">$1</span>'
  )

  // Highlight numbers
  result = result.replace(
    /\b(\d+\.?\d*)\b/g,
    '<span class="hl-number">$1</span>'
  )

  // Highlight comments
  result = result.replace(
    /(--.*$|\/\/.*$)/gm,
    '<span class="hl-comment">$1</span>'
  )

  // Highlight block comments
  result = result.replace(
    /(\/\*[\s\S]*?\*\/)/g,
    '<span class="hl-comment">$1</span>'
  )

  // Highlight keywords (case insensitive)
  const keywordPattern = new RegExp(
    `\\b(${keywords.join('|')})\\b`,
    'gi'
  )
  result = result.replace(
    keywordPattern,
    '<span class="hl-keyword">$1</span>'
  )

  // Highlight table/column references like [TableName] or Table[Column]
  result = result.replace(
    /(\[[\w\s]+\])/g,
    '<span class="hl-reference">$1</span>'
  )

  // Highlight functions (word followed by parenthesis)
  result = result.replace(
    /\b([A-Za-z_]\w*)\s*(?=\()/g,
    '<span class="hl-function">$1</span>'
  )

  return result
}

export default function CodeEditor({
  value,
  onChange,
  language,
  disabled = false,
  placeholder = ''
}: CodeEditorProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const highlightRef = useRef<HTMLPreElement>(null)

  // Sync scroll between textarea and highlight overlay
  const handleScroll = () => {
    if (textareaRef.current && highlightRef.current) {
      highlightRef.current.scrollTop = textareaRef.current.scrollTop
      highlightRef.current.scrollLeft = textareaRef.current.scrollLeft
    }
  }

  // Generate line numbers
  const lines = value.split('\n')
  const lineCount = lines.length

  // Highlighted HTML
  const highlightedCode = highlightCode(value, language)

  return (
    <div className="code-editor">
      <div className="line-numbers">
        {Array.from({ length: lineCount }, (_, i) => (
          <div key={i + 1} className="line-num">{i + 1}</div>
        ))}
      </div>
      <div className="code-area">
        <pre
          ref={highlightRef}
          className="code-highlight"
          aria-hidden="true"
          dangerouslySetInnerHTML={{ __html: highlightedCode + '\n' }}
        />
        <textarea
          ref={textareaRef}
          className="code-input"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onScroll={handleScroll}
          spellCheck={false}
          placeholder={placeholder}
          disabled={disabled}
        />
      </div>
    </div>
  )
}
