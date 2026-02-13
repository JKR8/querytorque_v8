/**
 * TabBar Component
 * Reusable tab bar for results panel
 */

import './TabBar.css'

export interface Tab {
  id: string
  label: string
  badge?: string
}

interface TabBarProps {
  tabs: Tab[]
  activeTab: string
  onTabChange: (tabId: string) => void
}

export default function TabBar({ tabs, activeTab, onTabChange }: TabBarProps) {
  return (
    <div className="tab-bar">
      {tabs.map(tab => (
        <button
          key={tab.id}
          className={`tab-bar-item ${activeTab === tab.id ? 'active' : ''}`}
          onClick={() => onTabChange(tab.id)}
        >
          <span className="tab-bar-label">{tab.label}</span>
          {tab.badge && <span className="tab-bar-badge">{tab.badge}</span>}
        </button>
      ))}
    </div>
  )
}
