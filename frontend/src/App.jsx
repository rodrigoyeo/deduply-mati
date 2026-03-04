import React, { useState, useEffect, useCallback, useRef, createContext, useContext, useMemo } from 'react';
import { LayoutDashboard, Users, Mail, FileText, Settings, Search, Plus, Trash2, X, Check, ArrowUpDown, Filter, Download, Upload, Edit2, LogOut, UserPlus, RefreshCw, ChevronLeft, ChevronRight, ChevronDown, ChevronUp, Loader2, AlertCircle, CheckCircle, Copy, ArrowRight, Layers, Merge, Eye, Webhook, Database, Send, Target, MessageCircle, MessageSquare, Zap, GitMerge, AlertTriangle, Trophy, List, LayoutGrid, Sparkles, Building2, User, ArrowRightLeft, Bold, Italic, Type, TrendingUp, Briefcase } from 'lucide-react';
import ReactQuill from 'react-quill';
import 'react-quill/dist/quill.snow.css';

const API_BASE = process.env.REACT_APP_API_URL || 'http://localhost:8001';
const API = `${API_BASE}/api`;

// API Helper
const api = {
  token: localStorage.getItem('deduply_token'),
  setToken(t) { this.token = t; t ? localStorage.setItem('deduply_token', t) : localStorage.removeItem('deduply_token'); },
  async fetch(endpoint, options = {}) {
    const headers = { 'Content-Type': 'application/json', ...options.headers };
    if (this.token) headers['Authorization'] = `Bearer ${this.token}`;
    const res = await fetch(`${API}${endpoint}`, { ...options, headers });
    if (!res.ok) { const e = await res.json().catch(() => ({ detail: 'Failed' })); throw new Error(e.detail || 'Failed'); }
    return res.json();
  },
  get: (e) => api.fetch(e),
  post: (e, d) => api.fetch(e, { method: 'POST', body: JSON.stringify(d) }),
  put: (e, d) => api.fetch(e, { method: 'PUT', body: JSON.stringify(d) }),
  delete: (e) => api.fetch(e, { method: 'DELETE' }),
};

// Hooks & Context
const useData = (endpoint) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const fetch = useCallback(async () => { setLoading(true); try { setData(await api.get(endpoint)); } catch {} setLoading(false); }, [endpoint]);
  useEffect(() => { fetch(); }, [fetch]);
  return { data, loading, refetch: fetch };
};

// Workspace-aware data hook — appends ?workspace=US/MX to endpoint
const useWorkspaceData = (endpoint) => {
  const { workspace } = useWorkspace();
  const sep = endpoint.includes('?') ? '&' : '?';
  return useData(`${endpoint}${sep}workspace=${workspace}`);
};

// Get current workspace from localStorage (for non-hook contexts)
const getWorkspace = () => localStorage.getItem('deduply_workspace') || 'US';

// Workspace-aware api.get helper
const wapi = {
  get: (endpoint) => {
    const ws = getWorkspace();
    const sep = endpoint.includes('?') ? '&' : '?';
    return api.get(`${endpoint}${sep}workspace=${ws}`);
  },
  post: (endpoint, data) => api.post(endpoint, { ...data, workspace: getWorkspace() }),
};

const ToastContext = createContext();
const ToastProvider = ({ children }) => {
  const [toasts, setToasts] = useState([]);
  const addToast = (msg, type = 'info') => { const id = Date.now(); setToasts(p => [...p, { id, msg, type }]); setTimeout(() => setToasts(p => p.filter(t => t.id !== id)), 3500); };
  return (<ToastContext.Provider value={{ addToast }}>{children}
    <div className="toast-container">{toasts.map(t => (<div key={t.id} className={`toast toast-${t.type}`}>{t.type === 'success' && <CheckCircle size={18} />}{t.type === 'error' && <AlertCircle size={18} />}{t.msg}</div>))}</div>
  </ToastContext.Provider>);
};
const useToast = () => useContext(ToastContext);

// Import Job Context for global progress tracking
const ImportJobContext = createContext();

// ============================================================
// WORKSPACE CONTEXT — US / MX toggle (persisted in localStorage)
// ============================================================
const WorkspaceContext = React.createContext({ workspace: 'US', setWorkspace: () => {} });
const useWorkspace = () => React.useContext(WorkspaceContext);

const WorkspaceProvider = ({ children }) => {
  const [workspace, setWorkspaceState] = React.useState(() => {
    return localStorage.getItem('deduply_workspace') || 'US';
  });
  const setWorkspace = (ws) => {
    localStorage.setItem('deduply_workspace', ws);
    setWorkspaceState(ws);
  };
  return (
    <WorkspaceContext.Provider value={{ workspace, setWorkspace }}>
      {children}
    </WorkspaceContext.Provider>
  );
};

const ImportJobProvider = ({ children }) => {
  const [importJob, setImportJob] = useState(null);

  // Check for active import jobs on mount
  useEffect(() => {
    const checkActiveJobs = async () => {
      try {
        const jobs = await api.get('/import/jobs/active');
        if (jobs.length > 0) {
          setImportJob(jobs[0]);
        }
      } catch (e) { console.error('Failed to check active import jobs:', e); }
    };
    checkActiveJobs();
  }, []);

  // Poll for job status when active
  useEffect(() => {
    if (!importJob?.id || importJob?.status === 'completed' || importJob?.status === 'failed' || importJob?.status === 'cancelled') return;

    const poll = async () => {
      try {
        const job = await api.get(`/import/job/${importJob.id}`);
        setImportJob(job);
      } catch (e) { console.error('Failed to poll import job:', e); }
    };

    const interval = setInterval(poll, 2000);
    return () => clearInterval(interval);
  }, [importJob?.id, importJob?.status]);

  const startImportJob = (job) => setImportJob(job);
  const clearImportJob = () => setImportJob(null);

  return (
    <ImportJobContext.Provider value={{ importJob, startImportJob, clearImportJob }}>
      {children}
    </ImportJobContext.Provider>
  );
};
const useImportJob = () => useContext(ImportJobContext);

// Modal
const Modal = ({ isOpen, onClose, title, children, size = 'md' }) => {
  if (!isOpen) return null;
  return (<div className="modal-overlay" onClick={onClose}><div className={`modal modal-${size}`} onClick={e => e.stopPropagation()}><div className="modal-header"><h2>{title}</h2><button className="modal-close" onClick={onClose}><X size={20} /></button></div><div className="modal-body">{children}</div></div></div>);
};

// Beautiful Multi-Select Dropdown
const MultiSelect = ({ options, value = [], onChange, placeholder = "Select...", label, renderOption, renderTag }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const containerRef = useRef(null);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (containerRef.current && !containerRef.current.contains(e.target)) setIsOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filtered = options.filter(opt =>
    opt.name?.toLowerCase().includes(search.toLowerCase()) ||
    opt.label?.toLowerCase().includes(search.toLowerCase())
  );

  const toggle = (id) => {
    onChange(value.includes(id) ? value.filter(v => v !== id) : [...value, id]);
  };

  const selectAll = () => onChange(options.map(o => o.id));
  const clearAll = () => onChange([]);

  const selectedItems = options.filter(o => value.includes(o.id));

  return (
    <div className="multi-select-container" ref={containerRef}>
      {label && <label className="multi-select-label">{label}</label>}
      <div className={`multi-select-trigger ${isOpen ? 'open' : ''}`} onClick={() => setIsOpen(!isOpen)}>
        <div className="multi-select-tags">
          {selectedItems.length === 0 ? (
            <span className="multi-select-placeholder">{placeholder}</span>
          ) : selectedItems.length <= 3 ? (
            selectedItems.map(item => (
              <span key={item.id} className="multi-select-tag">
                {renderTag ? renderTag(item) : item.name || item.label}
                <X size={14} onClick={(e) => { e.stopPropagation(); toggle(item.id); }} />
              </span>
            ))
          ) : (
            <span className="multi-select-tag count">{selectedItems.length} selected</span>
          )}
        </div>
        <ChevronDown size={18} className={`multi-select-arrow ${isOpen ? 'rotated' : ''}`} />
      </div>

      {isOpen && (
        <div className="multi-select-dropdown">
          <div className="multi-select-search">
            <Search size={16} />
            <input
              type="text"
              placeholder="Search..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              onClick={e => e.stopPropagation()}
              autoFocus
            />
          </div>
          <div className="multi-select-actions">
            <button type="button" onClick={selectAll}>Select All</button>
            <button type="button" onClick={clearAll}>Clear All</button>
          </div>
          <div className="multi-select-options">
            {filtered.length === 0 ? (
              <div className="multi-select-empty">No options found</div>
            ) : (
              filtered.map(option => (
                <div
                  key={option.id}
                  className={`multi-select-option ${value.includes(option.id) ? 'selected' : ''}`}
                  onClick={() => toggle(option.id)}
                >
                  <div className={`multi-select-checkbox ${value.includes(option.id) ? 'checked' : ''}`}>
                    {value.includes(option.id) && <Check size={12} />}
                  </div>
                  {renderOption ? renderOption(option) : (
                    <span className="multi-select-option-label">{option.name || option.label}</span>
                  )}
                </div>
              ))
            )}
          </div>
          <div className="multi-select-footer">
            {value.length} of {options.length} selected
          </div>
        </div>
      )}
    </div>
  );
};

// Sidebar with Arkode Branding

// Workspace Toggle — shown in sidebar
const WorkspaceToggle = ({ collapsed }) => {
  const { workspace, setWorkspace } = useWorkspace();
  if (collapsed) {
    return (
      <div className="ws-toggle-collapsed">
        <button onClick={() => setWorkspace(workspace === 'US' ? 'MX' : 'US')} className="ws-toggle-icon" title={`Switch to ${workspace === 'US' ? 'MX' : 'US'}`}>
          {workspace === 'US' ? '🇺🇸' : '🇲🇽'}
        </button>
      </div>
    );
  }
  return (
    <div className="ws-toggle">
      <div className="ws-toggle-label">WORKSPACE</div>
      <div className="ws-toggle-buttons">
        {[{id:'US', flag:'🇺🇸', label:'US'}, {id:'MX', flag:'🇲🇽', label:'MX'}].map(ws => (
          <button key={ws.id} onClick={() => setWorkspace(ws.id)}
            className={`ws-btn ${workspace === ws.id ? 'ws-btn-active' : ''}`}>
            <span className="ws-flag">{ws.flag}</span>
            <span className="ws-label">{ws.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
};

const Sidebar = ({ page, setPage, user, onLogout }) => {
  const { data: stats } = useData('/stats');
  const { importJob, clearImportJob } = useImportJob();
  const { addToast } = useToast();
  const [collapsed, setCollapsed] = useState(() => {
    return localStorage.getItem('deduply_sidebar_collapsed') === 'true';
  });
  const toggleCollapsed = () => {
    const next = !collapsed;
    localStorage.setItem('deduply_sidebar_collapsed', String(next));
    setCollapsed(next);
  };
  const nav = [{ id: 'inbox', label: 'Inbox', icon: MessageCircle }, { id: 'pipeline', label: 'Pipeline', icon: Target }, { id: 'campaigns', label: 'Campaigns', icon: Mail }, { id: 'contacts', label: 'Contacts', icon: Users }, { id: 'reports', label: 'Reports', icon: TrendingUp }, { id: 'settings', label: 'Settings', icon: Settings }];

  // Show toast when import completes
  useEffect(() => {
    if (importJob?.status === 'completed') {
      addToast(`Import complete: ${importJob.imported_count} imported, ${importJob.merged_count} merged`, 'success');
    } else if (importJob?.status === 'failed') {
      addToast(`Import failed: ${importJob.error_message || 'Unknown error'}`, 'error');
    }
  }, [importJob?.status]);

  const progress = importJob?.total_rows > 0 ? Math.round((importJob.processed_count / importJob.total_rows) * 100) : 0;

  return (<aside className={`sidebar ${collapsed ? 'sidebar-collapsed' : ''}`}>
    <div className="sidebar-header">
      <div className="sidebar-top-row">
        <div className="logo" style={{cursor:'pointer'}} onClick={() => setPage('inbox')}>
          {!collapsed && <span className="logo-text">Deduply</span>}
          {collapsed && <span className="logo-mark">D</span>}
        </div>
        <button className="sidebar-collapse-btn" onClick={toggleCollapsed} title={collapsed ? 'Expand' : 'Collapse'}>
          {collapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
        </button>
      </div>
    </div>

    <WorkspaceToggle collapsed={collapsed} />

    <nav className="sidebar-nav">{nav.map(item => (<button key={item.id} className={`nav-item ${page === item.id ? 'active' : ''}`} onClick={() => setPage(item.id)} title={item.label}><item.icon size={18} />{!collapsed && <span>{item.label}</span>}{!collapsed && item.id === 'contacts' && stats && <span className="nav-badge">{stats.unique_contacts?.toLocaleString()}</span>}{!collapsed && item.id === 'campaigns' && stats && <span className="nav-badge">{stats.total_campaigns}</span>}</button>))}</nav>

    {/* Import Progress Indicator */}
    {importJob && (importJob.status === 'pending' || importJob.status === 'running') && (
      <div className="sidebar-import-progress">
        <div className="import-progress-header">
          <Upload size={14} />
          <span>Importing {importJob.file_name || 'CSV'}...</span>
        </div>
        <div className="import-progress-bar">
          <div className="import-progress-fill" style={{ width: `${progress}%` }} />
        </div>
        <div className="import-progress-stats">
          {importJob.processed_count} / {importJob.total_rows} rows ({progress}%)
        </div>
        {importJob.current_row && (
          <div className="import-progress-current">
            <Loader2 className="spin" size={12} /> {importJob.current_row}
          </div>
        )}
      </div>
    )}

    {/* Import Completed */}
    {importJob && importJob.status === 'completed' && (
      <div className="sidebar-import-progress completed">
        <div className="import-progress-header">
          <CheckCircle size={14} />
          <span>Import Complete</span>
          <button className="import-dismiss" onClick={clearImportJob}><X size={12} /></button>
        </div>
        <div className="import-progress-stats">
          {importJob.imported_count} imported, {importJob.merged_count} merged
        </div>
      </div>
    )}

    <div className="sidebar-footer">
      {user && (<div className="user-info"><div className="user-avatar">{user.name?.[0] || user.email[0]}</div>{!collapsed && <><div className="user-details"><span className="user-name">{user.name || user.email}</span><span className="user-role">{user.role}</span></div><button className="logout-btn" onClick={onLogout} title="Logout"><LogOut size={16} /></button></>}</div>)}
    </div>
  </aside>);
};

// Login with Arkode Branding
const LoginPage = ({ onLogin }) => {
  const [email, setEmail] = useState('admin@deduply.io');
  const [password, setPassword] = useState('admin123');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const handleSubmit = async (e) => { e.preventDefault(); setLoading(true); setError(''); try { const r = await api.post('/auth/login', { email, password }); api.setToken(r.token); onLogin(r.user); } catch (e) { setError(e.message); } setLoading(false); };
  return (
    <div className="login-page">
      <div className="login-card">
        <div className="login-header">
          <div className="login-logo">
            <div className="login-logo-icon">D</div>
            <div className="login-logo-text">
              <span className="login-brand">Deduply</span>
              <span className="login-by">by Arkode</span>
            </div>
          </div>
          <h1>Welcome back</h1>
          <p>Sign in to manage your cold email operations</p>
        </div>
        <form onSubmit={handleSubmit}>
          {error && <div className="login-error"><AlertCircle size={16} />{error}</div>}
          <div className="form-group">
            <label>Email address</label>
            <input type="email" value={email} onChange={e => setEmail(e.target.value)} required placeholder="you@company.com" />
          </div>
          <div className="form-group">
            <label>Password</label>
            <input type="password" value={password} onChange={e => setPassword(e.target.value)} required placeholder="Enter your password" />
          </div>
          <button type="submit" className="btn btn-primary btn-block" disabled={loading}>
            {loading ? <Loader2 className="spin" size={18} /> : 'Sign In'}
          </button>
        </form>
        <div className="login-footer">
          <span className="login-hint">Demo credentials: admin@deduply.io / admin123</span>
          <div className="login-arkode">
            <span>Powered by</span>
            <strong>Arkode</strong>
          </div>
        </div>
      </div>
    </div>
  );
};

// Contacts Insights View Component with Charts
const ContactsInsightsView = ({ data }) => {
  const total = data.data_quality?.total || 0;
  const statusData = data.by_status || [];
  const seniorityData = data.by_seniority || [];
  const industryData = data.by_industry?.slice(0, 5) || [];
  const countryData = data.by_country?.slice(0, 5) || [];
  const companySizeData = data.by_company_size || [];
  const topCompanies = data.top_companies?.slice(0, 5) || [];
  const emailStatusData = data.by_email_status || [];

  // Calculate key metrics
  const seniorityTotal = seniorityData.reduce((sum, s) => sum + s.value, 0);
  const decisionMakers = seniorityData.filter(s =>
    ['C suite', 'Vp', 'Director', 'Founder', 'Owner', 'Partner'].includes(s.name)
  ).reduce((sum, s) => sum + s.value, 0);
  const decisionMakerPct = seniorityTotal > 0 ? ((decisionMakers / seniorityTotal) * 100).toFixed(0) : 0;

  const uniqueCompanies = topCompanies.length > 0 ? data.unique_companies || Math.floor(total / 3) : 0;
  const topIndustry = industryData[0]?.name || 'N/A';
  const topCountry = countryData[0]?.name || 'N/A';

  // Stacked bar colors
  const stackColors = ['#FF6C5D', '#001C43', '#05E2C1', '#8B5CF6', '#F59E0B', '#EC4899', '#6B7280'];

  // Email status colors and pie chart calculation
  const emailStatusColors = { 'Valid': '#10B981', 'Invalid': '#EF4444', 'Unknown': '#F59E0B', 'Not Verified': '#6B7280' };
  const emailStatusTotal = emailStatusData.reduce((sum, s) => sum + s.value, 0);
  const emailStatusPieGradient = (() => {
    if (emailStatusTotal === 0) return 'conic-gradient(#e5e7eb 0% 100%)';
    let cumulative = 0;
    const segments = emailStatusData.map(item => {
      const start = cumulative;
      const pct = (item.value / emailStatusTotal) * 100;
      cumulative += pct;
      const color = emailStatusColors[item.name] || '#6B7280';
      return `${color} ${start}% ${cumulative}%`;
    });
    return `conic-gradient(${segments.join(', ')})`;
  })();

  return (
    <div className="ci-dashboard">
      {/* Hero Metrics */}
      <div className="ci-hero">
        <div className="ci-hero-main">
          <div className="ci-hero-number">{total.toLocaleString()}</div>
          <div className="ci-hero-label">Total Contacts in Database</div>
        </div>
        <div className="ci-hero-stats">
          <div className="ci-hero-stat">
            <div className="ci-hero-stat-value">{decisionMakerPct}%</div>
            <div className="ci-hero-stat-label">Decision Makers</div>
            <div className="ci-hero-stat-sub">VP level and above</div>
          </div>
          <div className="ci-hero-stat">
            <div className="ci-hero-stat-value">{uniqueCompanies.toLocaleString()}</div>
            <div className="ci-hero-stat-label">Companies</div>
            <div className="ci-hero-stat-sub">Unique organizations</div>
          </div>
          <div className="ci-hero-stat">
            <div className="ci-hero-stat-value">{industryData.length}</div>
            <div className="ci-hero-stat-label">Industries</div>
            <div className="ci-hero-stat-sub">Top: {topIndustry}</div>
          </div>
          <div className="ci-hero-stat">
            <div className="ci-hero-stat-value">{countryData.length}</div>
            <div className="ci-hero-stat-label">Countries</div>
            <div className="ci-hero-stat-sub">Top: {topCountry}</div>
          </div>
        </div>
      </div>

      {/* Email Status Distribution - Pie Chart */}
      <div className="ci-section">
        <div className="ci-section-header">
          <h3>Email Verification Status</h3>
          <span className="ci-section-insight">
            {(() => {
              const validItem = emailStatusData.find(s => s.name === 'Valid');
              const validPct = emailStatusTotal > 0 && validItem ? ((validItem.value / emailStatusTotal) * 100).toFixed(0) : 0;
              return validPct >= 50 ? '✓ Good email quality' : '⚠ Consider verifying more emails';
            })()}
          </span>
        </div>
        <div className="ci-pie-container">
          <div className="ci-pie-chart" style={{ background: emailStatusPieGradient }}>
            <div className="ci-pie-center">
              <div className="ci-pie-total">{emailStatusTotal.toLocaleString()}</div>
              <div className="ci-pie-label">Total</div>
            </div>
          </div>
          <div className="ci-pie-legend">
            {emailStatusData.map((item, i) => {
              const pct = emailStatusTotal > 0 ? (item.value / emailStatusTotal) * 100 : 0;
              return (
                <div key={i} className="ci-pie-legend-item">
                  <span className="ci-legend-dot" style={{ background: emailStatusColors[item.name] || '#6B7280' }} />
                  <span className="ci-legend-name">{item.name}</span>
                  <span className="ci-legend-value">{item.value.toLocaleString()}</span>
                  <span className="ci-legend-pct">{pct.toFixed(1)}%</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Seniority Breakdown - Stacked Bar */}
      <div className="ci-section">
        <div className="ci-section-header">
          <h3>Seniority Breakdown</h3>
          <span className="ci-section-insight">
            {decisionMakerPct >= 50 ? '✓ Strong decision-maker coverage' : '⚠ Consider targeting more senior contacts'}
          </span>
        </div>
        <div className="ci-stacked-bar">
          {seniorityData.map((item, i) => {
            const pct = seniorityTotal > 0 ? (item.value / seniorityTotal) * 100 : 0;
            return pct > 0 ? (
              <div
                key={i}
                className="ci-stacked-segment"
                style={{ width: `${pct}%`, background: stackColors[i % stackColors.length] }}
                title={`${item.name}: ${item.value.toLocaleString()} (${pct.toFixed(1)}%)`}
              />
            ) : null;
          })}
        </div>
        <div className="ci-stacked-legend">
          {seniorityData.slice(0, 6).map((item, i) => {
            const pct = seniorityTotal > 0 ? (item.value / seniorityTotal) * 100 : 0;
            return (
              <div key={i} className="ci-legend-item">
                <span className="ci-legend-dot" style={{ background: stackColors[i % stackColors.length] }} />
                <span className="ci-legend-name">{item.name}</span>
                <span className="ci-legend-pct">{pct.toFixed(1)}%</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Two Column Layout */}
      <div className="ci-columns">
        {/* Industry Distribution */}
        <div className="ci-card">
          <h3>Top Industries</h3>
          <div className="ci-bar-list">
            {industryData.map((item, i) => {
              const maxVal = industryData[0]?.value || 1;
              const pct = (item.value / maxVal) * 100;
              return (
                <div key={i} className="ci-bar-item">
                  <div className="ci-bar-info">
                    <span className="ci-bar-name">{item.name}</span>
                    <span className="ci-bar-value">{item.value.toLocaleString()}</span>
                  </div>
                  <div className="ci-bar-track">
                    <div className="ci-bar-fill" style={{ width: `${pct}%` }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Geographic Distribution */}
        <div className="ci-card">
          <h3>Geographic Reach</h3>
          <div className="ci-bar-list">
            {countryData.map((item, i) => {
              const maxVal = countryData[0]?.value || 1;
              const pct = (item.value / maxVal) * 100;
              return (
                <div key={i} className="ci-bar-item">
                  <div className="ci-bar-info">
                    <span className="ci-bar-name">{item.name}</span>
                    <span className="ci-bar-value">{item.value.toLocaleString()}</span>
                  </div>
                  <div className="ci-bar-track">
                    <div className="ci-bar-fill coral" style={{ width: `${pct}%` }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Company Intelligence */}
      <div className="ci-columns">
        {/* Company Size */}
        <div className="ci-card">
          <h3>Company Size Distribution</h3>
          <div className="ci-size-grid">
            {(() => {
              const items = companySizeData.slice(0, 5);
              const maxVal = Math.max(...items.map(i => i.value), 1);
              const sizeTotal = items.reduce((sum, i) => sum + i.value, 0);
              return items.map((item, i) => {
                const pct = sizeTotal > 0 ? (item.value / sizeTotal) * 100 : 0;
                const barHeight = (item.value / maxVal) * 100;
                return (
                  <div key={i} className="ci-size-item">
                    <div className="ci-size-range">{item.name}</div>
                    <div className="ci-size-bar">
                      <div className="ci-size-fill" style={{ height: `${Math.max(barHeight, 4)}%` }} />
                    </div>
                    <div className="ci-size-value">{pct >= 1 ? pct.toFixed(0) : pct.toFixed(1)}%</div>
                    <div className="ci-size-count">{item.value.toLocaleString()}</div>
                  </div>
                );
              });
            })()}
          </div>
        </div>

        {/* Top Companies */}
        <div className="ci-card">
          <h3>Top Companies</h3>
          <div className="ci-company-list">
            {topCompanies.map((item, i) => (
              <div key={i} className="ci-company-item">
                <span className="ci-company-rank">{i + 1}</span>
                <span className="ci-company-name">{item.name}</span>
                <span className="ci-company-count">{item.value} contacts</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

// Dashboard
const DashboardPage = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const { data: stats, loading } = useData('/stats');
  const [dbStats, setDbStats] = useState(null);
  const [perfStats, setPerfStats] = useState(null);
  const [funnelStats, setFunnelStats] = useState(null);
  const [loadingDb, setLoadingDb] = useState(false);
  const [loadingPerf, setLoadingPerf] = useState(false);
  const [loadingFunnel, setLoadingFunnel] = useState(false);
  const [learningStats, setLearningStats] = useState(null);
  const [abWinners, setAbWinners] = useState(null);
  const [workspaceCompare, setWorkspaceCompare] = useState(null);
  const [loadingLearning, setLoadingLearning] = useState(false);

  useEffect(() => {
    if (activeTab === 'database' && !dbStats) {
      setLoadingDb(true);
      api.get('/stats/database').then(setDbStats).finally(() => setLoadingDb(false));
    }
    if (activeTab === 'performance' && !perfStats) {
      setLoadingPerf(true);
      api.get('/stats/performance').then(setPerfStats).finally(() => setLoadingPerf(false));
    }
    if (activeTab === 'funnel' && !funnelStats) {
      setLoadingFunnel(true);
      api.get('/stats/funnel').then(setFunnelStats).finally(() => setLoadingFunnel(false));
    }
    if (activeTab === 'learning' && !learningStats) {
      setLoadingLearning(true);
      Promise.all([
        api.get('/analytics/learning'),
        api.get('/analytics/ab-winners'),
        api.get('/analytics/workspace-compare'),
      ]).then(([ls, ab, ws]) => {
        setLearningStats(ls);
        setAbWinners(ab);
        setWorkspaceCompare(ws);
      }).finally(() => setLoadingLearning(false));
    }
  }, [activeTab, dbStats, perfStats, funnelStats, learningStats]);

  if (loading || !stats) return <div className="page loading"><Loader2 className="spin" size={24} /></div>;

  return (<div className="page dashboard-page">
    <div className="page-header">
      <div><h1>Dashboard</h1><p className="subtitle">Analytics and insights for your cold email operations</p></div>
    </div>

    <div className="dashboard-tabs">
      <button className={`dash-tab ${activeTab === 'overview' ? 'active' : ''}`} onClick={() => setActiveTab('overview')}>
        <LayoutDashboard size={18} /> Overview
      </button>
      <button className={`dash-tab ${activeTab === 'performance' ? 'active' : ''}`} onClick={() => setActiveTab('performance')}>
        <Target size={18} /> Performance
      </button>
      <button className={`dash-tab ${activeTab === 'database' ? 'active' : ''}`} onClick={() => setActiveTab('database')}>
        <Users size={18} /> Contacts Insights
      </button>
      <button className={`dash-tab ${activeTab === 'funnel' ? 'active' : ''}`} onClick={() => setActiveTab('funnel')}>
        <TrendingUp size={18} /> Sales Funnel
      </button>
      <button className={`dash-tab ${activeTab === 'learning' ? 'active' : ''}`} onClick={() => setActiveTab('learning')}>
        <Sparkles size={18} /> What's Working
      </button>
    </div>

    {activeTab === 'overview' && (
      <>
        <div className="stats-grid">
          <div className="stat-card accent"><div className="stat-icon"><Users size={24} /></div><div className="stat-value">{stats.unique_contacts?.toLocaleString()}</div><div className="stat-label">Total Contacts</div></div>
          <div className="stat-card"><div className="stat-value">{stats.total_campaigns}</div><div className="stat-label">Campaigns</div></div>
          <div className="stat-card"><div className="stat-value">{stats.emails_sent?.toLocaleString()}</div><div className="stat-label">Emails Sent</div></div>
          <div className="stat-card"><div className="stat-value">{stats.avg_open_rate}%</div><div className="stat-label">Open Rate</div></div>
          <div className="stat-card"><div className="stat-value">{stats.avg_reply_rate}%</div><div className="stat-label">Reply Rate</div></div>
          <div className="stat-card navy"><div className="stat-value">{stats.opportunities}</div><div className="stat-label">Opportunities</div></div>
        </div>
        <div className="dashboard-grid">
          <div className="card"><h3>Contacts by Status</h3><div className="chart-bars">{Object.entries(stats.by_status || {}).slice(0, 6).map(([s, c]) => (<div key={s} className="chart-row"><span className="chart-label">{s}</span><div className="chart-bar-track"><div className="chart-bar-fill" style={{ width: `${Math.min(100, (c / stats.unique_contacts) * 100)}%` }}></div></div><span className="chart-count">{c.toLocaleString()}</span></div>))}</div></div>
          <div className="card"><h3>Top Campaigns</h3><div className="chart-bars">{(stats.by_campaign || []).slice(0, 6).map(([n, c]) => (<div key={n} className="chart-row"><span className="chart-label">{n}</span><div className="chart-bar-track"><div className="chart-bar-fill navy" style={{ width: `${Math.min(100, (c / (stats.by_campaign?.[0]?.[1] || 1)) * 100)}%` }}></div></div><span className="chart-count">{c}</span></div>))}</div></div>
        </div>
      </>
    )}

    {activeTab === 'performance' && (
      loadingPerf ? <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading performance data...</span></div> : perfStats && (
        <>
          <div className="stats-grid">
            <div className="stat-card"><div className="stat-value">{perfStats.totals?.sent?.toLocaleString() || 0}</div><div className="stat-label">Total Sent</div></div>
            <div className="stat-card accent"><div className="stat-value">{perfStats.totals?.open_rate || 0}%</div><div className="stat-label">Open Rate</div></div>
            <div className="stat-card accent"><div className="stat-value">{perfStats.totals?.reply_rate || 0}%</div><div className="stat-label">Reply Rate</div></div>
            <div className="stat-card"><div className="stat-value">{perfStats.totals?.bounce_rate || 0}%</div><div className="stat-label">Bounce Rate</div></div>
            <div className="stat-card navy"><div className="stat-value">{perfStats.totals?.meetings || 0}</div><div className="stat-label">Meetings</div></div>
            <div className="stat-card navy"><div className="stat-value">{perfStats.totals?.opportunities || 0}</div><div className="stat-label">Opportunities</div></div>
          </div>
          <div className="dashboard-grid">
            <div className="card full-width"><h3>Campaign Performance</h3>
              <div className="perf-table-wrapper"><table className="perf-table"><thead><tr><th>Campaign</th><th>Country</th><th>Sent</th><th>Opened</th><th>Replied</th><th>Open Rate</th><th>Reply Rate</th><th>Meetings</th></tr></thead>
                <tbody>{perfStats.campaigns?.slice(0, 10).map(c => (<tr key={c.id}><td className="perf-name">{c.name}</td><td>{c.country ? <span className={`country-badge country-${c.country?.toLowerCase().replace(/ /g, '-')}`}>{c.country}</span> : '—'}</td><td>{c.emails_sent?.toLocaleString()}</td><td>{c.emails_opened?.toLocaleString()}</td><td>{c.emails_replied?.toLocaleString()}</td><td className="highlight">{c.open_rate || 0}%</td><td className="highlight">{c.reply_rate || 0}%</td><td>{c.meetings_booked || 0}</td></tr>))}</tbody></table></div>
            </div>
            <div className="card"><h3>Performance by Country</h3><div className="chart-bars">{perfStats.by_country?.map((c, i) => (<div key={i} className="chart-row"><span className="chart-label">{c.country}</span><div className="chart-bar-track"><div className="chart-bar-fill" style={{ width: `${Math.min(100, (c.sent / (perfStats.by_country?.[0]?.sent || 1)) * 100)}%`, background: 'var(--coral)' }}></div></div><span className="chart-count">{c.sent.toLocaleString()} sent</span></div>))}</div></div>
            <div className="card"><h3>Top Templates</h3><div className="chart-bars">{perfStats.top_templates?.slice(0, 6).map(t => (<div key={t.id} className="chart-row"><span className="chart-label">{t.name} ({t.variant})</span><div className="chart-bar-track"><div className="chart-bar-fill navy" style={{ width: `${Math.min(100, t.reply_rate)}%` }}></div></div><span className="chart-count">{t.reply_rate || 0}% reply</span></div>))}</div></div>
          </div>
        </>
      )
    )}

    {activeTab === 'database' && (
      loadingDb ? <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading contacts insights...</span></div> : dbStats && (
        <ContactsInsightsView data={dbStats} />
      )
    )}

    {activeTab === 'funnel' && (
      loadingFunnel ? <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading funnel data...</span></div> : funnelStats && (
        <>
          <div className="stats-grid">
            <div className="stat-card accent"><div className="stat-value">{funnelStats.conversions?.reply_rate?.toFixed(1) || 0}%</div><div className="stat-label">Reply Rate</div></div>
            <div className="stat-card"><div className="stat-value">{funnelStats.conversions?.booked_rate?.toFixed(1) || 0}%</div><div className="stat-label">Booked Rate</div></div>
            <div className="stat-card"><div className="stat-value">{funnelStats.conversions?.show_rate?.toFixed(1) || 0}%</div><div className="stat-label">Show Rate</div></div>
            <div className="stat-card"><div className="stat-value">{funnelStats.conversions?.qualified_rate?.toFixed(1) || 0}%</div><div className="stat-label">Qualified Rate</div></div>
            <div className="stat-card navy"><div className="stat-value">{funnelStats.conversions?.close_rate?.toFixed(1) || 0}%</div><div className="stat-label">Close Rate</div></div>
            <div className="stat-card navy"><div className="stat-value">{funnelStats.conversions?.overall_conversion?.toFixed(2) || 0}%</div><div className="stat-label">Overall Conversion</div></div>
          </div>
          <div className="dashboard-grid">
            <div className="card funnel-card">
              <h3>Sales Funnel</h3>
              <div className="funnel-chart">
                {['Lead', 'Contacted', 'Replied', 'Interested', 'Meeting Booked', 'Qualified', 'Client'].map((stage, idx, arr) => {
                  const count = funnelStats.funnel?.[stage] || 0;
                  const maxCount = Math.max(...Object.values(funnelStats.funnel || {}), 1);
                  const width = Math.max(20, (count / maxCount) * 100);
                  const nextStage = arr[idx + 1];
                  const nextCount = nextStage ? (funnelStats.funnel?.[nextStage] || 0) : null;
                  const conversionRate = count > 0 && nextCount !== null ? ((nextCount / count) * 100).toFixed(1) : null;
                  return (
                    <div key={stage} className="funnel-stage">
                      <div className="funnel-bar-wrapper">
                        <div className={`funnel-bar funnel-stage-${idx}`} style={{ width: `${width}%` }}>
                          <span className="funnel-stage-name">{stage}</span>
                          <span className="funnel-stage-count">{count.toLocaleString()}</span>
                        </div>
                      </div>
                      {conversionRate && <div className="funnel-conversion">{conversionRate}%</div>}
                    </div>
                  );
                })}
              </div>
            </div>
            <div className="card">
              <h3>Other Statuses</h3>
              <div className="chart-bars">
                {['No-Show', 'Not Interested', 'Bounced', 'Unsubscribed'].map(status => {
                  const count = funnelStats.funnel?.[status] || 0;
                  const maxCount = Math.max(...['No-Show', 'Not Interested', 'Bounced', 'Unsubscribed'].map(s => funnelStats.funnel?.[s] || 0), 1);
                  return (
                    <div key={status} className="chart-row">
                      <span className="chart-label">{status}</span>
                      <div className="chart-bar-track">
                        <div className="chart-bar-fill" style={{ width: `${(count / maxCount) * 100}%`, background: status === 'Bounced' || status === 'Unsubscribed' ? '#ef4444' : '#f59e0b' }}></div>
                      </div>
                      <span className="chart-count">{count.toLocaleString()}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </>
      )
    )}

    {activeTab === 'learning' && (
      loadingLearning ? <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading learning analytics...</span></div> : (
        <div className="dashboard-grid">
          {/* Top Templates */}
          <div className="card">
            <h3><Sparkles size={16} style={{display:'inline',marginRight:6,color:'var(--coral)'}} />What's Working — Top Templates</h3>
            {learningStats?.top_templates?.length > 0 ? (
              <div className="chart-bars">
                {learningStats.top_templates.map((t, i) => (
                  <div key={i} className="chart-row">
                    <span className="chart-label">
                      <span style={{marginRight:4}}>{t.workspace === 'MX' ? '🇲🇽' : '🇺🇸'}</span>
                      {t.template_name} ({t.variant})
                    </span>
                    <div className="chart-bar-track">
                      <div className="chart-bar-fill" style={{ width: `${Math.min(100, t.reply_rate * 5)}%`, background: 'var(--coral)' }}></div>
                    </div>
                    <span className="chart-count">{t.reply_rate}% reply ({t.sends} sent)</span>
                  </div>
                ))}
              </div>
            ) : <p className="empty-hint">No template data yet — data appears after campaigns run.</p>}
            {learningStats?.best_days?.length > 0 && (
              <>
                <h4 style={{marginTop:20,marginBottom:8,fontSize:'0.85rem',color:'var(--text-muted)'}}>Best Days to Send</h4>
                <div className="chart-bars">
                  {learningStats.best_days.slice(0, 5).map((d, i) => (
                    <div key={i} className="chart-row">
                      <span className="chart-label">{d.day}</span>
                      <div className="chart-bar-track">
                        <div className="chart-bar-fill navy" style={{ width: `${Math.min(100, (d.events / (learningStats.best_days[0]?.events || 1)) * 100)}%` }}></div>
                      </div>
                      <span className="chart-count">{d.events} events</span>
                    </div>
                  ))}
                </div>
              </>
            )}
            {learningStats && (
              <div style={{marginTop:16,display:'flex',gap:24,flexWrap:'wrap'}}>
                <div className="stat-card" style={{flex:'1 1 120px',minHeight:'auto',padding:'10px 14px'}}>
                  <div className="stat-value" style={{fontSize:'1.4rem'}}>{learningStats.reply_to_interested_rate ?? 0}%</div>
                  <div className="stat-label">Reply → Interested</div>
                </div>
                <div className="stat-card" style={{flex:'1 1 120px',minHeight:'auto',padding:'10px 14px'}}>
                  <div className="stat-value" style={{fontSize:'1.4rem'}}>{learningStats.avg_steps_to_reply ?? '—'}</div>
                  <div className="stat-label">Avg Steps to Reply</div>
                </div>
              </div>
            )}
          </div>

          {/* US vs MX Comparison */}
          <div className="card">
            <h3><ArrowRightLeft size={16} style={{display:'inline',marginRight:6,color:'var(--navy)'}} />US vs MX Workspace</h3>
            {workspaceCompare?.workspaces && (
              <div style={{display:'flex',gap:16,flexWrap:'wrap'}}>
                {['US', 'MX'].map(ws => {
                  const d = workspaceCompare.workspaces[ws];
                  return (
                    <div key={ws} style={{flex:'1 1 160px',background:'var(--bg-secondary)',borderRadius:8,padding:14}}>
                      <div style={{fontWeight:700,fontSize:'1.1rem',marginBottom:10}}>
                        {ws === 'US' ? '🇺🇸 US' : '🇲🇽 MX'}
                      </div>
                      <div className="chart-bars" style={{gap:6}}>
                        {[
                          ['Campaigns', d.campaign_count],
                          ['Sent', d.sent?.toLocaleString()],
                          ['Open Rate', `${d.open_rate}%`],
                          ['Reply Rate', `${d.reply_rate}%`],
                          ['Interested', d.interested],
                          ['Meetings', d.meetings],
                        ].map(([label, val]) => (
                          <div key={label} style={{display:'flex',justifyContent:'space-between',fontSize:'0.82rem',padding:'2px 0',borderBottom:'1px solid var(--border-light)'}}>
                            <span style={{color:'var(--text-muted)'}}>{label}</span>
                            <span style={{fontWeight:600}}>{val ?? 0}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* A/B Winners */}
          <div className="card full-width">
            <h3><Trophy size={16} style={{display:'inline',marginRight:6,color:'#f59e0b'}} />A/B Test Winners</h3>
            {abWinners?.winners?.length > 0 ? (
              <div className="perf-table-wrapper">
                <table className="perf-table">
                  <thead>
                    <tr>
                      <th>Campaign</th>
                      <th>Winner</th>
                      <th>Winner Reply Rate</th>
                      <th>Winner Sends</th>
                      <th>Loser</th>
                      <th>Loser Reply Rate</th>
                      <th>Delta</th>
                    </tr>
                  </thead>
                  <tbody>
                    {abWinners.winners.map((w, i) => (
                      <tr key={i}>
                        <td className="perf-name">{w.campaign_name}</td>
                        <td><span style={{background:'#dcfce7',color:'#166534',borderRadius:4,padding:'2px 8px',fontWeight:700}}>Variant {w.winner_variant}</span></td>
                        <td className="highlight">{w.winner_reply_rate}%</td>
                        <td>{w.winner_sends}</td>
                        <td>Variant {w.loser_variant}</td>
                        <td>{w.loser_reply_rate}%</td>
                        <td style={{color:'var(--coral)',fontWeight:600}}>+{(w.winner_reply_rate - w.loser_reply_rate).toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : <p className="empty-hint">No A/B winners yet — campaigns need ≥ 20 sends per variant with different reply rates.</p>}
          </div>
        </div>
      )
    )}
  </div>);
};

// Contacts Page
const ContactsPage = () => {
  const { addToast } = useToast();
  const [contacts, setContacts] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [search, setSearch] = useState('');
  const [filters, setFilters] = useState({});
  const [sortBy, setSortBy] = useState('id');
  const [sortOrder, setSortOrder] = useState('desc');
  const [selected, setSelected] = useState(new Set());
  const [selectAll, setSelectAll] = useState(false);
  const [selectLimit, setSelectLimit] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [showColumns, setShowColumns] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const [showAddContact, setShowAddContact] = useState(false);
  const [editingCell, setEditingCell] = useState(null);
  const [bulkField, setBulkField] = useState('');
  const [bulkAction, setBulkAction] = useState('set');
  const [bulkValue, setBulkValue] = useState('');
  const [bulkNewList, setBulkNewList] = useState('');
  const [bulkLoading, setBulkLoading] = useState(false);
  const [riCampaignId, setRiCampaignId] = useState('');
  const [riWorkspace, setRiWorkspace] = useState('US');
  const [riCampaignOptions, setRiCampaignOptions] = useState([]);
  const { data: filterOptions } = useData('/filters');
  const columnsRef = useRef(null);
  const [savedViews, setSavedViews] = useState(() => {
    const saved = localStorage.getItem('deduply_saved_views');
    return saved ? JSON.parse(saved) : [];
  });
  const [showSaveView, setShowSaveView] = useState(false);
  const [showViewsDropdown, setShowViewsDropdown] = useState(false);
  const [newViewName, setNewViewName] = useState('');
  const viewsRef = useRef(null);

  const allColumns = [
    // Basic info
    { id: 'first_name', label: 'First Name', editable: true, default: true },
    { id: 'last_name', label: 'Last Name', editable: true, default: true },
    { id: 'email', label: 'Email', editable: true, default: true },
    { id: 'company', label: 'Company', editable: true, default: true },
    { id: 'title', label: 'Title', editable: true, default: true },
    { id: 'headline', label: 'Headline', editable: true, default: false },
    { id: 'seniority', label: 'Seniority', editable: true, default: false },
    { id: 'industry', label: 'Industry', editable: true, default: false },
    // Contact info
    { id: 'first_phone', label: 'Phone', editable: true, default: false },
    { id: 'corporate_phone', label: 'Corporate Phone', editable: true, default: false },
    { id: 'person_linkedin_url', label: 'LinkedIn', editable: true, default: false },
    { id: 'company_linkedin_url', label: 'Company LinkedIn', editable: true, default: false },
    { id: 'website', label: 'Website', editable: true, default: false },
    { id: 'domain', label: 'Domain', editable: true, default: false },
    // Person location
    { id: 'city', label: 'City', editable: true, default: false },
    { id: 'state', label: 'State', editable: true, default: false },
    { id: 'country', label: 'Workspace', editable: true, default: false },
    // Company location
    { id: 'company_city', label: 'Company City', editable: true, default: false },
    { id: 'company_state', label: 'Company State', editable: true, default: false },
    { id: 'company_country', label: 'Company Country', editable: true, default: false },
    { id: 'company_street_address', label: 'Company Address', editable: true, default: false },
    { id: 'company_postal_code', label: 'Postal Code', editable: true, default: false },
    // Company details
    { id: 'employees', label: 'Employees', editable: true, default: false },
    { id: 'employee_bucket', label: 'Employee Bucket', editable: true, default: false },
    { id: 'annual_revenue', label: 'Revenue', editable: true, default: false },
    { id: 'annual_revenue_text', label: 'Revenue (Text)', editable: true, default: false },
    { id: 'company_description', label: 'Company Desc', editable: true, default: false },
    { id: 'company_seo_description', label: 'SEO Desc', editable: true, default: false },
    { id: 'company_technologies', label: 'Technologies', editable: false, default: false },
    { id: 'company_founded_year', label: 'Founded', editable: true, default: false },
    { id: 'keywords', label: 'Keywords', editable: true, default: false },
    // System fields
    { id: 'country_strategy', label: 'Strategy', editable: true, default: true, type: 'strategy' },
    { id: 'status', label: 'Status', editable: true, type: 'status', default: true },
    { id: 'email_status', label: 'Email Status', editable: true, default: false },
    { id: 'outreach_lists', label: 'Lists', editable: true, default: true },
    { id: 'campaigns_assigned', label: 'Campaigns', editable: true, default: true },
    { id: 'times_contacted', label: 'Contacted', editable: false, default: false },
    { id: 'last_contacted_at', label: 'Last Contact', editable: false, default: false },
    { id: 'notes', label: 'Notes', editable: true, default: false },
    { id: 'created_at', label: 'Created', editable: false, default: false }
  ];

  const [visibleColumns, setVisibleColumns] = useState(() => {
    const saved = localStorage.getItem('deduply_visible_columns');
    return saved ? JSON.parse(saved) : allColumns.filter(c => c.default).map(c => c.id);
  });

  useEffect(() => {
    localStorage.setItem('deduply_visible_columns', JSON.stringify(visibleColumns));
  }, [visibleColumns]);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (columnsRef.current && !columnsRef.current.contains(e.target)) setShowColumns(false);
      if (viewsRef.current && !viewsRef.current.contains(e.target)) setShowViewsDropdown(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const columns = allColumns.filter(c => visibleColumns.includes(c.id));

  // Calculate active filters count (handle both arrays and strings)
  const activeFiltersCount = Object.entries(filters).filter(([k, v]) => {
    if (Array.isArray(v)) return v.length > 0;
    return v && v !== '';
  }).length;

  // Multi-select filter options
  const statusOptions = (filterOptions?.statuses || []).map(s => ({ id: s, name: s }));
  const emailStatusOptions = [
    { id: 'Not Verified', name: 'Not Verified' },
    { id: 'Valid', name: 'Valid' },
    { id: 'Invalid', name: 'Invalid' },
    { id: 'Unknown', name: 'Unknown' }
  ];
  const countryStrategyOptions = (filterOptions?.country_strategies || []).map(c => ({ id: c, name: c }));
  const campaignFilterOptions = [{ id: '__none__', name: 'No Campaign' }, ...(filterOptions?.campaigns || []).map(c => ({ id: c, name: c }))];
  const listFilterOptions = [{ id: '__none__', name: 'No List' }, ...(filterOptions?.outreach_lists || []).map(l => ({ id: l, name: l }))];

  const { workspace } = useWorkspace();
  const fetchContacts = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page, page_size: pageSize, sort_by: sortBy, sort_order: sortOrder, workspace });
      if (search) params.append('search', search);
      // Handle both array and string filter values
      Object.entries(filters).forEach(([k, v]) => {
        if (Array.isArray(v) && v.length > 0) {
          params.append(k, v.join(','));
        } else if (v && !Array.isArray(v)) {
          params.append(k, v);
        }
      });
      const r = await api.get(`/contacts?${params}`);
      setContacts(r.data); setTotal(r.total);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  }, [page, pageSize, search, filters, sortBy, sortOrder, addToast, workspace]);

  useEffect(() => { fetchContacts(); }, [fetchContacts]);

  useEffect(() => {
    if (bulkField !== 'reachinbox_push') { setRiCampaignOptions([]); return; }
    api.get(`/reachinbox/campaigns?workspace=${riWorkspace}`)
      .then(res => setRiCampaignOptions(res.campaigns || []))
      .catch(() => setRiCampaignOptions([]));
  }, [bulkField, riWorkspace]);

  const toggleSelect = (id) => { const n = new Set(selected); n.has(id) ? n.delete(id) : n.add(id); setSelected(n); };
  const toggleSelectAll = () => { if (selectAll) { setSelected(new Set()); setSelectAll(false); } else { setSelected(new Set(contacts.map(c => c.id))); setSelectAll(true); } };

  const executeBulkAction = async () => {
    if (!bulkField) return;
    setBulkLoading(true);
    try {
      // HubSpot sync — separate flow
      if (bulkField === 'hubspot_push') {
        let contact_ids;
        if (selectAll) {
          const params = new URLSearchParams({ page: 1, page_size: selectLimitNum > 0 && selectLimitNum < total ? selectLimitNum : total, ...filters });
          if (search) params.append('search', search);
          const r = await api.get(`/contacts?${params}`);
          contact_ids = r.data.map(c => c.id);
        } else {
          contact_ids = Array.from(selected);
        }
        const result = await api.post('/hubspot/sync/bulk', { contact_ids, limit: contact_ids.length });
        addToast(`HubSpot: queued ${result.count} contacts for sync`, 'success');
        setSelected(new Set()); setSelectAll(false); setSelectLimit('');
        setBulkField('');
        setBulkLoading(false);
        return;
      }

      // ReachInbox push — separate flow
      if (bulkField === 'reachinbox_push') {
        if (!riCampaignId) { addToast('Enter a ReachInbox Campaign ID', 'error'); setBulkLoading(false); return; }
        let contact_ids;
        if (selectAll) {
          // Resolve IDs from filters + limit
          const params = new URLSearchParams({ page: 1, page_size: selectLimitNum > 0 && selectLimitNum < total ? selectLimitNum : total, ...filters });
          if (search) params.append('search', search);
          const r = await api.get(`/contacts?${params}`);
          contact_ids = r.data.map(c => c.id);
        } else {
          contact_ids = Array.from(selected);
        }
        const result = await api.post('/reachinbox/push', {
          contact_ids,
          reachinbox_campaign_id: parseInt(riCampaignId),
          workspace: riWorkspace,
          email_status_filter: ['Valid']
        });
        const s = result.stats;
        addToast(`ReachInbox: ${s.pushed} pushed, ${s.skipped_invalid_email} skipped (email), ${s.skipped_already_pushed} already in campaign, ${s.failed} failed`, 'success');
        setSelected(new Set()); setSelectAll(false); setSelectLimit('');
        setBulkField(''); setRiCampaignId(''); setRiWorkspace('US');
        fetchContacts();
        setBulkLoading(false);
        return;
      }

      if (bulkField !== 'delete' && !bulkValue && bulkAction !== 'remove') { setBulkLoading(false); return; }
      const effectiveBulkValue = bulkValue === '__new__' ? bulkNewList.trim() : bulkValue;
      const payload = {
        field: bulkField === 'delete' ? 'id' : bulkField,
        action: bulkField === 'delete' ? 'delete' : bulkAction,
        value: effectiveBulkValue
      };
      // If selectAll is true, send filters instead of IDs
      if (selectAll) {
        payload.filters = { search, ...filters };
        if (selectLimitNum > 0 && selectLimitNum < total) {
          payload.select_limit = selectLimitNum;
        }
      } else {
        payload.contact_ids = Array.from(selected);
      }
      const result = await api.post('/contacts/bulk', payload);
      addToast(`Updated ${result.updated.toLocaleString()} contacts`, 'success');
      setSelected(new Set()); setSelectAll(false); setSelectLimit('');
      setBulkField(''); setBulkAction('set'); setBulkValue(''); setBulkNewList('');
      fetchContacts();
    } catch (e) { addToast(e.message, 'error'); }
    setBulkLoading(false);
  };

  // Saved Views functions
  const saveCurrentView = () => {
    if (!newViewName.trim()) return;
    const view = { id: Date.now(), name: newViewName.trim(), filters: {...filters}, visibleColumns: [...visibleColumns], search, sortBy, sortOrder };
    const updated = [...savedViews, view];
    setSavedViews(updated);
    localStorage.setItem('deduply_saved_views', JSON.stringify(updated));
    setNewViewName(''); setShowSaveView(false);
    addToast('View saved', 'success');
  };

  const loadView = (view) => {
    // Convert old string-based filters to new array format
    const convertedFilters = {};
    const arrayFilterKeys = ['status', 'email_status', 'country_strategy', 'campaigns', 'outreach_lists'];
    Object.entries(view.filters || {}).forEach(([key, value]) => {
      if (arrayFilterKeys.includes(key)) {
        // Convert string to array if needed
        if (typeof value === 'string' && value) {
          convertedFilters[key] = [value];
        } else if (Array.isArray(value)) {
          convertedFilters[key] = value;
        } else {
          convertedFilters[key] = [];
        }
      } else {
        convertedFilters[key] = value;
      }
    });
    setFilters(convertedFilters);
    setVisibleColumns(view.visibleColumns || allColumns.filter(c => c.default).map(c => c.id));
    setSearch(view.search || '');
    if (view.sortBy) setSortBy(view.sortBy);
    if (view.sortOrder) setSortOrder(view.sortOrder);
    setPage(1);
    setShowViewsDropdown(false);
    addToast(`Loaded view: ${view.name}`, 'success');
  };

  const updateView = (viewId) => {
    const updated = savedViews.map(v =>
      v.id === viewId
        ? { ...v, filters: {...filters}, visibleColumns: [...visibleColumns], search, sortBy, sortOrder }
        : v
    );
    setSavedViews(updated);
    localStorage.setItem('deduply_saved_views', JSON.stringify(updated));
    addToast('View updated', 'success');
  };

  const deleteView = (viewId) => {
    const updated = savedViews.filter(v => v.id !== viewId);
    setSavedViews(updated);
    localStorage.setItem('deduply_saved_views', JSON.stringify(updated));
    addToast('View deleted', 'success');
  };

  // Bulk editable fields config
  const bulkEditableFields = [
    { id: 'status', label: 'Status', type: 'select', options: filterOptions?.statuses || [] },
    { id: 'email_status', label: 'Email Status', type: 'select', options: ['Not Verified', 'Valid', 'Invalid', 'Unknown'] },
    { id: 'country_strategy', label: 'Workspace', type: 'select', options: filterOptions?.country_strategies || [] },
    { id: 'campaigns_assigned', label: 'Campaign', type: 'list', options: filterOptions?.campaigns || [] },
    { id: 'outreach_lists', label: 'Outreach List', type: 'list', options: filterOptions?.outreach_lists || [] },
    { id: 'seniority', label: 'Seniority', type: 'select', options: filterOptions?.seniorities || [] },
    { id: 'industry', label: 'Industry', type: 'select', options: filterOptions?.industries || [] },
    { id: 'delete', label: 'Delete Contacts', type: 'action' },
    { id: 'reachinbox_push', label: 'Push to ReachInbox', type: 'reachinbox' },
    { id: 'hubspot_push', label: 'Sync to HubSpot', type: 'action' }
  ];

  const selectedBulkField = bulkEditableFields.find(f => f.id === bulkField);

  const saveEdit = async (contactId, field, value) => {
    try { await api.put(`/contacts/${contactId}`, { [field]: value }); fetchContacts(); addToast('Updated', 'success'); } catch (e) { addToast(e.message, 'error'); }
    setEditingCell(null);
  };

  const handleSort = (col) => { if (sortBy === col) setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc'); else { setSortBy(col); setSortOrder('desc'); } };

  const selectLimitNum = selectLimit ? parseInt(selectLimit) : 0;
  const selectedCount = selectAll ? (selectLimitNum > 0 && selectLimitNum < total ? selectLimitNum : total) : selected.size;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (<div className="page">
    <div className="page-header"><div><h1>Contacts</h1><p className="subtitle">{total.toLocaleString()} contacts</p></div>
      <div className="header-actions"><button className="btn btn-secondary" onClick={() => setShowImport(true)}><Upload size={16} /> Import</button><button className="btn btn-secondary" onClick={() => setShowExport(true)}><Download size={16} /> Export</button><button className="btn btn-primary" onClick={() => setShowAddContact(true)}><Plus size={16} /> Add Contact</button></div></div>

    <div className="toolbar">
      <div className="search-box"><Search size={18} /><input type="text" placeholder="Search by name, email, company, title..." value={search} onChange={e => { setSearch(e.target.value); setPage(1); }} /></div>
      <div className="toolbar-actions">
        <button className={`btn btn-icon ${showFilters ? 'active' : ''}`} onClick={() => setShowFilters(!showFilters)} title="Filters">
          <Filter size={18} />{activeFiltersCount > 0 && <span className="filter-badge">{activeFiltersCount}</span>}
        </button>
        <div className="columns-dropdown-wrapper" ref={columnsRef}>
          <button className="btn btn-icon" onClick={() => setShowColumns(!showColumns)} title="Columns"><Layers size={18} /></button>
          {showColumns && (
            <div className="columns-dropdown">
              <div className="columns-dropdown-header">
                <span>Visible Columns</span>
                <button className="btn btn-text" onClick={() => setVisibleColumns(allColumns.filter(c => c.default).map(c => c.id))}>Reset</button>
              </div>
              <div className="columns-dropdown-list">
                {allColumns.map(col => (
                  <label key={col.id} className="columns-dropdown-item">
                    <input type="checkbox" checked={visibleColumns.includes(col.id)}
                      onChange={e => setVisibleColumns(e.target.checked ? [...visibleColumns, col.id] : visibleColumns.filter(id => id !== col.id))} />
                    <span>{col.label}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
        <div className="views-dropdown-wrapper" ref={viewsRef}>
          <button className={`btn btn-icon ${showViewsDropdown ? 'active' : ''}`} onClick={() => setShowViewsDropdown(!showViewsDropdown)} title="Saved Views">
            <Eye size={18} />{savedViews.length > 0 && <span className="filter-badge">{savedViews.length}</span>}
          </button>
          {showViewsDropdown && (
            <div className="views-dropdown">
              <div className="views-dropdown-header">
                <span>Saved Views</span>
              </div>
              {showSaveView ? (
                <div className="views-save-form">
                  <input type="text" placeholder="View name..." value={newViewName} onChange={e => setNewViewName(e.target.value)} onKeyDown={e => { if (e.key === 'Enter' && newViewName.trim()) { saveCurrentView(); setShowSaveView(false); } if (e.key === 'Escape') setShowSaveView(false); }} autoFocus />
                  <div className="views-save-actions">
                    <button className="btn btn-primary btn-sm" onClick={() => { saveCurrentView(); setShowSaveView(false); }} disabled={!newViewName.trim()}>Save</button>
                    <button className="btn btn-text btn-sm" onClick={() => setShowSaveView(false)}>Cancel</button>
                  </div>
                </div>
              ) : (
                <>
                  <div className="views-dropdown-list">
                    {savedViews.length === 0 ? (
                      <div className="views-empty">No saved views yet</div>
                    ) : savedViews.map(v => (
                      <div key={v.id} className="views-dropdown-item">
                        <button className="view-load-btn" onClick={() => loadView(v)}>
                          <Eye size={14} /><span>{v.name}</span>
                        </button>
                        <div className="view-item-actions">
                          <button className="view-action-btn" onClick={() => updateView(v.id)} title="Update with current filters"><RefreshCw size={13} /></button>
                          <button className="view-action-btn danger" onClick={() => deleteView(v.id)} title="Delete view"><Trash2 size={13} /></button>
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className="views-dropdown-footer">
                    <button className="btn btn-sm btn-block" onClick={() => setShowSaveView(true)}><Plus size={14} /> Save Current View</button>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
        <button className="btn btn-icon" onClick={fetchContacts} title="Refresh"><RefreshCw size={18} /></button>
      </div>
    </div>

    {showFilters && filterOptions && (
      <div className="filters-panel-v2">
        <div className="filters-grid">
          <div className="filter-group-v2">
            <label>Status</label>
            <MultiSelect
              options={statusOptions}
              value={filters.status || []}
              onChange={v => setFilters({ ...filters, status: v })}
              placeholder="All Statuses"
            />
          </div>
          <div className="filter-group-v2">
            <label>Email Status</label>
            <MultiSelect
              options={emailStatusOptions}
              value={filters.email_status || []}
              onChange={v => setFilters({ ...filters, email_status: v })}
              placeholder="All Email Statuses"
            />
          </div>
          <div className="filter-group-v2">
            <label>Workspace</label>
            <MultiSelect
              options={countryStrategyOptions}
              value={filters.country_strategy || []}
              onChange={v => setFilters({ ...filters, country_strategy: v })}
              placeholder="All Strategies"
            />
          </div>
          <div className="filter-group-v2">
            <label>Campaign</label>
            <MultiSelect
              options={campaignFilterOptions}
              value={filters.campaigns || []}
              onChange={v => setFilters({ ...filters, campaigns: v })}
              placeholder="All Campaigns"
            />
          </div>
          <div className="filter-group-v2">
            <label>Outreach List</label>
            <MultiSelect
              options={listFilterOptions}
              value={filters.outreach_lists || []}
              onChange={v => setFilters({ ...filters, outreach_lists: v })}
              placeholder="All Lists"
            />
          </div>
          <div className="filter-group-v2">
            <label>Country</label>
            <select value={filters.country || ''} onChange={e => setFilters({ ...filters, country: e.target.value })}>
              <option value="">All Countries</option>
              {filterOptions.countries?.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div className="filter-group-v2">
            <label>Seniority</label>
            <select value={filters.seniority || ''} onChange={e => setFilters({ ...filters, seniority: e.target.value })}>
              <option value="">All Seniorities</option>
              {filterOptions.seniorities?.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div className="filter-group-v2">
            <label>Industry</label>
            <select value={filters.industry || ''} onChange={e => setFilters({ ...filters, industry: e.target.value })}>
              <option value="">All Industries</option>
              {filterOptions.industries?.map(i => <option key={i} value={i}>{i}</option>)}
            </select>
          </div>
          <div className="filter-group-v2">
            <label>Keywords</label>
            <input
              type="text"
              value={filters.keywords || ''}
              onChange={e => setFilters({ ...filters, keywords: e.target.value })}
              placeholder="roof,roofing,solar..."
              className="filter-keywords-input"
            />
          </div>
          <div className="filter-group-v2">
            <label>Workspace</label>
            <select value={filters.workspace || ''} onChange={e => setFilters({ ...filters, workspace: e.target.value })}>
              <option value="">All Workspaces</option>
              <option value="US">🇺🇸 US</option>
              <option value="MX">🇲🇽 MX</option>
            </select>
          </div>
        </div>
        {activeFiltersCount > 0 && (
          <div className="active-filters-bar">
            <span className="active-filters-label">Active filters:</span>
            <div className="filter-chips">
              {(filters.status || []).map(s => (
                <span key={`status-${s}`} className="filter-chip">
                  <span className="filter-chip-type">Status:</span> {s}
                  <X size={14} onClick={() => setFilters({ ...filters, status: filters.status.filter(x => x !== s) })} />
                </span>
              ))}
              {(filters.email_status || []).map(s => (
                <span key={`email-${s}`} className="filter-chip">
                  <span className="filter-chip-type">Email:</span> {s}
                  <X size={14} onClick={() => setFilters({ ...filters, email_status: filters.email_status.filter(x => x !== s) })} />
                </span>
              ))}
              {(filters.country_strategy || []).map(s => (
                <span key={`strategy-${s}`} className="filter-chip">
                  <span className="filter-chip-type">Strategy:</span> {s}
                  <X size={14} onClick={() => setFilters({ ...filters, country_strategy: filters.country_strategy.filter(x => x !== s) })} />
                </span>
              ))}
              {(filters.campaigns || []).map(s => (
                <span key={`campaign-${s}`} className="filter-chip">
                  <span className="filter-chip-type">Campaign:</span> {s}
                  <X size={14} onClick={() => setFilters({ ...filters, campaigns: filters.campaigns.filter(x => x !== s) })} />
                </span>
              ))}
              {(filters.outreach_lists || []).map(s => (
                <span key={`list-${s}`} className="filter-chip">
                  <span className="filter-chip-type">List:</span> {s}
                  <X size={14} onClick={() => setFilters({ ...filters, outreach_lists: filters.outreach_lists.filter(x => x !== s) })} />
                </span>
              ))}
              {filters.country && (
                <span className="filter-chip">
                  <span className="filter-chip-type">Country:</span> {filters.country}
                  <X size={14} onClick={() => setFilters({ ...filters, country: '' })} />
                </span>
              )}
              {filters.seniority && (
                <span className="filter-chip">
                  <span className="filter-chip-type">Seniority:</span> {filters.seniority}
                  <X size={14} onClick={() => setFilters({ ...filters, seniority: '' })} />
                </span>
              )}
              {filters.industry && (
                <span className="filter-chip">
                  <span className="filter-chip-type">Industry:</span> {filters.industry}
                  <X size={14} onClick={() => setFilters({ ...filters, industry: '' })} />
                </span>
              )}
              {filters.keywords && (
                <span className="filter-chip">
                  <span className="filter-chip-type">Keywords:</span> {filters.keywords}
                  <X size={14} onClick={() => setFilters({ ...filters, keywords: '' })} />
                </span>
              )}
              {filters.workspace && (
                <span className="filter-chip">
                  <span className="filter-chip-type">Workspace:</span> {filters.workspace === 'MX' ? '🇲🇽 MX' : '🇺🇸 US'}
                  <X size={14} onClick={() => setFilters({ ...filters, workspace: '' })} />
                </span>
              )}
            </div>
            <button className="btn btn-text btn-clear-filters" onClick={() => setFilters({})}>
              <X size={14} /> Clear All
            </button>
          </div>
        )}
      </div>
    )}

    {selectedCount > 0 && (<div className="bulk-bar">
      <span className="bulk-count">
        {selectedCount.toLocaleString()} selected
        {selectAll && !selectLimitNum && <span className="bulk-all-note"> (all matching filters)</span>}
        {selectAll && selectLimitNum > 0 && selectLimitNum < total && <span className="bulk-all-note"> of {total.toLocaleString()}</span>}
      </span>
      {selectAll && (
        <div className="bulk-limit-input">
          <span className="bulk-limit-label">Limit to</span>
          <input
            type="number"
            value={selectLimit}
            onChange={e => setSelectLimit(e.target.value)}
            placeholder={total.toLocaleString()}
            min="1"
            max={total}
            className="select-limit-input"
          />
          <span className="bulk-limit-label">contacts</span>
        </div>
      )}
      <div className="bulk-actions">
        <div className="bulk-group">
          <select value={bulkField} onChange={e => { setBulkField(e.target.value); setBulkValue(''); setBulkAction('set'); setBulkNewList(''); setRiCampaignId(''); setRiWorkspace('US'); }} className="bulk-field-select">
            <option value="">Select field to edit...</option>
            {bulkEditableFields.map(f => <option key={f.id} value={f.id}>{f.label}</option>)}
          </select>
          {selectedBulkField && selectedBulkField.type === 'list' && (
            <select value={bulkAction} onChange={e => setBulkAction(e.target.value)} className="bulk-action-select">
              <option value="add">Add to</option>
              <option value="remove">Remove from</option>
              <option value="set">Set to only</option>
            </select>
          )}
          {selectedBulkField && selectedBulkField.type !== 'action' && (
            <>
              <select value={bulkValue} onChange={e => { setBulkValue(e.target.value); if (e.target.value !== '__new__') setBulkNewList(''); }} className="bulk-value-select">
                <option value="">Select value...</option>
                {selectedBulkField.options.map(o => <option key={o} value={o}>{o}</option>)}
                {selectedBulkField.id === 'outreach_lists' && <option value="__new__">+ Create new list...</option>}
              </select>
              {bulkValue === '__new__' && (
                <input
                  type="text"
                  value={bulkNewList}
                  onChange={e => setBulkNewList(e.target.value)}
                  placeholder="New list name..."
                  className="bulk-new-list-input"
                  autoFocus
                />
              )}
            </>
          )}
          {selectedBulkField && selectedBulkField.type === 'action' && (
            <span className="bulk-warning"><AlertTriangle size={16} /> This will permanently delete {selectedCount.toLocaleString()} contacts</span>
          )}
          {selectedBulkField && selectedBulkField.type === 'reachinbox' && (
            <div className="ri-push-inputs">
              {riCampaignOptions.length > 0 ? (
                <select value={riCampaignId} onChange={e => setRiCampaignId(e.target.value)} className="ri-campaign-input">
                  <option value="">— Select RI Campaign —</option>
                  {riCampaignOptions.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                </select>
              ) : (
                <input
                  type="number"
                  value={riCampaignId}
                  onChange={e => setRiCampaignId(e.target.value)}
                  placeholder="ReachInbox Campaign ID"
                  className="ri-campaign-input"
                  min="1"
                />
              )}
              <select value={riWorkspace} onChange={e => setRiWorkspace(e.target.value)} className="ri-workspace-select">
                <option value="US">US Workspace</option>
                <option value="MX">MX Workspace</option>
              </select>
              <span className="ri-push-hint">Only contacts with Valid emails will be pushed</span>
            </div>
          )}
          <button className="btn btn-primary" onClick={executeBulkAction} disabled={
            !bulkField || bulkLoading ||
            (bulkField === 'reachinbox_push' && !riCampaignId) ||
            (bulkField !== 'delete' && bulkField !== 'reachinbox_push' && bulkField !== 'hubspot_push' && (!bulkValue || (bulkValue === '__new__' && !bulkNewList.trim())))
          }>
            {bulkLoading ? <Loader2 className="spin" size={16} /> : bulkField === 'reachinbox_push' ? <><Send size={14} /> Push</> : bulkField === 'hubspot_push' ? <><Zap size={14} /> Sync to HubSpot</> : 'Apply'}
          </button>
        </div>
      </div>
      <button className="btn btn-text" onClick={() => { setSelected(new Set()); setSelectAll(false); setBulkField(''); setSelectLimit(''); setBulkNewList(''); setRiCampaignId(''); setRiWorkspace('US'); }}><X size={14} /> Clear</button>
    </div>)}

    <div className="table-wrapper"><table className="data-table"><thead><tr><th className="col-checkbox"><input type="checkbox" checked={selectAll || (contacts.length > 0 && selected.size === contacts.length)} onChange={toggleSelectAll} /></th>{columns.map(col => (<th key={col.id} className="sortable" onClick={() => handleSort(col.id)}>{col.label}{sortBy === col.id && <ArrowUpDown size={12} />}</th>))}<th className="col-actions"></th></tr></thead>
      <tbody>{loading ? (<tr><td colSpan={columns.length + 2} className="loading-cell"><Loader2 className="spin" size={24} /></td></tr>) : contacts.length === 0 ? (<tr><td colSpan={columns.length + 2} className="empty-cell">No contacts found</td></tr>) : contacts.map(contact => (
        <tr key={contact.id} className={selected.has(contact.id) ? 'selected' : ''}><td className="col-checkbox"><input type="checkbox" checked={selected.has(contact.id)} onChange={() => toggleSelect(contact.id)} /></td>
          {columns.map(col => (<td key={col.id}>{editingCell?.id === contact.id && editingCell?.field === col.id ? (
            col.type === 'status' ? <select autoFocus value={editingCell.value} onChange={e => setEditingCell({...editingCell, value: e.target.value})} onBlur={() => saveEdit(contact.id, col.id, editingCell.value)}>{filterOptions?.statuses?.map(s => <option key={s} value={s}>{s}</option>)}</select>
            : col.type === 'strategy' ? <select autoFocus value={editingCell.value} onChange={e => setEditingCell({...editingCell, value: e.target.value})} onBlur={() => saveEdit(contact.id, col.id, editingCell.value)}><option value="">—</option>{filterOptions?.country_strategies?.map(s => <option key={s} value={s}>{s}</option>)}</select>
            : <input autoFocus type="text" value={editingCell.value} onChange={e => setEditingCell({...editingCell, value: e.target.value})} onBlur={() => saveEdit(contact.id, col.id, editingCell.value)} onKeyDown={e => { if (e.key === 'Enter') saveEdit(contact.id, col.id, editingCell.value); if (e.key === 'Escape') setEditingCell(null); }} className="cell-input" />
          ) : col.type === 'status' ? <span className={`status-badge status-${contact[col.id]?.toLowerCase().replace(/ /g, '-')}`} onClick={() => col.editable && setEditingCell({ id: contact.id, field: col.id, value: contact[col.id] || '' })}>{contact[col.id] || '—'}</span>
          : col.type === 'strategy' ? <span className={`strategy-badge strategy-${contact[col.id]?.toLowerCase().replace(/ /g, '-') || 'none'}`} onClick={() => col.editable && setEditingCell({ id: contact.id, field: col.id, value: contact[col.id] || '' })}>{contact[col.id] || '—'}</span>
          : col.id === 'email' ? <a href={`mailto:${contact[col.id]}`} className="email-link">{contact[col.id]}</a>
          : col.id === 'email_status' ? <span className={`email-status-badge ${(contact[col.id] || 'not-verified').toLowerCase().replace(/ /g, '-')}`}>{contact[col.id] || 'Not Verified'}</span>
          : col.id === 'first_name' ? <span className="cell-editable" onClick={() => col.editable && setEditingCell({ id: contact.id, field: col.id, value: contact[col.id] || '' })}>{contact[col.id] || '—'}</span>
          : (col.id === 'outreach_lists' || col.id === 'campaigns_assigned') ? (!contact[col.id] ? <span className="cell-empty" onClick={() => col.editable && setEditingCell({ id: contact.id, field: col.id, value: '' })}>—</span> : <div className="tags-cell" onClick={() => col.editable && setEditingCell({ id: contact.id, field: col.id, value: contact[col.id] })}>{contact[col.id].split(',').slice(0, 2).map((t, i) => <span key={i} className="tag">{t.trim()}</span>)}{contact[col.id].split(',').length > 2 && <span className="tag tag-more">+{contact[col.id].split(',').length - 2}</span>}</div>)
          : <span className="cell-editable" onClick={() => col.editable && setEditingCell({ id: contact.id, field: col.id, value: contact[col.id] || '' })}>{contact[col.id] || '—'}</span>}</td>))}
          <td className="col-actions"><button className="btn-icon-small danger" onClick={async () => { if (window.confirm('Delete?')) { await api.delete(`/contacts/${contact.id}`); fetchContacts(); addToast('Deleted', 'success'); } }}><Trash2 size={14} /></button></td></tr>))}</tbody></table></div>

    <div className="pagination"><div>Showing {((page - 1) * pageSize) + 1} - {Math.min(page * pageSize, total)} of {total.toLocaleString()}</div>
      <div className="pagination-controls"><button onClick={() => setPage(1)} disabled={page <= 1}><ChevronLeft size={14} /><ChevronLeft size={14} /></button><button onClick={() => setPage(page - 1)} disabled={page <= 1}><ChevronLeft size={14} /></button><span className="pagination-current">Page {page} of {totalPages}</span><button onClick={() => setPage(page + 1)} disabled={page >= totalPages}><ChevronRight size={14} /></button><button onClick={() => setPage(totalPages)} disabled={page >= totalPages}><ChevronRight size={14} /><ChevronRight size={14} /></button></div>
      <select value={pageSize} onChange={e => { setPageSize(parseInt(e.target.value)); setPage(1); }}><option value={25}>25 / page</option><option value={50}>50 / page</option><option value={100}>100 / page</option></select></div>

    <Modal isOpen={showImport} onClose={() => setShowImport(false)} title="Import Contacts" size="xl"><ImportWizard onSuccess={() => { setShowImport(false); fetchContacts(); addToast('Import complete', 'success'); }} filterOptions={filterOptions} /></Modal>
    <Modal isOpen={showExport} onClose={() => setShowExport(false)} title="Export Contacts"><ExportForm filters={filters} search={search} onClose={() => setShowExport(false)} /></Modal>
    <Modal isOpen={showAddContact} onClose={() => setShowAddContact(false)} title="Add Contact" size="lg"><AddContactForm onSuccess={() => { setShowAddContact(false); fetchContacts(); addToast('Contact created', 'success'); }} /></Modal>
  </div>);
};

// Import Wizard
const ImportWizard = ({ onSuccess, filterOptions }) => {
  const { addToast } = useToast();
  const { startImportJob } = useImportJob();
  const [step, setStep] = useState(1);
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [mapping, setMapping] = useState({});
  const [outreachList, setOutreachList] = useState('');
  const [newListName, setNewListName] = useState('');
  const [selectedCampaign, setSelectedCampaign] = useState('');
  const [newCampaignName, setNewCampaignName] = useState('');
  const [countryStrategy, setCountryStrategy] = useState('');
  const [checkDuplicates, setCheckDuplicates] = useState(true);
  const [mergeDuplicates, setMergeDuplicates] = useState(true);
  const [loading, setLoading] = useState(false);

  // Fixed strategy options
  const strategyOptions = ['United States', 'Mexico', 'Spain', 'Germany'];

  const handleFileSelect = async (e) => {
    const f = e.target.files[0];
    if (!f) return;
    setFile(f); setLoading(true);
    const formData = new FormData(); formData.append('file', f);
    try {
      const res = await fetch(`${API}/import/preview`, { method: 'POST', headers: api.token ? { 'Authorization': `Bearer ${api.token}` } : {}, body: formData });
      if (!res.ok) throw new Error('Failed to read file');
      const data = await res.json();
      setPreview(data); setMapping(data.suggested_mapping || {}); setStep(2);
    } catch (e) { addToast(e.message || 'Failed to read file', 'error'); }
    setLoading(false);
  };

  const executeImport = async () => {
    setLoading(true);
    const formData = new FormData(); formData.append('file', file);
    const params = new URLSearchParams({
      column_mapping: JSON.stringify(mapping),
      outreach_list: newListName || outreachList || '',
      campaigns: newCampaignName || selectedCampaign || '',
      country_strategy: countryStrategy || '',
      check_duplicates: checkDuplicates,
      merge_duplicates: mergeDuplicates
    });
    try {
      const res = await fetch(`${API}/import/execute?${params}`, { method: 'POST', headers: api.token ? { 'Authorization': `Bearer ${api.token}` } : {}, body: formData });
      const data = await res.json();
      if (data.job_id) {
        // Start tracking the job globally
        startImportJob({
          id: data.job_id,
          status: 'pending',
          total_rows: data.total_rows,
          processed_count: 0,
          file_name: file.name
        });
        addToast(`Import started: ${data.total_rows} rows`, 'info');
        onSuccess(); // Close modal immediately
      } else {
        throw new Error('Failed to start import job');
      }
    } catch (e) { addToast('Import failed to start', 'error'); }
    setLoading(false);
  };

  const targetColumns = ['', 'first_name', 'last_name', 'email', 'title', 'headline', 'company', 'seniority', 'first_phone', 'corporate_phone', 'employees', 'employee_bucket', 'industry', 'keywords', 'person_linkedin_url', 'website', 'domain', 'company_linkedin_url',
    // Person location
    'city', 'state', 'country',
    // Company location
    'company_city', 'company_state', 'company_country', 'company_street_address', 'company_postal_code',
    // Company details
    'annual_revenue', 'annual_revenue_text', 'company_description', 'company_seo_description', 'company_technologies', 'company_founded_year',
    // System fields
    'country_strategy', 'outreach_lists', 'campaigns_assigned', 'notes', 'email_status'];

  return (<div>
    <div className="import-steps"><div className={`import-step ${step >= 1 ? (step > 1 ? 'done' : 'active') : ''}`}><span className="import-step-num">1</span> Upload</div><div className={`import-step ${step >= 2 ? (step > 2 ? 'done' : 'active') : ''}`}><span className="import-step-num">2</span> Map Columns</div><div className={`import-step ${step >= 3 ? (step > 3 ? 'done' : 'active') : ''}`}><span className="import-step-num">3</span> Options</div><div className={`import-step ${step >= 4 ? 'active' : ''}`}><span className="import-step-num">4</span> Complete</div></div>

    {step === 1 && (<div><div className="upload-area" onClick={() => document.getElementById('csv-input').click()}><Upload size={40} /><span>{loading ? 'Reading file...' : 'Click to select CSV file'}</span></div><input id="csv-input" type="file" accept=".csv" onChange={handleFileSelect} style={{ display: 'none' }} /></div>)}

    {step === 2 && preview && (<div><p style={{ marginBottom: 16 }}>Map your CSV columns to contact fields. Found <strong>{preview.total_rows.toLocaleString()}</strong> rows.</p>
      <div className="mapping-table"><div className="mapping-row"><span>CSV Column</span><span className="mapping-arrow"></span><span>Maps To</span></div>{preview.columns.map(col => (<div key={col} className="mapping-row"><span>{col}</span><span className="mapping-arrow"><ArrowRight size={16} /></span><select value={mapping[col] || ''} onChange={e => setMapping({ ...mapping, [col]: e.target.value })}>{targetColumns.map(t => <option key={t} value={t}>{t || '— Skip —'}</option>)}</select></div>))}</div>
      <div className="modal-actions"><button className="btn btn-secondary" onClick={() => setStep(1)}>Back</button><button className="btn btn-primary" onClick={() => setStep(3)}>Next</button></div></div>)}

    {step === 3 && (<div><div className="import-options">
      <div className="form-group"><label>Assign to Outreach List</label><select value={outreachList} onChange={e => setOutreachList(e.target.value)}><option value="">Select existing list...</option>{filterOptions?.outreach_lists?.map(l => <option key={l} value={l}>{l}</option>)}</select></div>
      <div className="form-group"><label>Or Create New List</label><input type="text" value={newListName} onChange={e => setNewListName(e.target.value)} placeholder="New list name..." /></div>
      <div className="form-group"><label>Assign to Campaign</label><select value={selectedCampaign} onChange={e => setSelectedCampaign(e.target.value)}><option value="">Select existing campaign...</option>{filterOptions?.campaigns?.map(c => <option key={c} value={c}>{c}</option>)}</select></div>
      <div className="form-group"><label>Or Create New Campaign</label><input type="text" value={newCampaignName} onChange={e => setNewCampaignName(e.target.value)} placeholder="New campaign name..." /></div>
      <div className="form-group"><label>Workspace *</label><select value={countryStrategy} onChange={e => setCountryStrategy(e.target.value)} required><option value="">Select strategy...</option>{strategyOptions.map(s => <option key={s} value={s}>{s}</option>)}</select>{!countryStrategy && <span className="field-hint error">Required</span>}</div>
      <div></div>
      <div className="import-option"><label><input type="checkbox" checked={checkDuplicates} onChange={e => setCheckDuplicates(e.target.checked)} /> Check for duplicates</label></div>
      <div className="import-option"><label><input type="checkbox" checked={mergeDuplicates} onChange={e => setMergeDuplicates(e.target.checked)} disabled={!checkDuplicates} /> Merge duplicates (add to existing lists/campaigns)</label></div>
      </div>
      <div className="modal-actions"><button className="btn btn-secondary" onClick={() => setStep(2)}>Back</button><button className="btn btn-primary" onClick={executeImport} disabled={loading || !countryStrategy}>{loading ? <><Loader2 className="spin" size={16} /> Importing...</> : 'Import'}</button></div></div>)}

    {step === 4 && result && (<div style={{ textAlign: 'center', padding: 40 }}>
      <CheckCircle size={60} style={{ color: 'var(--success)', marginBottom: 20 }} />
      <h3 style={{ marginBottom: 20 }}>Import Complete!</h3>
      <div style={{ fontSize: 16, lineHeight: 2 }}>
        <p><strong>{result.imported}</strong> contacts imported</p>
        <p><strong>{result.merged}</strong> contacts merged</p>
        {result.duplicates_found > 0 && <p><strong>{result.duplicates_found}</strong> duplicates found</p>}
        {result.failed > 0 && <p style={{ color: 'var(--error)' }}><strong>{result.failed}</strong> failed</p>}
      </div>

      {/* Background Verification Progress */}
      {result.verification_job_id && verificationJob && (
        <div className="verification-progress-card">
          <h4><Mail size={16} /> Email Verification {verificationJob.status === 'running' ? 'In Progress' : verificationJob.status === 'completed' ? 'Complete' : verificationJob.status === 'cancelled' ? 'Cancelled' : 'Status'}</h4>

          {/* Progress Bar */}
          {verificationJob.status === 'running' && (
            <div className="verification-progress-bar">
              <div
                className="verification-progress-fill"
                style={{ width: `${verificationJob.total_contacts > 0 ? ((verificationJob.verified_count + verificationJob.skipped_count) / verificationJob.total_contacts * 100) : 0}%` }}
              />
            </div>
          )}

          {/* Current Status */}
          {verificationJob.status === 'running' && (
            <div className="verification-current">
              <Loader2 className="spin" size={14} />
              <span>Verifying: {verificationJob.current_email || '...'}</span>
            </div>
          )}

          {/* Stats Grid */}
          <div className="verification-stats">
            <div className="verification-stat">
              <div className="stat-value">{verificationJob.verified_count + verificationJob.skipped_count}/{verificationJob.total_contacts}</div>
              <div className="stat-label">Progress</div>
            </div>
            <div className="verification-stat valid">
              <div className="stat-value">{verificationJob.valid_count}</div>
              <div className="stat-label">Valid</div>
            </div>
            <div className="verification-stat invalid">
              <div className="stat-value">{verificationJob.invalid_count}</div>
              <div className="stat-label">Invalid</div>
            </div>
            <div className="verification-stat">
              <div className="stat-value">{verificationJob.unknown_count}</div>
              <div className="stat-label">Unknown</div>
            </div>
          </div>

          {verificationJob.skipped_count > 0 && (
            <p style={{ fontSize: 12, color: 'var(--text-3)', marginTop: 8 }}>{verificationJob.skipped_count} already verified (skipped)</p>
          )}

          {verificationJob.status === 'failed' && verificationJob.error_message && (
            <p style={{ color: 'var(--error)', marginTop: 8 }}><AlertCircle size={14} /> {verificationJob.error_message}</p>
          )}

          {/* Cancel Button */}
          {verificationJob.status === 'running' && (
            <button
              className="btn btn-secondary btn-sm"
              onClick={async () => {
                try {
                  await api.post(`/verify/job/${result.verification_job_id}/cancel`);
                } catch (e) { console.error('Failed to cancel:', e); }
              }}
              style={{ marginTop: 12 }}
            >
              <X size={14} /> Cancel Verification
            </button>
          )}
        </div>
      )}

      {/* No verification job but contacts could be verified */}
      {result.contacts_to_verify > 0 && !result.verification_job_id && (
        <p style={{ fontSize: 14, color: 'var(--text-3)', marginTop: 16 }}>{result.contacts_to_verify} contacts ready for verification</p>
      )}

      <button className="btn btn-primary" onClick={onSuccess} style={{ marginTop: 20 }}>Done</button>
    </div>)}
  </div>);
};

// Export Form
const ExportForm = ({ filters, search, onClose }) => {
  const allColumns = [
    { id: 'first_name', label: 'First Name' }, { id: 'last_name', label: 'Last Name' }, { id: 'email', label: 'Email' },
    { id: 'title', label: 'Title' }, { id: 'headline', label: 'Headline' }, { id: 'company', label: 'Company' }, { id: 'seniority', label: 'Seniority' },
    { id: 'first_phone', label: 'Phone' }, { id: 'corporate_phone', label: 'Corporate Phone' },
    { id: 'person_linkedin_url', label: 'LinkedIn' }, { id: 'company_linkedin_url', label: 'Company LinkedIn' },
    { id: 'website', label: 'Website' }, { id: 'domain', label: 'Domain' },
    { id: 'city', label: 'City' }, { id: 'state', label: 'State' }, { id: 'country', label: 'Workspace' },
    { id: 'company_city', label: 'Company City' }, { id: 'company_state', label: 'Company State' }, { id: 'company_country', label: 'Company Country' },
    { id: 'company_street_address', label: 'Company Address' }, { id: 'company_postal_code', label: 'Postal Code' },
    { id: 'employees', label: 'Employees' }, { id: 'employee_bucket', label: 'Employee Bucket' }, { id: 'industry', label: 'Industry' },
    { id: 'annual_revenue', label: 'Revenue' }, { id: 'annual_revenue_text', label: 'Revenue (Text)' },
    { id: 'company_description', label: 'Company Desc' }, { id: 'company_seo_description', label: 'SEO Desc' },
    { id: 'company_technologies', label: 'Technologies' }, { id: 'company_founded_year', label: 'Founded' }, { id: 'keywords', label: 'Keywords' },
    { id: 'country_strategy', label: 'Workspace' }, { id: 'outreach_lists', label: 'Outreach Lists' }, { id: 'campaigns_assigned', label: 'Campaigns' },
    { id: 'status', label: 'Status' }, { id: 'email_status', label: 'Email Status' }, { id: 'times_contacted', label: 'Contacted' },
    { id: 'last_contacted_at', label: 'Last Contact' }, { id: 'notes', label: 'Notes' }, { id: 'created_at', label: 'Created' }
  ];
  const [selected, setSelected] = useState(allColumns.slice(0, 10).map(c => c.id));
  const [validEmailsOnly, setValidEmailsOnly] = useState(false);
  const handleExport = () => { const params = new URLSearchParams(); if (selected.length) params.append('columns', selected.join(',')); if (search) params.append('search', search); Object.entries(filters).forEach(([k, v]) => { if (v) params.append(k, v); }); if (validEmailsOnly) params.append('valid_emails_only', 'true'); window.open(`${API}/contacts/export?${params}`, '_blank'); onClose(); };
  return (<div>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
      <p style={{ margin: 0 }}>Select columns to include:</p>
      <div style={{ display: 'flex', gap: 8 }}>
        <button className="btn btn-text" onClick={() => setSelected(allColumns.map(c => c.id))}>Select All</button>
        <button className="btn btn-text" onClick={() => setSelected([])}>Clear All</button>
      </div>
    </div>
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 8, marginBottom: 20, maxHeight: 300, overflowY: 'auto' }}>
      {allColumns.map(col => (<label key={col.id} style={{ display: 'flex', alignItems: 'center', gap: 8 }}><input type="checkbox" checked={selected.includes(col.id)} onChange={e => { if (e.target.checked) setSelected([...selected, col.id]); else setSelected(selected.filter(c => c !== col.id)); }} />{col.label}</label>))}
    </div>
    <div className="export-option" style={{ padding: '16px', background: 'var(--bg-3)', borderRadius: '8px', marginBottom: '20px' }}>
      <label style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer', fontWeight: 500 }}>
        <input type="checkbox" checked={validEmailsOnly} onChange={e => setValidEmailsOnly(e.target.checked)} />
        <CheckCircle size={14} style={{ color: 'var(--success)' }} /> Export only verified valid emails
      </label>
      <span style={{ display: 'block', fontSize: '12px', color: 'var(--text-3)', marginTop: '4px', marginLeft: '24px' }}>
        Excludes Invalid and Unknown email statuses
      </span>
    </div>
    <div className="modal-actions"><button className="btn btn-secondary" onClick={onClose}>Cancel</button><button className="btn btn-primary" onClick={handleExport}><Download size={16} /> Export CSV</button></div>
  </div>);
};

// Add Contact Form
const AddContactForm = ({ onSuccess }) => {
  const { addToast } = useToast();
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(false);
  const handleSubmit = async (e) => { e.preventDefault(); setLoading(true); try { await api.post('/contacts', data); onSuccess(); } catch (e) { addToast(e.message, 'error'); } setLoading(false); };
  return (<form onSubmit={handleSubmit}>
    <div className="form-row"><div className="form-group"><label>First Name</label><input type="text" value={data.first_name || ''} onChange={e => setData({ ...data, first_name: e.target.value })} /></div><div className="form-group"><label>Last Name</label><input type="text" value={data.last_name || ''} onChange={e => setData({ ...data, last_name: e.target.value })} /></div></div>
    <div className="form-row"><div className="form-group"><label>Email *</label><input type="email" value={data.email || ''} onChange={e => setData({ ...data, email: e.target.value })} required /></div><div className="form-group"><label>Phone</label><input type="text" value={data.first_phone || ''} onChange={e => setData({ ...data, first_phone: e.target.value })} /></div></div>
    <div className="form-row"><div className="form-group"><label>Company</label><input type="text" value={data.company || ''} onChange={e => setData({ ...data, company: e.target.value })} /></div><div className="form-group"><label>Title</label><input type="text" value={data.title || ''} onChange={e => setData({ ...data, title: e.target.value })} /></div></div>
    <div className="form-row"><div className="form-group"><label>Outreach Lists</label><input type="text" value={data.outreach_lists || ''} onChange={e => setData({ ...data, outreach_lists: e.target.value })} placeholder="List1, List2" /></div><div className="form-group"><label>Campaigns</label><input type="text" value={data.campaigns_assigned || ''} onChange={e => setData({ ...data, campaigns_assigned: e.target.value })} placeholder="Campaign1, Campaign2" /></div></div>
    <div className="form-group"><label>Notes</label><textarea value={data.notes || ''} onChange={e => setData({ ...data, notes: e.target.value })} rows={3} /></div>
    <div className="modal-actions"><button type="submit" className="btn btn-primary" disabled={loading}>{loading ? <Loader2 className="spin" size={16} /> : 'Create Contact'}</button></div></form>);
};

// Duplicates Page
// Enrichment Page - Data Cleaning & Enrichment
const EnrichmentPage = () => {
  const { addToast } = useToast();
  const [stats, setStats] = useState(null);
  const [activeTab, setActiveTab] = useState('names');
  const [nameChanges, setNameChanges] = useState([]);
  const [companyChanges, setCompanyChanges] = useState([]);
  const [titleChanges, setTitleChanges] = useState([]);
  const [nameTotalCount, setNameTotalCount] = useState(0);
  const [companyTotalCount, setCompanyTotalCount] = useState(0);
  const [titleTotalCount, setTitleTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [applying, setApplying] = useState(false);
  const [selectedNames, setSelectedNames] = useState([]);
  const [selectedCompanies, setSelectedCompanies] = useState([]);
  const [selectedTitles, setSelectedTitles] = useState([]);
  // Email verification state
  const [verifyStatus, setVerifyStatus] = useState(null);
  const [verifyJob, setVerifyJob] = useState(null);
  const [verifyLoading, setVerifyLoading] = useState(false);

  const fetchStats = async () => {
    try {
      const s = await api.get('/cleaning/stats');
      setStats(s);
    } catch (e) { console.error(e); }
  };

  const fetchNamePreview = async () => {
    setLoading(true);
    try {
      const r = await api.get('/cleaning/names/preview?limit=500');
      setNameChanges(r.changes || []);
      setNameTotalCount(r.total || 0);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  const fetchCompanyPreview = async () => {
    setLoading(true);
    try {
      const r = await api.get('/cleaning/companies/preview?limit=500');
      setCompanyChanges(r.changes || []);
      setCompanyTotalCount(r.total || 0);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  const fetchTitlePreview = async () => {
    setLoading(true);
    try {
      const r = await api.get('/cleaning/titles/preview?limit=500');
      setTitleChanges(r.changes || []);
      setTitleTotalCount(r.total || 0);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  const fetchVerifyStatus = async () => {
    try {
      const s = await api.get('/verify/status');
      setVerifyStatus(s);
      // Also check for any active jobs (running or pending)
      const activeJobs = await api.get('/verify/jobs/active');
      if (activeJobs && activeJobs.length > 0) {
        // Set the first active job (most recent)
        setVerifyJob(activeJobs[0]);
      }
    } catch (e) { console.error(e); }
  };

  const startBulkVerify = async (limit = null) => {
    if (!verifyStatus?.configured) {
      addToast('Configure API key in Settings → Integrations first', 'error');
      return;
    }
    const count = limit || verifyStatus?.unverified_count || 0;
    if (!window.confirm(`Start verification for ${count.toLocaleString()} contacts?\n\nThis will use ${count.toLocaleString()} API credits.\n\nNote: Rate limit is ~1,500/hour. Large batches will take time.\n\nVerification runs in the background.`)) return;

    setVerifyLoading(true);
    try {
      const params = limit ? `?limit=${limit}` : '';
      const r = await api.post(`/verify/bulk${params}`);
      if (r.job_id) {
        setVerifyJob({ id: r.job_id, status: 'running', total_contacts: r.total_contacts, verified_count: 0 });
        addToast(`Started verification for ${r.total_contacts} contacts`, 'success');
      } else {
        addToast(r.message || 'No contacts need verification', 'info');
      }
    } catch (e) { addToast(e.message, 'error'); }
    setVerifyLoading(false);
  };

  useEffect(() => { fetchStats(); fetchVerifyStatus(); }, []);
  useEffect(() => {
    if (activeTab === 'names') fetchNamePreview();
    else if (activeTab === 'companies') fetchCompanyPreview();
    else if (activeTab === 'titles') fetchTitlePreview();
    else if (activeTab === 'verify') fetchVerifyStatus();
  }, [activeTab]);

  // Poll verification job status
  useEffect(() => {
    if (!verifyJob?.id || verifyJob?.status === 'completed' || verifyJob?.status === 'failed') return;
    const poll = async () => {
      try {
        const job = await api.get(`/verify/job/${verifyJob.id}`);
        setVerifyJob(job);
        if (job.status === 'completed' || job.status === 'failed') {
          fetchVerifyStatus();
          if (job.status === 'completed') addToast(`Verification complete: ${job.valid_count} valid, ${job.invalid_count} invalid`, 'success');
        }
      } catch (e) { console.error(e); }
    };
    const interval = setInterval(poll, 2000);
    return () => clearInterval(interval);
  }, [verifyJob?.id, verifyJob?.status]);

  const handleApplySelected = async (type) => {
    const ids = type === 'names' ? selectedNames : type === 'titles' ? selectedTitles : selectedCompanies;
    if (ids.length === 0) { addToast('Select items to apply', 'error'); return; }
    setApplying(true);
    try {
      const endpoint = type === 'names' ? '/cleaning/names/apply' : type === 'titles' ? '/cleaning/titles/apply' : '/cleaning/companies/apply';
      const r = await api.post(endpoint, { contact_ids: ids, field: type });
      addToast(r.message, 'success');
      if (type === 'names') { setSelectedNames([]); fetchNamePreview(); }
      else if (type === 'titles') { setSelectedTitles([]); fetchTitlePreview(); }
      else { setSelectedCompanies([]); fetchCompanyPreview(); }
      fetchStats();
    } catch (e) { addToast(e.message, 'error'); }
    setApplying(false);
  };

  const handleApplyAll = async (type) => {
    const count = type === 'names' ? nameTotalCount : type === 'titles' ? titleTotalCount : companyTotalCount;
    if (!window.confirm(`Apply cleaning to ALL ${count.toLocaleString()} ${type}?\n\nThis will process all items in the database, not just the visible ones.\n\nThis action cannot be undone.`)) return;
    setApplying(true);
    try {
      const endpoint = type === 'names' ? '/cleaning/names/apply-all' : type === 'titles' ? '/cleaning/titles/apply-all' : '/cleaning/companies/apply-all';
      const r = await api.post(endpoint);
      addToast(r.message, 'success');
      if (type === 'names') fetchNamePreview();
      else if (type === 'titles') fetchTitlePreview();
      else fetchCompanyPreview();
      fetchStats();
    } catch (e) { addToast(e.message, 'error'); }
    setApplying(false);
  };

  const toggleSelectAll = (type) => {
    if (type === 'names') {
      setSelectedNames(selectedNames.length === nameChanges.length ? [] : nameChanges.map(c => c.id));
    } else if (type === 'titles') {
      setSelectedTitles(selectedTitles.length === titleChanges.length ? [] : titleChanges.map(c => c.id));
    } else {
      setSelectedCompanies(selectedCompanies.length === companyChanges.length ? [] : companyChanges.map(c => c.id));
    }
  };

  return (
    <div className="page enrichment-page">
      <div className="page-header">
        <div>
          <h1>Data Enrichment</h1>
          <p className="subtitle">Clean and enrich your contact data</p>
        </div>
        <div className="header-actions">
          <button className="btn btn-secondary" onClick={() => { fetchStats(); activeTab === 'names' ? fetchNamePreview() : fetchCompanyPreview(); }}>
            <RefreshCw size={16} /> Refresh
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="duplicates-stats">
        <div className="dup-stat-card">
          <div className="dup-stat-icon"><User size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.names?.needs_cleaning?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Names to Clean</span>
          </div>
        </div>
        <div className="dup-stat-card warning">
          <div className="dup-stat-icon"><Building2 size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.companies?.needs_cleaning?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Companies to Clean</span>
          </div>
        </div>
        <div className="dup-stat-card accent">
          <div className="dup-stat-icon"><Briefcase size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.titles?.needs_cleaning?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Titles to Clean</span>
          </div>
        </div>
        <div className="dup-stat-card accent">
          <div className="dup-stat-icon"><Database size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.total_contacts?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Total Contacts</span>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="enrichment-tabs">
        <button className={`tab-btn ${activeTab === 'names' ? 'active' : ''}`} onClick={() => setActiveTab('names')}>
          <User size={18} /> Name Cleaning
          {nameTotalCount > 0 && <span className="tab-badge">{nameTotalCount}</span>}
        </button>
        <button className={`tab-btn ${activeTab === 'companies' ? 'active' : ''}`} onClick={() => setActiveTab('companies')}>
          <Building2 size={18} /> Company Cleaning
          {companyTotalCount > 0 && <span className="tab-badge">{companyTotalCount}</span>}
        </button>
        <button className={`tab-btn ${activeTab === 'titles' ? 'active' : ''}`} onClick={() => setActiveTab('titles')}>
          <Briefcase size={18} /> Title Cleaning
          {titleTotalCount > 0 && <span className="tab-badge">{titleTotalCount}</span>}
        </button>
        <button className={`tab-btn ${activeTab === 'verify' ? 'active' : ''}`} onClick={() => setActiveTab('verify')}>
          <Mail size={18} /> Email Verification
          {verifyStatus?.unverified_count > 0 && <span className="tab-badge">{verifyStatus.unverified_count.toLocaleString()}</span>}
        </button>
      </div>

      {/* Action Bar */}
      {((activeTab === 'names' && nameChanges.length > 0) || (activeTab === 'companies' && companyChanges.length > 0) || (activeTab === 'titles' && titleChanges.length > 0)) && (
        <div className="duplicates-action-bar">
          <div className="action-bar-info">
            <input
              type="checkbox"
              checked={activeTab === 'names' ? selectedNames.length === nameChanges.length : activeTab === 'titles' ? selectedTitles.length === titleChanges.length : selectedCompanies.length === companyChanges.length}
              onChange={() => toggleSelectAll(activeTab)}
            />
            <span>
              {activeTab === 'names' ? selectedNames.length : activeTab === 'titles' ? selectedTitles.length : selectedCompanies.length} of {activeTab === 'names' ? nameChanges.length : activeTab === 'titles' ? titleChanges.length : companyChanges.length} selected
              {activeTab === 'names' && nameTotalCount > nameChanges.length && <span style={{opacity: 0.7}}> (showing {nameChanges.length} of {nameTotalCount} total)</span>}
              {activeTab === 'titles' && titleTotalCount > titleChanges.length && <span style={{opacity: 0.7}}> (showing {titleChanges.length} of {titleTotalCount} total)</span>}
              {activeTab === 'companies' && companyTotalCount > companyChanges.length && <span style={{opacity: 0.7}}> (showing {companyChanges.length} of {companyTotalCount} total)</span>}
            </span>
          </div>
          <div className="action-bar-buttons">
            <button
              className="btn btn-secondary"
              onClick={() => handleApplySelected(activeTab)}
              disabled={applying || (activeTab === 'names' ? selectedNames.length === 0 : activeTab === 'titles' ? selectedTitles.length === 0 : selectedCompanies.length === 0)}
            >
              {applying ? <Loader2 className="spin" size={16} /> : <Check size={16} />}
              Apply Selected
            </button>
            <button
              className="btn btn-primary"
              onClick={() => handleApplyAll(activeTab)}
              disabled={applying}
            >
              {applying ? <Loader2 className="spin" size={16} /> : <Zap size={16} />}
              Apply All ({activeTab === 'names' ? nameTotalCount.toLocaleString() : activeTab === 'titles' ? titleTotalCount.toLocaleString() : companyTotalCount.toLocaleString()})
            </button>
          </div>
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="loading-container"><Loader2 className="spin" size={32} /></div>
      ) : activeTab === 'names' ? (
        nameChanges.length === 0 ? (
          <div className="empty-state">
            <CheckCircle size={48} />
            <h3>All names look good!</h3>
            <p>No name cleaning needed at this time.</p>
          </div>
        ) : (
          <div className="enrichment-table-container">
            <table className="enrichment-table">
              <thead>
                <tr>
                  <th style={{width: '40px'}}></th>
                  <th>First Name</th>
                  <th style={{width: '40px'}}></th>
                  <th>Last Name</th>
                </tr>
              </thead>
              <tbody>
                {nameChanges.map(change => (
                  <tr key={change.id}>
                    <td>
                      <input
                        type="checkbox"
                        checked={selectedNames.includes(change.id)}
                        onChange={() => setSelectedNames(prev =>
                          prev.includes(change.id) ? prev.filter(id => id !== change.id) : [...prev, change.id]
                        )}
                      />
                    </td>
                    <td>
                      {change.first_name.changed ? (
                        <div className="change-cell">
                          <span className="before">{change.first_name.before}</span>
                          <ArrowRightLeft size={14} />
                          <span className="after">{change.first_name.after}</span>
                        </div>
                      ) : (
                        <span className="no-change">{change.first_name.before || '-'}</span>
                      )}
                    </td>
                    <td></td>
                    <td>
                      {change.last_name.changed ? (
                        <div className="change-cell">
                          <span className="before">{change.last_name.before}</span>
                          <ArrowRightLeft size={14} />
                          <span className="after">{change.last_name.after}</span>
                        </div>
                      ) : (
                        <span className="no-change">{change.last_name.before || '-'}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      ) : activeTab === 'companies' ? (
        companyChanges.length === 0 ? (
          <div className="empty-state">
            <CheckCircle size={48} />
            <h3>All company names look good!</h3>
            <p>No company cleaning needed at this time.</p>
          </div>
        ) : (
          <div className="enrichment-table-container">
            <table className="enrichment-table">
              <thead>
                <tr>
                  <th style={{width: '40px'}}></th>
                  <th>Before</th>
                  <th style={{width: '40px'}}></th>
                  <th>After</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {companyChanges.map(change => (
                  <tr key={change.id}>
                    <td>
                      <input
                        type="checkbox"
                        checked={selectedCompanies.includes(change.id)}
                        onChange={() => setSelectedCompanies(prev =>
                          prev.includes(change.id) ? prev.filter(id => id !== change.id) : [...prev, change.id]
                        )}
                      />
                    </td>
                    <td><span className="before">{change.company.before}</span></td>
                    <td><ArrowRightLeft size={14} /></td>
                    <td><span className="after">{change.company.after}</span></td>
                    <td><span className="reason-badge">{change.company.reason}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      ) : activeTab === 'titles' ? (
        titleChanges.length === 0 ? (
          <div className="empty-state">
            <CheckCircle size={48} />
            <h3>All titles look good!</h3>
            <p>No title cleaning needed at this time.</p>
          </div>
        ) : (
          <div className="enrichment-table-container">
            <table className="enrichment-table">
              <thead>
                <tr>
                  <th style={{width: '40px'}}></th>
                  <th>Before</th>
                  <th style={{width: '40px'}}></th>
                  <th>After</th>
                </tr>
              </thead>
              <tbody>
                {titleChanges.map(change => (
                  <tr key={change.id}>
                    <td>
                      <input
                        type="checkbox"
                        checked={selectedTitles.includes(change.id)}
                        onChange={() => setSelectedTitles(prev =>
                          prev.includes(change.id) ? prev.filter(id => id !== change.id) : [...prev, change.id]
                        )}
                      />
                    </td>
                    <td>
                      <div className="change-cell">
                        <span className="before">{change.title.before}</span>
                        <ArrowRightLeft size={14} />
                        <span className="after">{change.title.after}</span>
                      </div>
                    </td>
                    <td></td>
                    <td></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      ) : activeTab === 'verify' ? (
        <div className="verify-tab-content">
          {/* Verification Status Card */}
          <div className="verify-status-card">
            <div className="verify-status-header">
              <h3><Mail size={20} /> Email Verification</h3>
              {verifyStatus?.configured ? (
                <span className="status-badge success">API Configured</span>
              ) : (
                <span className="status-badge warning">API Not Configured</span>
              )}
            </div>

            <div className="verify-stats-grid">
              <div className="verify-stat">
                <span className="verify-stat-value">{verifyStatus?.unverified_count?.toLocaleString() || 0}</span>
                <span className="verify-stat-label">Unverified Contacts</span>
              </div>
            </div>

            {/* Bulk Verify Button */}
            <div className="verify-actions" style={{ marginTop: 20 }}>
              <button
                className="btn btn-primary"
                onClick={() => startBulkVerify()}
                disabled={verifyLoading || !verifyStatus?.configured || verifyStatus?.unverified_count === 0}
              >
                {verifyLoading ? <Loader2 className="spin" size={16} /> : <Zap size={16} />}
                Verify All ({verifyStatus?.unverified_count?.toLocaleString() || 0} contacts)
              </button>
              {!verifyStatus?.configured && (
                <span className="verify-hint warning">Configure API key in Settings → Integrations</span>
              )}
            </div>

            {/* Batch Options */}
            {verifyStatus?.unverified_count > 100 && verifyStatus?.configured && (
              <div className="verify-batch-options">
                <span>Or verify in batches:</span>
                <button className="btn btn-secondary btn-sm" onClick={() => startBulkVerify(100)} disabled={verifyLoading}>100</button>
                <button className="btn btn-secondary btn-sm" onClick={() => startBulkVerify(500)} disabled={verifyLoading}>500</button>
                <button className="btn btn-secondary btn-sm" onClick={() => startBulkVerify(1000)} disabled={verifyLoading}>1,000</button>
              </div>
            )}
          </div>

          {/* Active Job Progress */}
          {verifyJob && verifyJob.status === 'running' && (
            <div className="verify-job-card">
              <h4><Loader2 className="spin" size={16} /> Verification In Progress</h4>
              <div className="verification-progress-bar">
                <div
                  className="verification-progress-fill"
                  style={{ width: `${verifyJob.total_contacts > 0 ? (verifyJob.verified_count / verifyJob.total_contacts * 100) : 0}%` }}
                />
              </div>
              <div className="verify-job-stats">
                <span>{verifyJob.verified_count} / {verifyJob.total_contacts} verified</span>
                <span className="valid">{verifyJob.valid_count} valid</span>
                <span className="invalid">{verifyJob.invalid_count} invalid</span>
                <span className="unknown">{verifyJob.unknown_count} unknown</span>
              </div>
              {verifyJob.current_email && (
                <p className="current-email">Checking: {verifyJob.current_email}</p>
              )}
            </div>
          )}

          {/* Completed Job */}
          {verifyJob && verifyJob.status === 'completed' && (
            <div className="verify-job-card completed">
              <h4><CheckCircle size={16} /> Verification Complete</h4>
              <div className="verify-job-stats">
                <span>{verifyJob.verified_count} verified</span>
                <span className="valid">{verifyJob.valid_count} valid</span>
                <span className="invalid">{verifyJob.invalid_count} invalid</span>
                <span className="unknown">{verifyJob.unknown_count} unknown</span>
              </div>
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
};

const DuplicatesPage = () => {
  const { addToast } = useToast();
  const [stats, setStats] = useState(null);
  const [groups, setGroups] = useState([]);
  const [loading, setLoading] = useState(true);
  const [merging, setMerging] = useState(false);
  const [page, setPage] = useState(1);
  const [expandedGroup, setExpandedGroup] = useState(null);
  const pageSize = 20;

  const fetchStats = async () => {
    try {
      const s = await api.get('/duplicates/stats');
      setStats(s);
    } catch (e) { console.error(e); }
  };

  const fetchGroups = async () => {
    setLoading(true);
    try {
      const r = await api.get('/duplicates');
      setGroups(r.groups || []);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { fetchStats(); fetchGroups(); }, []);

  const handleAutoMergeAll = async () => {
    if (!window.confirm(`This will automatically merge ALL ${stats?.total_groups || 0} duplicate groups.\n\nThe oldest contact in each group becomes the primary.\nAll campaigns and lists will be combined.\n\nContinue?`)) return;
    setMerging(true);
    try {
      const r = await api.post('/duplicates/auto-merge');
      addToast(r.message, 'success');
      fetchStats();
      fetchGroups();
    } catch (e) { addToast(e.message, 'error'); }
    setMerging(false);
  };

  const handleMergeGroup = async (email) => {
    try {
      const r = await api.post(`/duplicates/merge-group/${encodeURIComponent(email)}`);
      addToast(r.message, 'success');
      fetchStats();
      fetchGroups();
    } catch (e) { addToast(e.message, 'error'); }
  };

  const paginatedGroups = groups.slice((page - 1) * pageSize, page * pageSize);
  const totalPages = Math.ceil(groups.length / pageSize);

  return (
    <div className="page duplicates-page">
      <div className="page-header">
        <div>
          <h1>Duplicate Manager</h1>
          <p className="subtitle">Find and merge duplicate contacts automatically</p>
        </div>
        <div className="header-actions">
          <button className="btn btn-secondary" onClick={() => { fetchStats(); fetchGroups(); }}><RefreshCw size={16} /> Refresh</button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="duplicates-stats">
        <div className="dup-stat-card">
          <div className="dup-stat-icon"><Layers size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.total_groups?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Duplicate Groups</span>
          </div>
        </div>
        <div className="dup-stat-card warning">
          <div className="dup-stat-icon"><AlertTriangle size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.total_duplicates?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Duplicate Contacts</span>
          </div>
        </div>
        <div className="dup-stat-card success">
          <div className="dup-stat-icon"><GitMerge size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.merged_count?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Already Merged</span>
          </div>
        </div>
        <div className="dup-stat-card accent">
          <div className="dup-stat-icon"><Zap size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.potential_savings?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">Can Be Cleaned</span>
          </div>
        </div>
      </div>

      {/* Action Bar */}
      {stats?.total_groups > 0 && (
        <div className="duplicates-action-bar">
          <div className="action-bar-info">
            <Zap size={20} />
            <span><strong>{stats.total_groups} duplicate groups</strong> found with <strong>{stats.total_duplicates} contacts</strong> that can be merged</span>
          </div>
          <button className="btn btn-accent btn-lg" onClick={handleAutoMergeAll} disabled={merging}>
            {merging ? <><Loader2 className="spin" size={18} /> Merging...</> : <><Zap size={18} /> Auto-Merge All</>}
          </button>
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="loading-state"><Loader2 className="spin" size={32} /><span>Scanning for duplicates...</span></div>
      ) : groups.length === 0 ? (
        <div className="empty-duplicates-state">
          <div className="empty-icon"><Check size={48} /></div>
          <h3>No duplicates found!</h3>
          <p>Your contact list is clean. All contacts have unique email addresses.</p>
        </div>
      ) : (
        <>
          <div className="duplicates-list">
            {paginatedGroups.map((group, i) => (
              <div key={group.email} className={`duplicate-card ${expandedGroup === group.email ? 'expanded' : ''}`}>
                <div className="duplicate-card-header" onClick={() => setExpandedGroup(expandedGroup === group.email ? null : group.email)}>
                  <div className="duplicate-card-left">
                    <button className="expand-btn">{expandedGroup === group.email ? <ChevronDown size={18} /> : <ChevronRight size={18} />}</button>
                    <div className="duplicate-card-info">
                      <span className="duplicate-email">{group.email}</span>
                      <span className="duplicate-meta">{group.count} copies • Click to expand</span>
                    </div>
                  </div>
                  <div className="duplicate-card-right" onClick={e => e.stopPropagation()}>
                    <button className="btn btn-primary btn-sm" onClick={() => handleMergeGroup(group.email)}>
                      <Merge size={14} /> Merge
                    </button>
                  </div>
                </div>
                {expandedGroup === group.email && (
                  <div className="duplicate-card-body">
                    <div className="duplicate-contacts-grid">
                      {group.contacts.map((c, idx) => (
                        <div key={c.id} className={`duplicate-contact-card ${idx === 0 ? 'primary' : ''}`}>
                          {idx === 0 && <span className="primary-badge">Primary (Oldest)</span>}
                          <div className="contact-name">{c.first_name} {c.last_name}<span className="workspace-badge" title={`Workspace: ${c.reachinbox_workspace || 'US'}`}>{c.reachinbox_workspace === 'MX' ? '🇲🇽' : '🇺🇸'}</span></div>
                          <div className="contact-detail"><strong>Company:</strong> {c.company || '—'}</div>
                          <div className="contact-detail"><strong>Title:</strong> {c.title || '—'}</div>
                          <div className="contact-detail"><strong>Lists:</strong> {c.outreach_lists || '—'}</div>
                          <div className="contact-detail"><strong>Campaigns:</strong> {c.campaigns_assigned || '—'}</div>
                        </div>
                      ))}
                    </div>
                    <div className="merge-info">
                      <AlertCircle size={16} />
                      <span>Merging will keep the <strong>oldest contact</strong> as primary and combine all lists & campaigns from duplicates.</span>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="pagination">
              <button className="btn btn-secondary btn-sm" disabled={page === 1} onClick={() => setPage(p => p - 1)}>
                <ChevronLeft size={16} /> Previous
              </button>
              <span className="pagination-info">Page {page} of {totalPages}</span>
              <button className="btn btn-secondary btn-sm" disabled={page === totalPages} onClick={() => setPage(p => p + 1)}>
                Next <ChevronRight size={16} />
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
};

// Campaigns Page with Template Breakdown
const CampaignsPage = () => {
  const { addToast } = useToast();
  const [mainTab, setMainTab] = useState('campaigns'); // 'campaigns' | 'templates'
  const [campaigns, setCampaigns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [editData, setEditData] = useState({});
  const [expandedCampaign, setExpandedCampaign] = useState(null);
  const [campaignDetails, setCampaignDetails] = useState(null);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [filterStatuses, setFilterStatuses] = useState([]);
  const [filterCountries, setFilterCountries] = useState([]);
  const [editingSettings, setEditingSettings] = useState(null);
  const [riPushModal, setRiPushModal] = useState(null);
  const [riCampaignsList, setRiCampaignsList] = useState([]);
  const [riCampaignsLoading, setRiCampaignsLoading] = useState(false);
  const [riCampaignsFallback, setRiCampaignsFallback] = useState(false);
  const [riSelectedId, setRiSelectedId] = useState('');
  const [riPushing, setRiPushing] = useState(false);
  const [syncing, setSyncing] = useState(false);

  const syncReachInbox = async () => {
    setSyncing(true);
    try {
      // Sync campaign stats
      await api.post(`/reachinbox/sync-campaigns?workspace=${workspace}`);
      // Sync analytics (step + variant data)
      await api.post(`/reachinbox/sync-analytics?workspace=${workspace}`);
      addToast('ReachInbox synced! Campaigns updated with latest stats.', 'success');
      fetchCampaigns();
    } catch (e) {
      addToast(e.message || 'Sync failed', 'error');
    }
    setSyncing(false);
  };

  const statusOptions = [
    { id: 'Active', name: 'Active' },
    { id: 'Paused', name: 'Paused' },
    { id: 'Completed', name: 'Completed' }
  ];
  const countryOptions = [
    { id: 'Mexico', name: 'Mexico' },
    { id: 'United States', name: 'United States' },
    { id: 'Germany', name: 'Germany' },
    { id: 'Spain', name: 'Spain' }
  ];

  const { workspace } = useWorkspace();
  const fetchCampaigns = async () => {
    setLoading(true);
    try {
      const qs = new URLSearchParams({ workspace });
      if (search) qs.set('search', search);
      const r = await api.get(`/campaigns?${qs}`);
      setCampaigns(r.data);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { fetchCampaigns(); }, [search, workspace]);

  const fetchCampaignDetails = async (id) => {
    setLoadingDetails(true);
    try {
      const r = await api.get(`/campaigns/${id}`);
      setCampaignDetails(r);
    } catch (e) { addToast(e.message, 'error'); }
    setLoadingDetails(false);
  };

  const toggleCampaign = (id) => {
    if (expandedCampaign === id) { setExpandedCampaign(null); setCampaignDetails(null); }
    else { setExpandedCampaign(id); fetchCampaignDetails(id); }
  };

  const handleCreate = async (data) => { try { await api.post('/campaigns', { ...data, market: workspace }); addToast('Campaign created!', 'success'); setShowCreate(false); fetchCampaigns(); } catch (e) { addToast(e.message, 'error'); } };
  const handleUpdate = async (id) => { try { await api.put(`/campaigns/${id}`, editData); addToast('Campaign updated!', 'success'); setEditingId(null); fetchCampaigns(); if (expandedCampaign === id) fetchCampaignDetails(id); } catch (e) { addToast(e.message, 'error'); } };
  const handleUpdateSettings = async (data) => { try { await api.put(`/campaigns/${editingSettings.id}`, data); addToast('Campaign updated!', 'success'); setEditingSettings(null); fetchCampaigns(); } catch (e) { addToast(e.message, 'error'); } };
  const handleDelete = async (id) => { if (!window.confirm('Delete this campaign?')) return; try { await api.delete(`/campaigns/${id}`); addToast('Campaign deleted', 'success'); fetchCampaigns(); } catch (e) { addToast(e.message, 'error'); } };

  const openRiPushModal = async (camp) => {
    setRiPushModal(camp);
    setRiSelectedId('');
    setRiCampaignsList([]);
    setRiCampaignsFallback(false);
    setRiCampaignsLoading(true);
    try {
      const res = await api.get(`/reachinbox/campaigns?workspace=${camp.market || 'US'}`);
      if (res.campaigns && res.campaigns.length > 0) {
        setRiCampaignsList(res.campaigns);
      } else {
        setRiCampaignsFallback(true);
      }
    } catch (e) {
      setRiCampaignsFallback(true);
    }
    setRiCampaignsLoading(false);
  };

  const executeRiPush = async () => {
    if (!riSelectedId) { addToast('Select a ReachInbox campaign', 'error'); return; }
    setRiPushing(true);
    try {
      const res = await api.post(`/reachinbox/campaigns/${parseInt(riSelectedId)}/push-contacts`, {
        deduply_campaign_id: riPushModal.id
      });
      const s = res.stats || {};
      addToast(`Pushed: ${s.pushed || 0} contacts (${s.skipped_already_pushed || 0} already pushed, ${s.failed || 0} failed)`, s.failed > 0 ? 'warning' : 'success');
      setRiPushModal(null);
    } catch (e) {
      addToast(e.message || 'Push failed', 'error');
    }
    setRiPushing(false);
  };

  const filteredCampaigns = campaigns.filter(c => {
    if (filterStatuses.length > 0 && !filterStatuses.includes(c.status)) return false;
    if (filterCountries.length > 0 && !filterCountries.includes(c.country)) return false;
    return true;
  });
  const totalSent = campaigns.reduce((s, c) => s + (c.emails_sent || 0), 0);
  const totalOpened = campaigns.reduce((s, c) => s + (c.emails_opened || 0), 0);
  const totalReplied = campaigns.reduce((s, c) => s + (c.emails_replied || 0), 0);
  const avgOpenRate = totalSent > 0 ? Math.round(100 * totalOpened / totalSent) : 0;
  const avgReplyRate = totalSent > 0 ? Math.round(100 * totalReplied / totalSent) : 0;

  return (<div className="page campaigns-page">
    <div className="page-header">
      <div>
        <h1>Campaigns</h1>
        <p className="subtitle">Email campaigns and sequence templates</p>
      </div>
      <div className="header-actions">
        {mainTab === 'campaigns' && <>
          <button className="sync-now-btn" onClick={syncReachInbox} disabled={syncing}>
            {syncing ? <><Loader2 size={14} className="spin" /> Syncing...</> : <><RefreshCw size={14} /> Sync ReachInbox</>}
          </button>
          <button className="btn btn-secondary" onClick={fetchCampaigns}><RefreshCw size={16} /> Refresh</button>
          <button className="btn btn-primary" onClick={() => setShowCreate(true)}><Plus size={16} /> New Campaign</button>
        </>}
      </div>
    </div>

    {/* Main tab switcher */}
    <div style={{display:'flex', gap:4, marginBottom:20, borderBottom:'1px solid var(--border)', paddingBottom:0}}>
      {[{id:'campaigns',label:'Campaigns',icon:'📧'},{id:'templates',label:'Templates',icon:'📝'}].map(t => (
        <button key={t.id} onClick={() => setMainTab(t.id)}
          style={{padding:'8px 20px', border:'none', cursor:'pointer', fontWeight: mainTab===t.id ? 600 : 400,
            borderBottom: mainTab===t.id ? '2px solid var(--coral)' : '2px solid transparent',
            background:'transparent', color: mainTab===t.id ? 'var(--coral)' : 'var(--text-secondary)',
            fontSize:14, display:'flex', alignItems:'center', gap:6, transition:'all 0.15s'}}>
          {t.icon} {t.label}
        </button>
      ))}
    </div>

    {mainTab === 'templates' && <TemplatesPage />}
    {mainTab === 'campaigns' && <>

    {/* Campaign Stats */}
    <div className="campaigns-stats">
      <div className="campaign-stat-card">
        <div className="c-stat-icon"><Mail size={20} /></div>
        <div className="c-stat-info">
          <span className="c-stat-value">{campaigns.length}</span>
          <span className="c-stat-label">Total Campaigns</span>
        </div>
      </div>
      <div className="campaign-stat-card">
        <div className="c-stat-icon sent"><Send size={20} /></div>
        <div className="c-stat-info">
          <span className="c-stat-value">{totalSent.toLocaleString()}</span>
          <span className="c-stat-label">Emails Sent</span>
        </div>
      </div>
      <div className="campaign-stat-card">
        <div className="c-stat-icon success"><Eye size={20} /></div>
        <div className="c-stat-info">
          <span className="c-stat-value">{avgOpenRate}%</span>
          <span className="c-stat-label">Avg Open Rate</span>
        </div>
      </div>
      <div className="campaign-stat-card">
        <div className="c-stat-icon success"><MessageCircle size={20} /></div>
        <div className="c-stat-info">
          <span className="c-stat-value">{avgReplyRate}%</span>
          <span className="c-stat-label">Avg Reply Rate</span>
        </div>
      </div>
      <div className="campaign-stat-card highlight">
        <div className="c-stat-icon"><Target size={20} /></div>
        <div className="c-stat-info">
          <span className="c-stat-value">{campaigns.reduce((s, c) => s + (c.meetings_booked || 0), 0)}</span>
          <span className="c-stat-label">Meetings Booked</span>
        </div>
      </div>
    </div>

    <div className="campaigns-toolbar">
      <div className="search-box"><Search size={18} /><input type="text" placeholder="Search campaigns..." value={search} onChange={e => setSearch(e.target.value)} /></div>
      <div className="campaign-filters">
        <MultiSelect
          options={statusOptions}
          value={filterStatuses}
          onChange={setFilterStatuses}
          placeholder="All Statuses"
        />
        <MultiSelect
          options={countryOptions}
          value={filterCountries}
          onChange={setFilterCountries}
          placeholder="All Countries"
        />
      </div>
    </div>

    {/* Filter Chips */}
    {(filterStatuses.length > 0 || filterCountries.length > 0) && (
      <div className="filter-chips">
        {filterStatuses.map(status => (
          <span key={status} className="filter-chip">
            {status}
            <button onClick={() => setFilterStatuses(filterStatuses.filter(s => s !== status))}><X size={12} /></button>
          </span>
        ))}
        {filterCountries.map(country => (
          <span key={country} className="filter-chip">
            {country}
            <button onClick={() => setFilterCountries(filterCountries.filter(c => c !== country))}><X size={12} /></button>
          </span>
        ))}
        <button className="filter-chip-clear" onClick={() => { setFilterStatuses([]); setFilterCountries([]); }}>
          Clear all
        </button>
      </div>
    )}

    {loading ? (
      <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading campaigns...</span></div>
    ) : filteredCampaigns.length === 0 ? (
      <div className="empty-campaigns-state">
        <div className="empty-icon"><Mail size={48} /></div>
        <h3>{campaigns.length === 0 ? 'No campaigns yet' : 'No campaigns match your filter'}</h3>
        <p>{campaigns.length === 0 ? 'Create your first campaign to start tracking cold email performance.' : 'Try adjusting your filter.'}</p>
        {campaigns.length === 0 && <button className="btn btn-primary" onClick={() => setShowCreate(true)}><Plus size={16} /> Create First Campaign</button>}
      </div>
    ) : (
      <div className="campaigns-list-v2">
        {filteredCampaigns.map(camp => (
          <div key={camp.id} className={`campaign-card ${expandedCampaign === camp.id ? 'expanded' : ''}`}>
            <div className="campaign-card-header" onClick={() => toggleCampaign(camp.id)}>
              <div className="campaign-left">
                <button className="expand-icon">{expandedCampaign === camp.id ? <ChevronDown size={20} /> : <ChevronRight size={20} />}</button>
                <div className="campaign-info">
                  <div className="campaign-title-row">
                    <h3>{camp.name}</h3>
                    {camp.country && <span className={`country-badge country-${camp.country?.toLowerCase().replace(/ /g, '-')}`}>{camp.country}</span>}
                    <span className={`campaign-status-badge ${camp.status?.toLowerCase()}`}>{camp.status}</span>
                    {camp.created_by === 'hermes' && <span className="agent-badge" style={{fontSize:10, padding:'2px 8px'}}>Agent</span>}
                    {camp.created_by === 'hermes' && !camp.approved_by && <span className="campaign-status-badge" style={{background:'#fff3cd', color:'#856404', border:'1px solid #ffc107'}}>Needs Approval</span>}
                  </div>
                  {camp.description && <p className="campaign-desc">{camp.description}</p>}
                </div>
              </div>
              <div className="campaign-stats-row">
                <div className="c-metric"><span className="c-metric-value">{camp.total_leads?.toLocaleString() || 0}</span><span className="c-metric-label">Leads</span></div>
                <div className="c-metric"><span className="c-metric-value">{camp.emails_sent?.toLocaleString() || 0}</span><span className="c-metric-label">Sent</span></div>
                <div className="c-metric"><span className="c-metric-value">{camp.open_rate || 0}%</span><span className="c-metric-label">Open Rate</span></div>
                <div className="c-metric"><span className="c-metric-value">{camp.reply_rate || 0}%</span><span className="c-metric-label">Reply Rate</span></div>
                <div className="c-metric highlight"><span className="c-metric-value">{camp.meetings_booked || 0}</span><span className="c-metric-label">Meetings</span></div>
                <div className="c-metric highlight"><span className="c-metric-value">{camp.opportunities || 0}</span><span className="c-metric-label">Opps</span></div>
              </div>
              <div className="campaign-card-actions" onClick={e => e.stopPropagation()}>
                <button className="btn-icon-small" onClick={() => setEditingSettings(camp)} title="Edit Settings"><Settings size={14} /></button>
                <button className="btn-icon-small" onClick={() => { setEditingId(camp.id); setEditData({...camp}); }} title="Edit Metrics"><Edit2 size={14} /></button>
                <button className="btn-icon-small danger" onClick={() => handleDelete(camp.id)} title="Delete"><Trash2 size={14} /></button>
              </div>
            </div>

            {expandedCampaign === camp.id && (
              <div className="campaign-expanded-content">
                {/* Strategy Brief Section */}
                {(camp.strategy_brief || camp.target_vertical || camp.hypothesis || camp.created_by === 'hermes') && (
                  <div className="strategy-brief-section">
                    <div className="strategy-header">
                      <div className="strategy-title-row">
                        <h4 className="strategy-title">Campaign Strategy</h4>
                        {camp.created_by === 'hermes' && <span className="agent-badge">Created by Hermes</span>}
                        {camp.approved_by && <span className="approved-badge">Approved by {camp.approved_by}</span>}
                        {camp.created_by === 'hermes' && !camp.approved_by && (
                          <button className="btn btn-v2 btn-v2-primary btn-sm" onClick={async (e) => {
                            e.stopPropagation();
                            try {
                              await api.put(`/campaigns/${camp.id}`, { approved_by: 'rodrigo', approved_at: new Date().toISOString() });
                              addToast('Campaign approved!', 'success');
                              fetchCampaigns();
                            } catch(err) { addToast(err.message, 'error'); }
                          }}>
                            <Check size={14} /> Approve Campaign
                          </button>
                        )}
                      </div>
                    </div>
                    <div className="strategy-cards">
                      {camp.target_vertical && (
                        <div className="strategy-card">
                          <div className="strategy-card-label">Target Vertical</div>
                          <div className="strategy-card-value">{camp.target_vertical}</div>
                        </div>
                      )}
                      {camp.target_icp && (
                        <div className="strategy-card">
                          <div className="strategy-card-label">Ideal Customer Profile</div>
                          <div className="strategy-card-value">{camp.target_icp}</div>
                        </div>
                      )}
                    </div>
                    {camp.strategy_brief && (
                      <div className="strategy-brief-body">
                        <div className="strategy-card-label" style={{marginBottom:6}}>Strategy Brief</div>
                        <p className="strategy-text">{camp.strategy_brief}</p>
                      </div>
                    )}
                    {camp.hypothesis && (
                      <div className="hypothesis-box">
                        <div className="strategy-card-label" style={{marginBottom:4}}>Hypothesis</div>
                        <p className="hypothesis-text">{camp.hypothesis}</p>
                      </div>
                    )}
                  </div>
                )}

                {/* Templates Section */}
                {loadingDetails ? (
                  <div className="loading-state" style={{padding: 40}}><Loader2 className="spin" size={24} /><span>Loading templates...</span></div>
                ) : campaignDetails?.template_breakdown && campaignDetails.template_breakdown.length > 0 ? (
                  <CampaignTemplateBreakdown breakdown={campaignDetails.template_breakdown} campaignId={camp.id} onUpdate={() => { fetchCampaigns(); fetchCampaignDetails(camp.id); }} addToast={addToast} />
                ) : (
                  <div className="no-templates-message">
                    <FileText size={32} />
                    <h4>No templates assigned</h4>
                    <p>Go to Templates and assign this campaign to a template to see performance data here.</p>
                  </div>
                )}
                {/* Sequence Analytics */}
                <SequenceAnalytics campaignName={camp.name} workspace={workspace} addToast={addToast} />

                <div className="campaign-push-bar">
                  <button className="btn btn-secondary btn-sm" onClick={e => { e.stopPropagation(); openRiPushModal(camp); }}>
                    <Send size={14} /> Push to ReachInbox
                  </button>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    )}

    <Modal isOpen={!!riPushModal} onClose={() => setRiPushModal(null)} title="Push to ReachInbox">
      {riPushModal && (
        <div className="ri-push-modal-body">
          <p className="ri-push-info">Workspace: <strong>{riPushModal.market || 'US'}</strong> · Campaign: <strong>{riPushModal.name}</strong></p>
          {riCampaignsLoading ? (
            <div className="ri-loading"><Loader2 size={16} className="spin" /> Loading ReachInbox campaigns...</div>
          ) : riCampaignsList.length > 0 ? (
            <div className="form-group">
              <label>ReachInbox Campaign</label>
              <select className="form-control" value={riSelectedId} onChange={e => setRiSelectedId(e.target.value)}>
                <option value="">— Select campaign —</option>
                {riCampaignsList.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
          ) : (
            <div className="form-group">
              <label>ReachInbox Campaign ID</label>
              <p className="help-text" style={{marginBottom: 6}}>API unavailable — enter the campaign ID manually.</p>
              <input type="number" className="form-control" value={riSelectedId} onChange={e => setRiSelectedId(e.target.value)} placeholder="e.g. 12345" min="1" />
            </div>
          )}
          <div className="modal-actions">
            <button className="btn btn-secondary" onClick={() => setRiPushModal(null)}>Cancel</button>
            <button className="btn btn-primary" onClick={executeRiPush} disabled={!riSelectedId || riPushing}>
              {riPushing ? <><Loader2 size={16} className="spin" /> Pushing...</> : <><Send size={14} /> Push Contacts</>}
            </button>
          </div>
        </div>
      )}
    </Modal>
    <Modal isOpen={editingId} onClose={() => setEditingId(null)} title="Edit Campaign Metrics" size="lg">
      <div className="metrics-edit-form">
        <div className="form-row">
          <div className="form-group"><label>Emails Sent</label><input type="number" value={editData.emails_sent||0} onChange={e=>setEditData({...editData,emails_sent:parseInt(e.target.value)||0})}/></div>
          <div className="form-group"><label>Emails Opened</label><input type="number" value={editData.emails_opened||0} onChange={e=>setEditData({...editData,emails_opened:parseInt(e.target.value)||0})}/></div>
        </div>
        <div className="form-row">
          <div className="form-group"><label>Emails Clicked</label><input type="number" value={editData.emails_clicked||0} onChange={e=>setEditData({...editData,emails_clicked:parseInt(e.target.value)||0})}/></div>
          <div className="form-group"><label>Emails Replied</label><input type="number" value={editData.emails_replied||0} onChange={e=>setEditData({...editData,emails_replied:parseInt(e.target.value)||0})}/></div>
        </div>
        <div className="form-row">
          <div className="form-group"><label>Emails Bounced</label><input type="number" value={editData.emails_bounced||0} onChange={e=>setEditData({...editData,emails_bounced:parseInt(e.target.value)||0})}/></div>
        </div>
        <p className="form-note">Meetings and Opportunities are calculated from template metrics.</p>
        <div className="form-group">
          <label>Campaign Status</label>
          <div className="status-selector">
            {['Active', 'Paused', 'Completed'].map(s => (
              <button key={s} type="button" className={`status-btn ${editData.status === s ? 'active ' + s.toLowerCase() : ''}`} onClick={() => setEditData({...editData, status: s})}>{s}</button>
            ))}
          </div>
        </div>
      </div>
      <div className="modal-actions">
        <button className="btn btn-secondary" onClick={() => setEditingId(null)}>Cancel</button>
        <button className="btn btn-primary" onClick={() => handleUpdate(editingId)}>Save Changes</button>
      </div>
    </Modal>
    <Modal isOpen={showCreate} onClose={() => setShowCreate(false)} title="Create New Campaign"><CampaignForm onSubmit={handleCreate} onCancel={() => setShowCreate(false)} /></Modal>
    <Modal isOpen={!!editingSettings} onClose={() => setEditingSettings(null)} title="Edit Campaign Settings">{editingSettings && <CampaignForm initial={editingSettings} onSubmit={handleUpdateSettings} onCancel={() => setEditingSettings(null)} />}</Modal>
    </>}
  </div>);
};

// Template Preview Tooltip
const TemplatePreviewTooltip = ({ template, children }) => {
  const [show, setShow] = useState(false);
  const [position, setPosition] = useState({ x: 0, y: 0 });
  const timeoutRef = useRef(null);

  const handleMouseEnter = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    setPosition({ x: rect.right + 10, y: rect.top });
    timeoutRef.current = setTimeout(() => setShow(true), 300);
  };

  const handleMouseLeave = () => {
    clearTimeout(timeoutRef.current);
    setShow(false);
  };

  // Strip HTML tags for plain text preview
  const stripHtml = (html) => {
    if (!html) return '';
    const tmp = document.createElement('div');
    tmp.innerHTML = html;
    return tmp.textContent || tmp.innerText || '';
  };

  return (
    <span className="template-preview-wrapper" onMouseEnter={handleMouseEnter} onMouseLeave={handleMouseLeave}>
      {children}
      {show && (template.subject || template.body) && (
        <div className="template-preview-tooltip" style={{ left: position.x, top: position.y }}>
          <div className="preview-subject"><strong>Subject:</strong> {template.subject || '(no subject)'}</div>
          <div className="preview-body">{stripHtml(template.body) || '(no content)'}</div>
        </div>
      )}
    </span>
  );
};

// Campaign Template Breakdown - Clay-style spreadsheet editing (no refresh, instant navigation)
const CampaignTemplateBreakdown = ({ breakdown, campaignId, onUpdate, addToast }) => {
  // Local state - completely independent, no external refresh
  const [localData, setLocalData] = useState(() => {
    const initial = {};
    breakdown.forEach(step => {
      step.variants.forEach(v => {
        initial[v.id] = {
          sent: v.sent || 0,
          opened: v.opened || 0,
          replied: v.replied || 0,
          opportunities: v.opportunities || 0,
          meetings: v.meetings || 0
        };
      });
    });
    return initial;
  });

  // Track which cell is being edited
  const [activeCell, setActiveCell] = useState(null); // { templateId, field }
  const [editValue, setEditValue] = useState('');
  const inputRef = useRef(null);

  // Build flat list of all cells for navigation
  const allCells = useMemo(() => {
    const cells = [];
    breakdown.forEach(step => {
      step.variants.forEach(v => {
        ['times_sent', 'times_opened', 'times_replied', 'opportunities', 'meetings'].forEach(field => {
          cells.push({ templateId: v.id, field });
        });
      });
    });
    return cells;
  }, [breakdown]);

  // Focus input when cell becomes active
  useEffect(() => {
    if (activeCell && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [activeCell]);

  // Calculate local totals
  const getStepTotals = (stepVariants) => {
    return stepVariants.reduce((acc, v) => {
      const local = localData[v.id] || {};
      return {
        sent: acc.sent + (local.sent || 0),
        opened: acc.opened + (local.opened || 0),
        replied: acc.replied + (local.replied || 0),
        opportunities: acc.opportunities + (local.opportunities || 0),
        meetings: acc.meetings + (local.meetings || 0)
      };
    }, { sent: 0, opened: 0, replied: 0, opportunities: 0, meetings: 0 });
  };

  const fieldToLocal = { times_sent: 'sent', times_opened: 'opened', times_replied: 'replied', opportunities: 'opportunities', meetings: 'meetings' };

  const getValue = (templateId, field) => {
    const localField = fieldToLocal[field] || field;
    return localData[templateId]?.[localField] || 0;
  };

  const startEdit = (templateId, field) => {
    const value = getValue(templateId, field);
    setActiveCell({ templateId, field });
    setEditValue(String(value));
  };

  const commitAndMove = (direction = null) => {
    if (!activeCell) return;

    const newValue = parseInt(editValue) || 0;
    const oldValue = getValue(activeCell.templateId, activeCell.field);

    // Update local state immediately
    if (newValue !== oldValue) {
      const localField = fieldToLocal[activeCell.field] || activeCell.field;
      setLocalData(prev => ({
        ...prev,
        [activeCell.templateId]: { ...prev[activeCell.templateId], [localField]: newValue }
      }));

      // Fire-and-forget save (no waiting, no callback)
      fetch(`${API}/campaigns/${campaignId}/templates/${activeCell.templateId}/metrics`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [activeCell.field]: newValue })
      }).catch(() => {}); // Silently ignore errors for speed
    }

    // Navigate to next/prev cell
    if (direction) {
      const currentIdx = allCells.findIndex(c => c.templateId === activeCell.templateId && c.field === activeCell.field);
      const nextIdx = direction === 'next' ? currentIdx + 1 : currentIdx - 1;
      if (nextIdx >= 0 && nextIdx < allCells.length) {
        const next = allCells[nextIdx];
        setActiveCell(next);
        setEditValue(String(getValue(next.templateId, next.field)));
        return; // Don't close, move to next
      }
    }

    setActiveCell(null);
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Tab') {
      e.preventDefault();
      commitAndMove(e.shiftKey ? 'prev' : 'next');
    } else if (e.key === 'Enter') {
      e.preventDefault();
      commitAndMove('next');
    } else if (e.key === 'Escape') {
      setActiveCell(null);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      // Move down 5 cells (one row)
      const currentIdx = allCells.findIndex(c => c.templateId === activeCell.templateId && c.field === activeCell.field);
      const nextIdx = currentIdx + 5;
      if (nextIdx < allCells.length) {
        commitAndMove();
        const next = allCells[nextIdx];
        startEdit(next.templateId, next.field);
      }
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      const currentIdx = allCells.findIndex(c => c.templateId === activeCell.templateId && c.field === activeCell.field);
      const nextIdx = currentIdx - 5;
      if (nextIdx >= 0) {
        commitAndMove();
        const next = allCells[nextIdx];
        startEdit(next.templateId, next.field);
      }
    }
  };

  const isActive = (templateId, field) => activeCell?.templateId === templateId && activeCell?.field === field;

  return (
    <div className="template-breakdown">
      <div className="breakdown-header">
        <h4>Template Performance Breakdown</h4>
        <span className="breakdown-hint">Click to edit | Tab/Enter = next | Shift+Tab = prev | Arrow keys navigate</span>
      </div>
      {breakdown.map((step, idx) => {
        const totals = getStepTotals(step.variants);
        return (
          <div key={idx} className="breakdown-step">
            <div className="breakdown-step-header">
              <div>
                <h5>{step.step_type}</h5>
                <span className="step-variant-count">{step.variants.length} variant{step.variants.length !== 1 ? 's' : ''}</span>
              </div>
              <div className="step-totals">
                <span className="step-total">Sent: {totals.sent.toLocaleString()}</span>
                <span className="step-total">Opened: {totals.opened.toLocaleString()}</span>
                <span className="step-total">Replied: {totals.replied.toLocaleString()}</span>
                <span className="step-total highlight">Opps: {totals.opportunities}</span>
                <span className="step-total highlight">Meetings: {totals.meetings}</span>
              </div>
            </div>
            <div className="breakdown-variants">
              <table className="variants-table spreadsheet">
                <thead>
                  <tr>
                    <th>Template</th>
                    <th>Variant</th>
                    <th>Sent</th>
                    <th>Opened</th>
                    <th>Replied</th>
                    <th>Opps</th>
                    <th>Meetings</th>
                  </tr>
                </thead>
                <tbody>
                  {step.variants.map(variant => (
                    <tr key={variant.id}>
                      <td><TemplatePreviewTooltip template={variant}><strong className="template-name-hover">{variant.name}</strong></TemplatePreviewTooltip></td>
                      <td><span className={`variant-badge variant-${variant.variant}`}>{variant.variant}</span></td>
                      {['times_sent', 'times_opened', 'times_replied', 'opportunities', 'meetings'].map(field => {
                        const isHighlight = field === 'opportunities' || field === 'meetings';
                        const active = isActive(variant.id, field);
                        return (
                          <td
                            key={field}
                            className={`metric-cell editable ${isHighlight ? 'highlight' : ''} ${active ? 'active' : ''}`}
                            onClick={() => startEdit(variant.id, field)}
                          >
                            {active ? (
                              <input
                                ref={inputRef}
                                type="number"
                                className="spreadsheet-input"
                                value={editValue}
                                onChange={e => setEditValue(e.target.value)}
                                onBlur={() => commitAndMove()}
                                onKeyDown={handleKeyDown}
                                min="0"
                              />
                            ) : (
                              getValue(variant.id, field)
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </div>
  );
};


// Sequence Analytics — step + variant performance view
const SequenceAnalytics = ({ campaignName, workspace, addToast }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchSequences = async () => {
      try {
        const res = await api.get(`/reachinbox/campaign-sequences?workspace=${workspace}`);
        if (res.campaigns && res.campaigns[campaignName]) {
          setData(res.campaigns[campaignName]);
        }
      } catch (e) { console.error(e); }
      setLoading(false);
    };
    fetchSequences();
  }, [campaignName, workspace]);

  if (loading) return <div className="loading-state" style={{padding:20}}><Loader2 className="spin" size={16} /><span style={{fontSize:13}}>Loading sequences...</span></div>;
  if (!data || !data.steps || Object.keys(data.steps).length === 0) return null;

  const steps = Object.entries(data.steps).sort(([a],[b]) => Number(a) - Number(b));

  return (
    <div className="sequence-analytics">
      <h4 className="sequence-title">Sequence Performance</h4>
      <div className="sequence-steps">
        {steps.map(([stepNum, step]) => {
          const totalSent = step.variants.reduce((s, v) => s + (v.sent || 0), 0);
          return (
            <div key={stepNum} className="sequence-step">
              <div className="step-header">
                <span className="step-badge">{step.type === 'initial' ? 'Initial' : `Follow-up ${stepNum - 1}`}</span>
                <span className="step-sent">{totalSent.toLocaleString()} sent</span>
                <span className="step-variants-count">{step.variants.length} variant{step.variants.length !== 1 ? 's' : ''}</span>
              </div>
              <div className="variant-bars">
                {step.variants.map((v, vi) => {
                  const pct = totalSent > 0 ? Math.round(100 * v.sent / totalSent) : 0;
                  return (
                    <div key={vi} className="variant-row">
                      <span className="variant-label">V{vi + 1}</span>
                      <div className="variant-bar-track">
                        <div className="variant-bar-fill" style={{width: `${pct}%`}} />
                      </div>
                      <span className="variant-count">{v.sent.toLocaleString()}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const CampaignForm = ({ onSubmit, onCancel, initial = {} }) => {
  const [data, setData] = useState({ name: '', description: '', country: '', status: 'Active', strategy_brief: '', target_vertical: '', target_icp: '', hypothesis: '', ...initial });
  const countries = ['Mexico', 'United States', 'Germany', 'Spain'];
  return (<form onSubmit={e => { e.preventDefault(); onSubmit(data); }} className="campaign-form-v2">
    <div className="form-group"><label>Campaign Name *</label><input type="text" value={data.name} onChange={e => setData({ ...data, name: e.target.value })} required placeholder="E.g., CE-SL: Roofing Companies 002 - US - ARKODE" /></div>
    <div className="form-group"><label>Description</label><textarea value={data.description || ''} onChange={e => setData({ ...data, description: e.target.value })} rows={2} placeholder="Brief description of this campaign..." /></div>
    <div className="form-row">
      <div className="form-group"><label>Target Vertical</label><input type="text" value={data.target_vertical || ''} onChange={e => setData({ ...data, target_vertical: e.target.value })} placeholder="E.g., Roofing, HVAC, Manufacturing" /></div>
      <div className="form-group"><label>Target ICP</label><input type="text" value={data.target_icp || ''} onChange={e => setData({ ...data, target_icp: e.target.value })} placeholder="E.g., Owners of 25-200 employee companies" /></div>
    </div>
    <div className="form-group">
      <label>Strategy Brief</label>
      <p className="form-hint">What are we testing? Why this audience? What messaging angle?</p>
      <textarea value={data.strategy_brief || ''} onChange={e => setData({ ...data, strategy_brief: e.target.value })} rows={4} placeholder="Describe the campaign strategy, messaging approach, and what you're trying to learn..." style={{fontFamily:'var(--font-body)', lineHeight:1.6}} />
    </div>
    <div className="form-group">
      <label>Hypothesis</label>
      <textarea value={data.hypothesis || ''} onChange={e => setData({ ...data, hypothesis: e.target.value })} rows={2} placeholder="E.g., Roofing company owners respond better to cost-savings messaging than growth messaging" />
    </div>
    <div className="form-row">
      <div className="form-group"><label>Workspace</label><select value={data.country || ''} onChange={e => setData({ ...data, country: e.target.value })}><option value="">Select country...</option>{countries.map(c => <option key={c} value={c}>{c}</option>)}</select></div>
      <div className="form-group"><label>Status</label><select value={data.status} onChange={e => setData({ ...data, status: e.target.value })}><option>Draft</option><option>Active</option><option>Paused</option><option>Completed</option></select></div>
    </div>
    <div className="modal-actions"><button type="button" className="btn btn-secondary" onClick={onCancel}>Cancel</button><button type="submit" className="btn btn-primary">{initial.id ? 'Save Changes' : 'Create Campaign'}</button></div>
  </form>);
};

// Bulk Assign Form for Templates
const BulkAssignForm = ({ campaigns, templateCount, onSubmit, onCancel }) => {
  const [selectedCampaigns, setSelectedCampaigns] = useState([]);
  const campaignOptions = campaigns.map(c => ({ id: c.id, name: c.name }));

  const handleSubmit = (e) => {
    e.preventDefault();
    if (selectedCampaigns.length === 0) return;
    onSubmit(selectedCampaigns);
  };

  return (
    <form onSubmit={handleSubmit}>
      <p className="form-help-text" style={{ marginBottom: '16px' }}>
        Select campaigns to assign the {templateCount} selected template{templateCount !== 1 ? 's' : ''} to.
        Templates will be added to selected campaigns without removing existing assignments.
      </p>
      <div className="form-group">
        <label>Select Campaigns</label>
        <MultiSelect
          options={campaignOptions}
          value={selectedCampaigns}
          onChange={setSelectedCampaigns}
          placeholder="Select campaigns..."
        />
      </div>
      <div className="modal-actions">
        <button type="button" className="btn btn-secondary" onClick={onCancel}>Cancel</button>
        <button type="submit" className="btn btn-primary" disabled={selectedCampaigns.length === 0}>
          <Briefcase size={16} /> Assign to {selectedCampaigns.length || 0} Campaign{selectedCampaigns.length !== 1 ? 's' : ''}
        </button>
      </div>
    </form>
  );
};

// Templates Page with Tabs - Premium Design
const TemplatesPage = () => {
  const { addToast } = useToast();
  const [templates, setTemplates] = useState([]);
  const [groupedData, setGroupedData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [showEdit, setShowEdit] = useState(null);
  const [activeTab, setActiveTab] = useState('all');
  const [viewMode, setViewMode] = useState('grouped'); // 'grouped', 'list', or 'cards'
  const [filterSteps, setFilterSteps] = useState([]);
  const [filterVariants, setFilterVariants] = useState([]);
  const [filterCampaigns, setFilterCampaigns] = useState([]);
  const [filterCountries, setFilterCountries] = useState([]);
  const [expandedSteps, setExpandedSteps] = useState(['Main', 'Followup 1', 'Followup 2', 'Followup 3']); // All expanded by default
  const [selected, setSelected] = useState(new Set());
  const [showBulkAssign, setShowBulkAssign] = useState(false);
  const { data: campaigns } = useData('/campaigns');

  // Toggle selection for a single template
  const toggleSelect = (id, e) => {
    if (e) e.stopPropagation();
    setSelected(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  // Toggle select all filtered templates
  const toggleSelectAll = () => {
    if (selected.size === filteredTemplates.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filteredTemplates.map(t => t.id)));
    }
  };

  // Clear selection
  const clearSelection = () => setSelected(new Set());

  // Filter options for multi-select
  const stepOptions = [
    { id: 'Main', name: 'Main' },
    { id: 'Followup 1', name: 'Followup 1' },
    { id: 'Followup 2', name: 'Followup 2' },
    { id: 'Followup 3', name: 'Followup 3' }
  ];
  const variantOptions = [
    { id: 'A', name: 'Variant A' },
    { id: 'B', name: 'Variant B' },
    { id: 'C', name: 'Variant C' },
    { id: 'D', name: 'Variant D' },
    { id: 'E', name: 'Variant E' }
  ];
  const countryOptions = [
    { id: 'Mexico', name: 'Mexico' },
    { id: 'United States', name: 'United States' },
    { id: 'Germany', name: 'Germany' },
    { id: 'Spain', name: 'Spain' }
  ];
  const campaignOptions = (campaigns?.data || []).map(c => ({ id: c.id, name: c.name }));

  const fetchTemplates = async () => {
    setLoading(true);
    try {
      const r = await api.get(`/templates${search ? `?search=${search}` : ''}`);
      setTemplates(r.data);
    } catch (e) {
      addToast(e.message, 'error');
    }
    setLoading(false);
  };

  const fetchGroupedTemplates = async () => {
    setLoading(true);
    try {
      const r = await api.get('/templates/grouped/by-step');
      setGroupedData(r.data);
    } catch (e) {
      addToast(e.message, 'error');
    }
    setLoading(false);
  };

  useEffect(() => {
    if (activeTab === 'all') {
      fetchTemplates();
      if (viewMode === 'grouped') {
        fetchGroupedTemplates();
      }
    } else {
      fetchGroupedTemplates();
    }
  }, [search, activeTab, viewMode]);

  const toggleStep = (stepType) => {
    setExpandedSteps(prev =>
      prev.includes(stepType)
        ? prev.filter(s => s !== stepType)
        : [...prev, stepType]
    );
  };

  const handleCreate = async (data) => { try { await api.post('/templates', data); addToast('Template created successfully!', 'success'); setShowCreate(false); fetchTemplates(); if (activeTab === 'data') fetchGroupedTemplates(); } catch (e) { addToast(e.message, 'error'); } };
  const handleUpdate = async (id, data) => { try { await api.put(`/templates/${id}`, data); addToast('Template updated!', 'success'); setShowEdit(null); fetchTemplates(); if (activeTab === 'data') fetchGroupedTemplates(); } catch (e) { addToast(e.message, 'error'); } };
  const handleDelete = async (id) => { if (!window.confirm('Are you sure you want to delete this template?')) return; try { await api.delete(`/templates/${id}`); addToast('Template deleted', 'success'); fetchTemplates(); if (activeTab === 'data') fetchGroupedTemplates(); } catch (e) { addToast(e.message, 'error'); } };

  // Bulk assign templates to campaigns
  const handleBulkAssign = async (campaignIds) => {
    try {
      await api.post('/templates/bulk/assign-campaigns', {
        template_ids: Array.from(selected),
        campaign_ids: campaignIds
      });
      addToast(`Assigned ${selected.size} template${selected.size !== 1 ? 's' : ''} to ${campaignIds.length} campaign${campaignIds.length !== 1 ? 's' : ''}`, 'success');
      setShowBulkAssign(false);
      clearSelection();
      fetchTemplates();
    } catch (e) {
      addToast(e.message, 'error');
    }
  };

  const copyToClipboard = async (template) => {
    // Convert Quill's HTML for Gmail compatibility
    let cleanBody = (template.body || '')
      .replace(/<p><br><\/p>/g, '<br><br>')       // Empty paragraphs become double line break (blank line)
      .replace(/<p><br\/><\/p>/g, '<br><br>')     // Empty paragraphs (self-closing br)
      .replace(/<\/p>\s*<p>/g, '<br><br>')        // Paragraph breaks become double line breaks (blank line between)
      .replace(/<p>/g, '')                         // Remove opening p tags
      .replace(/<\/p>/g, '');                      // Remove closing p tags

    // Create HTML version with subject and body
    const subject = template.subject || '';
    const htmlContent = `<div><strong>Subject:</strong> ${subject}</div><br><br>${cleanBody}`;

    // Create plain text version as fallback
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = template.body || '';
    const plainBody = tempDiv.textContent || tempDiv.innerText || '';
    const plainText = `Subject: ${subject}\n\n${plainBody}`;

    await copyHtmlToClipboard(htmlContent, plainText);
    addToast('Copied to clipboard with formatting!', 'success');
  };

  const variables = ['{{firstName}}', '{{lastName}}', '{{companyName}}', '{{Headline}}', '{{accountSignature}}'];

  // Filter templates
  const filteredTemplates = templates.filter(t => {
    if (filterSteps.length > 0 && !filterSteps.includes(t.step_type)) return false;
    if (filterVariants.length > 0 && !filterVariants.includes(t.variant)) return false;
    if (filterCampaigns.length > 0) {
      const tCamps = (t.campaign_ids || []).map(Number);
      const filterCampsNum = filterCampaigns.map(Number);
      if (!filterCampsNum.some(c => tCamps.includes(c))) return false;
    }
    if (filterCountries.length > 0) {
      const tCountries = (t.campaigns || []).map(c => c.country).filter(Boolean);
      if (!filterCountries.some(c => tCountries.includes(c))) return false;
    }
    return true;
  });

  // Get unique steps and count
  const stepCounts = templates.reduce((acc, t) => { acc[t.step_type] = (acc[t.step_type] || 0) + 1; return acc; }, {});

  return (<div className="page templates-page">
    <div className="page-header">
      <div>
        <h1>Email Templates</h1>
        <p className="subtitle">Create, manage and optimize your cold email sequences</p>
      </div>
      <div className="header-actions">
        <button className="btn btn-secondary" onClick={() => activeTab === 'all' ? fetchTemplates() : fetchGroupedTemplates()}><RefreshCw size={16} /> Refresh</button>
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}><Plus size={16} /> New Template</button>
      </div>
    </div>

    {/* Tabs */}
    <div className="tabs-container">
      <div className="tabs">
        <button className={`tab ${activeTab === 'all' ? 'active' : ''}`} onClick={() => setActiveTab('all')}>
          <FileText size={16} /> All Templates
        </button>
        <button className={`tab ${activeTab === 'data' ? 'active' : ''}`} onClick={() => setActiveTab('data')}>
          <LayoutDashboard size={16} /> Performance View
        </button>
      </div>
    </div>

    {activeTab === 'all' ? (
      <>
        <div className="templates-toolbar">
          <div className="search-box"><Search size={18} /><input type="text" placeholder="Search templates..." value={search} onChange={e => setSearch(e.target.value)} /></div>
          <div className="template-filters">
            <MultiSelect
              options={stepOptions}
              value={filterSteps}
              onChange={setFilterSteps}
              placeholder="All Steps"
            />
            <MultiSelect
              options={variantOptions}
              value={filterVariants}
              onChange={setFilterVariants}
              placeholder="All Variants"
            />
            <MultiSelect
              options={campaignOptions}
              value={filterCampaigns}
              onChange={setFilterCampaigns}
              placeholder="All Campaigns"
            />
            <MultiSelect
              options={countryOptions}
              value={filterCountries}
              onChange={setFilterCountries}
              placeholder="All Countries"
            />
            <div className="view-toggle">
              <button className={`view-toggle-btn ${viewMode === 'grouped' ? 'active' : ''}`} onClick={() => setViewMode('grouped')} title="Grouped by Step"><Layers size={18} /></button>
              <button className={`view-toggle-btn ${viewMode === 'list' ? 'active' : ''}`} onClick={() => setViewMode('list')} title="List View"><List size={18} /></button>
              <button className={`view-toggle-btn ${viewMode === 'cards' ? 'active' : ''}`} onClick={() => setViewMode('cards')} title="Card View"><LayoutGrid size={18} /></button>
            </div>
          </div>
        </div>

        {/* Bulk Action Bar */}
        {selected.size > 0 && (
          <div className="bulk-action-bar">
            <div className="bulk-action-left">
              <input type="checkbox" checked={selected.size === filteredTemplates.length} onChange={toggleSelectAll} />
              <span>{selected.size} template{selected.size !== 1 ? 's' : ''} selected</span>
            </div>
            <div className="bulk-action-right">
              <button className="btn btn-primary btn-sm" onClick={() => setShowBulkAssign(true)}>
                <Briefcase size={14} /> Assign to Campaigns
              </button>
              <button className="btn btn-secondary btn-sm" onClick={clearSelection}>Clear</button>
            </div>
          </div>
        )}

        {loading ? (
          <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading templates...</span></div>
        ) : filteredTemplates.length === 0 ? (
          <div className="empty-templates-state">
            <div className="empty-icon"><FileText size={48} /></div>
            <h3>{templates.length === 0 ? 'No templates yet' : 'No templates match your filters'}</h3>
            <p>{templates.length === 0 ? 'Create your first email template to start building your outreach sequences.' : 'Try adjusting your filters to see more templates.'}</p>
            {templates.length === 0 && <button className="btn btn-primary" onClick={() => setShowCreate(true)}><Plus size={16} /> Create First Template</button>}
          </div>
        ) : viewMode === 'list' ? (
          /* List View - Similar to Campaigns */
          <div className="templates-list">
            {filteredTemplates.map(t => (
              <div key={t.id} className={`template-list-item ${t.is_winner ? 'winner' : ''} ${selected.has(t.id) ? 'selected' : ''}`} onClick={() => setShowEdit(t)}>
                <div className="template-list-left">
                  <input
                    type="checkbox"
                    checked={selected.has(t.id)}
                    onChange={(e) => toggleSelect(t.id, e)}
                    onClick={(e) => e.stopPropagation()}
                    className="template-checkbox"
                  />
                  <span className={`variant-badge variant-${t.variant}`}>{t.variant}</span>
                  <div className="template-list-info">
                    <div className="template-list-header">
                      <h4 className="template-list-name">{t.name}</h4>
                      {t.is_winner && <span className="winner-badge-sm"><Trophy size={12} /> Winner</span>}
                    </div>
                    <div className="template-list-meta">
                      <span className="step-tag">{t.step_type}</span>
                      {t.subject && <span className="subject-preview">Subject: {t.subject.length > 50 ? t.subject.substring(0, 50) + '...' : t.subject}</span>}
                      {t.campaign_ids?.length > 0 && (
                        <span className="campaign-count-badge" data-tooltip={t.campaign_names}>
                          <Briefcase size={11} />
                          {t.campaign_ids.length}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="template-list-metrics">
                  <div className="list-metric">
                    <span className="list-metric-value">{t.total_sent || 0}</span>
                    <span className="list-metric-label">Sent</span>
                  </div>
                  <div className="list-metric">
                    <span className="list-metric-value">{t.total_opened || 0}</span>
                    <span className="list-metric-label">Opened</span>
                  </div>
                  <div className="list-metric">
                    <span className="list-metric-value">{t.total_replied || 0}</span>
                    <span className="list-metric-label">Replied</span>
                  </div>
                  <div className="list-metric highlight">
                    <span className="list-metric-value">{t.opportunities || 0}</span>
                    <span className="list-metric-label">Opps</span>
                  </div>
                  <div className="list-metric highlight">
                    <span className="list-metric-value">{t.meetings || 0}</span>
                    <span className="list-metric-label">Meetings</span>
                  </div>
                </div>
                <div className="template-list-actions" onClick={e => e.stopPropagation()}>
                  <button className="btn-icon-small" onClick={() => copyToClipboard(t)} title="Copy"><Copy size={14} /></button>
                  <button className="btn-icon-small" onClick={() => setShowEdit(t)} title="Edit"><Edit2 size={14} /></button>
                  <button className="btn-icon-small danger" onClick={() => handleDelete(t.id)} title="Delete"><Trash2 size={14} /></button>
                </div>
              </div>
            ))}
          </div>
        ) : viewMode === 'cards' ? (
          /* Card View */
          <div className="templates-grid">
            {filteredTemplates.map(t => (
              <div key={t.id} className={`template-card-v2 ${t.is_winner ? 'winner' : ''} ${selected.has(t.id) ? 'selected' : ''}`} onClick={() => setShowEdit(t)}>
                <div className="template-card-top">
                  <input
                    type="checkbox"
                    checked={selected.has(t.id)}
                    onChange={(e) => toggleSelect(t.id, e)}
                    onClick={(e) => e.stopPropagation()}
                    className="template-checkbox"
                  />
                  <div className="template-badges">
                    <span className={`variant-badge-lg variant-${t.variant}`}>{t.variant}</span>
                    <span className="step-badge">{t.step_type}</span>
                    {t.is_winner && <span className="winner-badge-sm"><Trophy size={12} /> Winner</span>}
                  </div>
                  <div className="template-actions-menu" onClick={e => e.stopPropagation()}>
                    <button className="btn-icon-small" onClick={() => copyToClipboard(t)} title="Copy"><Copy size={14} /></button>
                    <button className="btn-icon-small" onClick={() => setShowEdit(t)} title="Edit"><Edit2 size={14} /></button>
                    <button className="btn-icon-small danger" onClick={() => handleDelete(t.id)} title="Delete"><Trash2 size={14} /></button>
                  </div>
                </div>
                <div className="template-card-content">
                  <h4 className="template-name">{t.name}</h4>
                  {t.subject && <div className="template-subject"><strong>Subject:</strong> <span>{t.subject}</span></div>}
                  <div className="template-body-preview">{t.body ? (t.body.length > 150 ? t.body.substring(0, 150) + '...' : t.body) : 'No content yet'}</div>
                </div>
                {t.campaign_names && (
                  <div className="template-campaigns">
                    <span className="campaigns-label">Campaigns:</span>
                    <div className="campaign-tags">
                      {t.campaign_names.split(', ').slice(0, 3).map((name, i) => (
                        <span key={i} className="campaign-tag">{name}</span>
                      ))}
                      {t.campaign_names.split(', ').length > 3 && <span className="campaign-tag more">+{t.campaign_names.split(', ').length - 3}</span>}
                    </div>
                  </div>
                )}
                <div className="template-card-footer">
                  <div className="template-metrics-row">
                    <div className="metric-item">
                      <span className="metric-val">{t.total_sent || 0}</span>
                      <span className="metric-lbl">Sent</span>
                    </div>
                    <div className="metric-item">
                      <span className="metric-val">{t.total_opened || 0}</span>
                      <span className="metric-lbl">Opened</span>
                    </div>
                    <div className="metric-item">
                      <span className="metric-val">{t.total_replied || 0}</span>
                      <span className="metric-lbl">Replied</span>
                    </div>
                    <div className="metric-item highlight">
                      <span className="metric-val">{t.opportunities || 0}</span>
                      <span className="metric-lbl">Opps</span>
                    </div>
                    <div className="metric-item highlight">
                      <span className="metric-val">{t.meetings || 0}</span>
                      <span className="metric-lbl">Meetings</span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          /* Grouped by Step View */
          <div className="templates-grouped">
            {groupedData ? (
              ['Main', 'Followup 1', 'Followup 2', 'Followup 3'].map(stepType => {
                const stepData = groupedData.find(s => s.step_type === stepType);
                // Apply same filters to grouped view (including search)
                const templates = (stepData?.variants || []).filter(t => {
                  // Search filter
                  if (search) {
                    const s = search.toLowerCase();
                    if (!t.name?.toLowerCase().includes(s) && !t.subject?.toLowerCase().includes(s)) return false;
                  }
                  if (filterSteps.length > 0 && !filterSteps.includes(t.step_type)) return false;
                  if (filterVariants.length > 0 && !filterVariants.includes(t.variant)) return false;
                  if (filterCampaigns.length > 0) {
                    const tCamps = (t.campaign_ids || []).map(Number);
                    const filterCampsNum = filterCampaigns.map(Number);
                    if (!filterCampsNum.some(c => tCamps.includes(c))) return false;
                  }
                  if (filterCountries.length > 0) {
                    const tCountries = (t.campaigns || []).map(c => c.country).filter(Boolean);
                    if (!filterCountries.some(c => tCountries.includes(c))) return false;
                  }
                  return true;
                });
                const isExpanded = expandedSteps.includes(stepType);
                return (
                  <div key={stepType} className="template-step-group">
                    <div className={`step-group-header ${isExpanded ? 'expanded' : ''}`} onClick={() => toggleStep(stepType)}>
                      <div className="step-group-title">
                        {isExpanded ? <ChevronDown size={20} /> : <ChevronRight size={20} />}
                        <span className="step-name">{stepType}</span>
                        <span className="step-count">{templates.length} template{templates.length !== 1 ? 's' : ''}</span>
                      </div>
                    </div>
                    {isExpanded && (
                      <div className="step-templates">
                        {templates.length === 0 ? (
                          <div className="step-empty">No templates for this step</div>
                        ) : (
                          templates.map(t => (
                            <div key={t.id} className={`template-list-item ${t.is_winner ? 'winner' : ''} ${selected.has(t.id) ? 'selected' : ''}`} onClick={() => setShowEdit(t)}>
                              <div className="template-list-left">
                                <input
                                  type="checkbox"
                                  checked={selected.has(t.id)}
                                  onChange={(e) => toggleSelect(t.id, e)}
                                  onClick={(e) => e.stopPropagation()}
                                  className="template-checkbox"
                                />
                                <span className={`variant-badge variant-${t.variant}`}>{t.variant}</span>
                                <div className="template-list-info">
                                  <div className="template-list-header">
                                    <h4 className="template-list-name">{t.name}</h4>
                                    {t.is_winner && <span className="winner-badge-sm"><Trophy size={12} /> Winner</span>}
                                  </div>
                                  <div className="template-list-meta">
                                    {t.subject && <span className="subject-preview">Subject: {t.subject?.length > 50 ? t.subject.substring(0, 50) + '...' : t.subject}</span>}
                                    {t.campaign_ids?.length > 0 && (
                                      <span className="campaign-count-badge" data-tooltip={t.campaign_names}>
                                        <Briefcase size={11} />
                                        {t.campaign_ids.length}
                                      </span>
                                    )}
                                  </div>
                                </div>
                              </div>
                              <div className="template-list-metrics">
                                <div className="list-metric"><span className="list-metric-value">{t.total_sent || 0}</span><span className="list-metric-label">Sent</span></div>
                                <div className="list-metric"><span className="list-metric-value">{t.total_opened || 0}</span><span className="list-metric-label">Opened</span></div>
                                <div className="list-metric"><span className="list-metric-value">{t.total_replied || 0}</span><span className="list-metric-label">Replied</span></div>
                                <div className="list-metric highlight"><span className="list-metric-value">{t.opportunities || 0}</span><span className="list-metric-label">Opps</span></div>
                                <div className="list-metric highlight"><span className="list-metric-value">{t.meetings || 0}</span><span className="list-metric-label">Meetings</span></div>
                              </div>
                              <div className="template-list-actions" onClick={e => e.stopPropagation()}>
                                <button className="btn-icon-small" onClick={() => copyToClipboard(t)} title="Copy"><Copy size={14} /></button>
                                <button className="btn-icon-small" onClick={() => setShowEdit(t)} title="Edit"><Edit2 size={14} /></button>
                                <button className="btn-icon-small danger" onClick={() => handleDelete(t.id)} title="Delete"><Trash2 size={14} /></button>
                              </div>
                            </div>
                          ))
                        )}
                      </div>
                    )}
                  </div>
                );
              })
            ) : (
              <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading templates...</span></div>
            )}
          </div>
        )}
      </>
    ) : (
      <TemplateDataView data={groupedData} loading={loading} onEdit={setShowEdit} onDelete={handleDelete} copyToClipboard={copyToClipboard} />
    )}

    <Modal isOpen={showCreate} onClose={() => setShowCreate(false)} title="Create New Template" size="xl"><TemplateForm campaigns={campaigns?.data || []} onSubmit={handleCreate} onCancel={() => setShowCreate(false)} variables={variables} /></Modal>
    <Modal isOpen={!!showEdit} onClose={() => setShowEdit(null)} title="Edit Template" size="xl">{showEdit && <TemplateForm campaigns={campaigns?.data || []} initial={showEdit} onSubmit={(d) => handleUpdate(showEdit.id, d)} onCancel={() => setShowEdit(null)} variables={variables} />}</Modal>

    {/* Bulk Assign Modal */}
    <Modal isOpen={showBulkAssign} onClose={() => setShowBulkAssign(false)} title={`Assign ${selected.size} Template${selected.size !== 1 ? 's' : ''} to Campaigns`}>
      <BulkAssignForm
        campaigns={campaigns?.data || []}
        templateCount={selected.size}
        onSubmit={handleBulkAssign}
        onCancel={() => setShowBulkAssign(false)}
      />
    </Modal>
  </div>);
};

// Template Data View Component - Premium Performance Analytics
const TemplateDataView = ({ data, loading, onEdit, onDelete, copyToClipboard }) => {
  const [sortBy, setSortBy] = useState('oppRate');
  const [sortDir, setSortDir] = useState('desc');

  if (loading) return <div className="loading-state"><Loader2 className="spin" size={32} /><span>Loading performance data...</span></div>;
  if (!data || data.length === 0) return (
    <div className="empty-templates-state">
      <div className="empty-icon"><LayoutDashboard size={48} /></div>
      <h3>No template data yet</h3>
      <p>Create templates and assign them to campaigns to see performance analytics here.</p>
    </div>
  );

  // Calculate overall stats
  const allVariants = data.flatMap(s => s.variants);
  const totalSent = allVariants.reduce((sum, v) => sum + (v.total_sent || 0), 0);
  const totalOpened = allVariants.reduce((sum, v) => sum + (v.total_opened || 0), 0);
  const totalReplied = allVariants.reduce((sum, v) => sum + (v.total_replied || 0), 0);
  const totalOpps = allVariants.reduce((sum, v) => sum + (v.opportunities || 0), 0);
  const totalMeetings = allVariants.reduce((sum, v) => sum + (v.meetings || 0), 0);

  // Calculate rates
  const calcRate = (num, denom) => denom > 0 ? ((num / denom) * 100) : 0;
  const avgOpenRate = calcRate(totalOpened, totalSent);
  const avgReplyRate = calcRate(totalReplied, totalSent);
  const avgReplyToOpen = calcRate(totalReplied, totalOpened);
  const avgOppRate = calcRate(totalOpps, totalSent);
  const avgMeetingRate = calcRate(totalMeetings, totalSent);
  const avgOppConversion = calcRate(totalOpps, totalReplied);

  // Enrich variants with calculated metrics
  const enrichedVariants = allVariants.map(v => {
    const sent = v.total_sent || 0;
    const opened = v.total_opened || 0;
    const replied = v.total_replied || 0;
    const opps = v.opportunities || 0;
    const meetings = v.meetings || 0;
    return {
      ...v,
      sent, opened, replied, opps, meetings,
      openRate: calcRate(opened, sent),
      replyRate: calcRate(replied, sent),
      replyToOpen: calcRate(replied, opened),
      oppRate: calcRate(opps, sent),
      meetingRate: calcRate(meetings, sent),
      oppConversion: calcRate(opps, replied),
      meetingConversion: calcRate(meetings, opps)
    };
  }).filter(v => v.sent > 0);

  // Sort variants
  const sortedVariants = [...enrichedVariants].sort((a, b) => {
    const aVal = a[sortBy] || 0;
    const bVal = b[sortBy] || 0;
    return sortDir === 'desc' ? bVal - aVal : aVal - bVal;
  });

  // Find top performers
  const topByOpps = enrichedVariants.length > 0 ? enrichedVariants.reduce((best, v) => v.oppRate > best.oppRate ? v : best, enrichedVariants[0]) : null;
  const topByMeetings = enrichedVariants.length > 0 ? enrichedVariants.reduce((best, v) => v.meetingRate > best.meetingRate ? v : best, enrichedVariants[0]) : null;
  const topByReplyToOpen = enrichedVariants.length > 0 ? enrichedVariants.reduce((best, v) => v.replyToOpen > best.replyToOpen ? v : best, enrichedVariants[0]) : null;

  const handleSort = (col) => {
    if (sortBy === col) setSortDir(sortDir === 'desc' ? 'asc' : 'desc');
    else { setSortBy(col); setSortDir('desc'); }
  };

  const SortIcon = ({ col }) => (
    <span className={`sort-icon ${sortBy === col ? 'active' : ''}`}>
      {sortBy === col ? (sortDir === 'desc' ? <ChevronDown size={14} /> : <ChevronUp size={14} />) : <ChevronDown size={14} />}
    </span>
  );

  const RateBadge = ({ value, avg, suffix = '%' }) => {
    const diff = value - avg;
    const isGood = diff > 0;
    const isBad = diff < -2;
    return (
      <span className={`rate-badge ${isGood ? 'good' : isBad ? 'bad' : 'neutral'}`}>
        {value.toFixed(1)}{suffix}
      </span>
    );
  };

  return (
    <div className="data-view-container-v2">
      {/* KPI Summary Cards */}
      <div className="kpi-grid">
        <div className="kpi-card">
          <div className="kpi-header">
            <Mail size={20} />
            <span>Open Rate</span>
          </div>
          <div className="kpi-value">{avgOpenRate.toFixed(1)}%</div>
          <div className="kpi-detail">{totalOpened.toLocaleString()} of {totalSent.toLocaleString()} sent</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-header">
            <MessageSquare size={20} />
            <span>Reply Rate</span>
          </div>
          <div className="kpi-value">{avgReplyRate.toFixed(1)}%</div>
          <div className="kpi-detail">{totalReplied.toLocaleString()} replies received</div>
        </div>
        <div className="kpi-card highlight">
          <div className="kpi-header">
            <Target size={20} />
            <span>Opportunity Rate</span>
          </div>
          <div className="kpi-value">{avgOppRate.toFixed(2)}%</div>
          <div className="kpi-detail">{totalOpps} opps from {totalSent.toLocaleString()} emails</div>
        </div>
        <div className="kpi-card highlight">
          <div className="kpi-header">
            <Trophy size={20} />
            <span>Meeting Rate</span>
          </div>
          <div className="kpi-value">{avgMeetingRate.toFixed(2)}%</div>
          <div className="kpi-detail">{totalMeetings} meetings booked</div>
        </div>
      </div>

      {/* Insights Row */}
      {enrichedVariants.length > 0 && (
        <div className="insights-row">
          <div className="insight-card">
            <div className="insight-icon best"><Target size={18} /></div>
            <div className="insight-content">
              <span className="insight-label">Best Pipeline Generator</span>
              <span className="insight-value">{topByOpps?.name || '-'}</span>
              <span className="insight-detail">{topByOpps?.oppRate.toFixed(2)}% opp rate ({topByOpps?.opps} opps)</span>
            </div>
          </div>
          <div className="insight-card">
            <div className="insight-icon best"><Trophy size={18} /></div>
            <div className="insight-content">
              <span className="insight-label">Best Meeting Converter</span>
              <span className="insight-value">{topByMeetings?.name || '-'}</span>
              <span className="insight-detail">{topByMeetings?.meetingRate.toFixed(2)}% meeting rate ({topByMeetings?.meetings} meetings)</span>
            </div>
          </div>
          <div className="insight-card">
            <div className="insight-icon"><MessageSquare size={18} /></div>
            <div className="insight-content">
              <span className="insight-label">Best Body Copy</span>
              <span className="insight-value">{topByReplyToOpen?.name || '-'}</span>
              <span className="insight-detail">{topByReplyToOpen?.replyToOpen.toFixed(1)}% reply-to-open rate</span>
            </div>
          </div>
        </div>
      )}

      {/* Conversion Funnel */}
      <div className="funnel-card">
        <h4>Conversion Funnel</h4>
        <div className="funnel-row">
          <div className="funnel-stage">
            <span className="funnel-num">{totalSent.toLocaleString()}</span>
            <span className="funnel-label">Sent</span>
          </div>
          <div className="funnel-arrow"><ChevronRight size={20} /></div>
          <div className="funnel-stage">
            <span className="funnel-num">{totalOpened.toLocaleString()}</span>
            <span className="funnel-label">Opened</span>
            <span className="funnel-rate">{avgOpenRate.toFixed(1)}%</span>
          </div>
          <div className="funnel-arrow"><ChevronRight size={20} /></div>
          <div className="funnel-stage">
            <span className="funnel-num">{totalReplied.toLocaleString()}</span>
            <span className="funnel-label">Replied</span>
            <span className="funnel-rate">{avgReplyToOpen.toFixed(1)}% of opened</span>
          </div>
          <div className="funnel-arrow"><ChevronRight size={20} /></div>
          <div className="funnel-stage highlight">
            <span className="funnel-num">{totalOpps}</span>
            <span className="funnel-label">Opportunities</span>
            <span className="funnel-rate">{avgOppConversion.toFixed(1)}% of replied</span>
          </div>
          <div className="funnel-arrow"><ChevronRight size={20} /></div>
          <div className="funnel-stage highlight">
            <span className="funnel-num">{totalMeetings}</span>
            <span className="funnel-label">Meetings</span>
            <span className="funnel-rate">{calcRate(totalMeetings, totalOpps).toFixed(1)}% of opps</span>
          </div>
        </div>
      </div>

      {/* Detailed Performance Table */}
      <div className="performance-table-card">
        <div className="table-header">
          <h4>Template Performance Comparison</h4>
          <span className="table-hint">Click column headers to sort. Green = above avg, Red = below avg</span>
        </div>
        <div className="table-scroll">
          <table className="performance-table">
            <thead>
              <tr>
                <th>Template</th>
                <th>Step</th>
                <th className="sortable" onClick={() => handleSort('sent')}>Sent <SortIcon col="sent" /></th>
                <th className="sortable" onClick={() => handleSort('openRate')}>Open % <SortIcon col="openRate" /></th>
                <th className="sortable" onClick={() => handleSort('replyRate')}>Reply % <SortIcon col="replyRate" /></th>
                <th className="sortable" onClick={() => handleSort('replyToOpen')}>Reply/Open <SortIcon col="replyToOpen" /></th>
                <th className="sortable highlight-col" onClick={() => handleSort('oppRate')}>Opp % <SortIcon col="oppRate" /></th>
                <th className="sortable highlight-col" onClick={() => handleSort('meetingRate')}>Meeting % <SortIcon col="meetingRate" /></th>
                <th className="sortable" onClick={() => handleSort('oppConversion')}>Opp/Reply <SortIcon col="oppConversion" /></th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {sortedVariants.map((v, idx) => (
                <tr key={v.id} className={idx === 0 && sortBy === 'oppRate' ? 'top-performer' : ''}>
                  <td>
                    <div className="template-cell">
                      <span className={`variant-badge variant-${v.variant}`}>{v.variant}</span>
                      <div>
                        <strong>{v.name}</strong>
                        {v.is_winner && <Trophy size={14} className="inline-trophy" />}
                      </div>
                    </div>
                  </td>
                  <td><span className="step-tag">{v.step_type}</span></td>
                  <td>{v.sent.toLocaleString()}</td>
                  <td><RateBadge value={v.openRate} avg={avgOpenRate} /></td>
                  <td><RateBadge value={v.replyRate} avg={avgReplyRate} /></td>
                  <td><RateBadge value={v.replyToOpen} avg={avgReplyToOpen} /></td>
                  <td className="highlight-col"><RateBadge value={v.oppRate} avg={avgOppRate} /></td>
                  <td className="highlight-col"><RateBadge value={v.meetingRate} avg={avgMeetingRate} /></td>
                  <td><RateBadge value={v.oppConversion} avg={avgOppConversion} /></td>
                  <td className="action-cell">
                    <button className="btn-icon-small" onClick={() => copyToClipboard(v)} title="Copy"><Copy size={14} /></button>
                    <button className="btn-icon-small" onClick={() => onEdit(v)} title="Edit"><Edit2 size={14} /></button>
                    <button className="btn-icon-small danger" onClick={() => onDelete(v.id)} title="Delete"><Trash2 size={14} /></button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Legend */}
      <div className="table-legend">
        <div className="legend-item"><span className="legend-dot good"></span> Above average</div>
        <div className="legend-item"><span className="legend-dot neutral"></span> Average</div>
        <div className="legend-item"><span className="legend-dot bad"></span> Below average</div>
        <div className="legend-item"><span className="legend-note">Reply/Open = Body copy effectiveness | Opp/Reply = Conversation quality</span></div>
      </div>
    </div>
  );
};

// Rich Text Editor Component for Email Templates (Gmail-style toolbar)
const RichTextEditor = ({ value, onChange, placeholder, variables, onInsertVariable }) => {
  const quillRef = useRef(null);

  const modules = {
    toolbar: {
      container: [
        [{ 'font': [] }],
        [{ 'size': ['small', false, 'large', 'huge'] }],
        ['bold', 'italic', 'underline'],
        [{ 'color': [] }],
        [{ 'list': 'ordered'}, { 'list': 'bullet' }],
        ['link'],
        ['clean']
      ]
    }
  };

  const formats = ['font', 'size', 'bold', 'italic', 'underline', 'color', 'list', 'bullet', 'link'];

  const insertVariable = (variable) => {
    if (quillRef.current) {
      const editor = quillRef.current.getEditor();
      const range = editor.getSelection(true);
      editor.insertText(range ? range.index : editor.getLength(), variable);
      editor.setSelection(range ? range.index + variable.length : editor.getLength());
    }
  };

  return (
    <div className="rich-text-editor">
      <ReactQuill
        ref={quillRef}
        theme="snow"
        value={value || ''}
        onChange={onChange}
        modules={modules}
        formats={formats}
        placeholder={placeholder}
      />
      <div className="variable-chips">
        {variables.map(v => (
          <span key={v} className="variable-chip" onClick={() => insertVariable(v)}>{v}</span>
        ))}
      </div>
    </div>
  );
};

// Helper function to copy HTML content to clipboard (preserves formatting for Gmail)
const copyHtmlToClipboard = async (html, plainText) => {
  try {
    // Create a blob with HTML content
    const htmlBlob = new Blob([html], { type: 'text/html' });
    const textBlob = new Blob([plainText], { type: 'text/plain' });

    // Use the Clipboard API with both HTML and plain text
    await navigator.clipboard.write([
      new ClipboardItem({
        'text/html': htmlBlob,
        'text/plain': textBlob
      })
    ]);
    return true;
  } catch (err) {
    // Fallback to plain text copy
    await navigator.clipboard.writeText(plainText);
    return true;
  }
};

const TemplateForm = ({ campaigns, onSubmit, onCancel, initial = {}, variables }) => {
  const initialCampaignIds = initial.campaign_ids || (initial.campaigns ? initial.campaigns.map(c => c.id) : []);
  const [data, setData] = useState({ name: '', variant: 'A', step_type: 'Main', country: '', subject: '', body: '', campaign_ids: initialCampaignIds, ...initial, campaign_ids: initialCampaignIds });
  const insertVariable = (v, field) => { setData({ ...data, [field]: (data[field] || '') + v }); };
  const countries = ['Mexico', 'United States', 'Germany', 'Spain'];

  return (
    <form onSubmit={e => { e.preventDefault(); onSubmit(data); }} className="template-form">
      <div className="form-row">
        <div className="form-group" style={{flex:2}}>
          <label>Template Name *</label>
          <input type="text" value={data.name} onChange={e => setData({ ...data, name: e.target.value })} required placeholder="E.g., SaaS Outreach - Main" />
        </div>
        <div className="form-group">
          <label>Variant</label>
          <div className="variant-selector">
            {['A', 'B', 'C', 'D', 'E'].map(v => (
              <button key={v} type="button" className={`variant-btn ${data.variant === v ? 'active' : ''}`} onClick={() => setData({ ...data, variant: v })}>{v}</button>
            ))}
          </div>
        </div>
        <div className="form-group">
          <label>Step</label>
          <select value={data.step_type} onChange={e => setData({ ...data, step_type: e.target.value })}>
            <option>Main</option><option>Followup 1</option><option>Followup 2</option><option>Followup 3</option>
          </select>
        </div>
        <div className="form-group">
          <label>Workspace</label>
          <select value={data.country || ''} onChange={e => setData({ ...data, country: e.target.value })}>
            <option value="">Select country...</option>
            {countries.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      </div>

      <div className="form-group">
        <MultiSelect
          label="Assign to Campaigns"
          options={campaigns}
          value={data.campaign_ids || []}
          onChange={(ids) => setData({ ...data, campaign_ids: ids })}
          placeholder="Select campaigns..."
          renderOption={(c) => (
            <div className="campaign-option">
              <span className="campaign-option-name">{c.name}</span>
              <span className={`campaign-option-status status-${c.status?.toLowerCase()}`}>{c.status}</span>
            </div>
          )}
        />
      </div>

      <div className="form-group">
        <label>Subject Line</label>
        <input type="text" value={data.subject || ''} onChange={e => setData({ ...data, subject: e.target.value })} placeholder="Quick question about {{company}}" />
        <div className="variable-chips">{variables.map(v => (<span key={v} className="variable-chip" onClick={() => insertVariable(v, 'subject')}>{v}</span>))}</div>
      </div>
      <div className="form-group">
        <label>Email Body</label>
        <RichTextEditor
          value={data.body || ''}
          onChange={(content) => setData({ ...data, body: content })}
          placeholder="Hi {{firstName}}, I noticed that..."
          variables={variables}
        />
      </div>
      <div className="modal-actions">
        <button type="button" className="btn btn-secondary" onClick={onCancel}>Cancel</button>
        <button type="submit" className="btn btn-primary">{initial.id ? 'Update Template' : 'Create Template'}</button>
      </div>
    </form>
  );
};

// Settings Page with Enhanced Webhooks
const SettingsPage = () => {
  const { addToast } = useToast();
  const [tab, setTab] = useState('account');
  const { data: users, refetch } = useData('/users');
  const { data: webhooks, refetch: refetchWebhooks } = useData('/webhooks?limit=20');
  const [showAddUser, setShowAddUser] = useState(false);
  const [passwordForm, setPasswordForm] = useState({ current: '', new: '', confirm: '' });
  const [changingPassword, setChangingPassword] = useState(false);

  // Email verification API key state
  const [apiKeyConfigured, setApiKeyConfigured] = useState(false);
  const [apiKey, setApiKey] = useState('');
  const [savingApiKey, setSavingApiKey] = useState(false);
  const [loadingApiKey, setLoadingApiKey] = useState(true);

  // ReachInbox API key state
  const [riKeys, setRiKeys] = useState({ US: { configured: false, value: '' }, MX: { configured: false, value: '' } });
  const [savingRiKey, setSavingRiKey] = useState({ US: false, MX: false });
  const [riTestResults, setRiTestResults] = useState({ US: null, MX: null });

  // HubSpot state
  const [hsToken, setHsToken] = useState('');
  const [hsConfigured, setHsConfigured] = useState(false);
  const [savingHsToken, setSavingHsToken] = useState(false);
  const [hsTestResult, setHsTestResult] = useState(null);

  // BlitzAPI state
  const [blitzKey, setBlitzKey] = useState('');
  const [blitzConfigured, setBlitzConfigured] = useState(false);
  const [savingBlitzKey, setSavingBlitzKey] = useState(false);
  const [blitzTestResult, setBlitzTestResult] = useState(null);

  // Load API key status on mount
  useEffect(() => {
    const loadApiKeyStatus = async () => {
      try {
        const res = await api.get('/settings/bulkemailchecker_api_key');
        setApiKeyConfigured(res.configured);
      } catch (e) {
        console.error('Failed to load API key status:', e);
      }
      setLoadingApiKey(false);
    };
    const loadRiStatus = async () => {
      try {
        const res = await api.get('/reachinbox/workspace-status');
        setRiKeys(prev => ({
          US: { ...prev.US, configured: res.US?.configured || false },
          MX: { ...prev.MX, configured: res.MX?.configured || false }
        }));
      } catch (e) { console.error('Failed to load ReachInbox status:', e); }
    };
    const loadHsStatus = async () => {
      try {
        const res = await api.get('/settings/hubspot_private_app_token');
        setHsConfigured(res.configured);
      } catch (e) {}
    };
    const loadBlitzStatus = async () => {
      try {
        const res = await api.get('/settings/blitzapi_api_key');
        setBlitzConfigured(res.configured);
      } catch (e) {}
    };
    loadApiKeyStatus();
    loadRiStatus();
    loadHsStatus();
    loadBlitzStatus();
  }, []);

  const testRiConnection = async (ws) => {
    try {
      const res = await api.get(`/reachinbox/campaigns?workspace=${ws}`);
      if (res.campaigns && res.campaigns.length > 0) {
        addToast(`${ws} workspace: ${res.campaigns.length} campaigns found`, 'success');
        setRiTestResults(prev => ({ ...prev, [ws]: `${res.campaigns.length} campaigns` }));
      } else {
        addToast(`${ws} workspace connected (0 campaigns)`, 'success');
        setRiTestResults(prev => ({ ...prev, [ws]: 'Connected' }));
      }
    } catch (e) {
      addToast(`${ws} connection test failed`, 'error');
    }
  };

  const saveBlitzKey = async () => {
    if (!blitzKey.trim()) { addToast('Enter an API key', 'error'); return; }
    setSavingBlitzKey(true);
    try {
      await api.put('/settings/blitzapi_api_key', { value: blitzKey.trim() });
      addToast('BlitzAPI key saved!', 'success');
      setBlitzConfigured(true);
      setBlitzKey('');
    } catch (e) { addToast(e.message || 'Failed to save', 'error'); }
    setSavingBlitzKey(false);
  };

  const testBlitzConnection = async () => {
    try {
      const res = await api.get('/leadgen/credits');
      if (res.valid) {
        const msg = `Connected — ${(res.remaining_credits || 0).toLocaleString()} credits remaining`;
        addToast(msg, 'success');
        setBlitzTestResult(msg);
      } else {
        addToast('BlitzAPI key invalid', 'error');
        setBlitzTestResult('Invalid key');
      }
    } catch (e) { addToast('Connection test failed', 'error'); }
  };

  const saveHsToken = async () => {
    if (!hsToken.trim()) { addToast('Enter a token', 'error'); return; }
    setSavingHsToken(true);
    try {
      await api.put('/settings/hubspot_private_app_token', { value: hsToken.trim() });
      addToast('HubSpot token saved!', 'success');
      setHsConfigured(true);
      setHsToken('');
    } catch (e) { addToast(e.message || 'Failed to save', 'error'); }
    setSavingHsToken(false);
  };

  const testHsConnection = async () => {
    try {
      const res = await api.get('/hubspot/status');
      if (res.status === 'connected') {
        addToast('HubSpot connected!', 'success');
        setHsTestResult('Connected');
      } else {
        addToast(`HubSpot: ${res.detail || res.status}`, 'error');
        setHsTestResult(`Error: ${res.detail || res.status}`);
      }
    } catch (e) { addToast('Connection test failed', 'error'); }
  };

  const saveRiKey = async (workspace) => {
    const val = riKeys[workspace].value.trim();
    if (!val) { addToast('Please enter an API key', 'error'); return; }
    setSavingRiKey(prev => ({ ...prev, [workspace]: true }));
    try {
      await api.put(`/settings/reachinbox_api_key_${workspace.toLowerCase()}`, { value: val });
      addToast(`ReachInbox ${workspace} key saved!`, 'success');
      setRiKeys(prev => ({ ...prev, [workspace]: { configured: true, value: '' } }));
    } catch (e) { addToast(e.message || 'Failed to save', 'error'); }
    setSavingRiKey(prev => ({ ...prev, [workspace]: false }));
  };

  const saveApiKey = async () => {
    if (!apiKey.trim()) {
      addToast('Please enter an API key', 'error');
      return;
    }
    setSavingApiKey(true);
    try {
      await api.put('/settings/bulkemailchecker_api_key', { value: apiKey.trim() });
      addToast('API key saved successfully!', 'success');
      setApiKeyConfigured(true);
      setApiKey('');
    } catch (e) {
      addToast(e.message || 'Failed to save API key', 'error');
    }
    setSavingApiKey(false);
  };
  const handleAddUser = async (data) => { try { await api.post('/auth/register', data); addToast('User created!', 'success'); setShowAddUser(false); refetch(); } catch (e) { addToast(e.message, 'error'); } };
  const handleDeleteUser = async (id) => { if (!window.confirm('Deactivate this user?')) return; try { await api.delete(`/users/${id}`); addToast('User deactivated', 'success'); refetch(); } catch (e) { addToast(e.message, 'error'); } };
  const copyUrl = (url) => { navigator.clipboard.writeText(url); addToast('URL copied to clipboard!', 'success'); };
  const handleChangePassword = async (e) => {
    e.preventDefault();
    if (passwordForm.new !== passwordForm.confirm) {
      addToast('New passwords do not match', 'error');
      return;
    }
    if (passwordForm.new.length < 6) {
      addToast('Password must be at least 6 characters', 'error');
      return;
    }
    setChangingPassword(true);
    try {
      await api.post('/auth/change-password', { current_password: passwordForm.current, new_password: passwordForm.new });
      addToast('Password changed successfully!', 'success');
      setPasswordForm({ current: '', new: '', confirm: '' });
    } catch (e) {
      addToast(e.message || 'Failed to change password', 'error');
    }
    setChangingPassword(false);
  };

  return (<div className="page settings-page">
    <div className="page-header">
      <div>
        <h1>Settings</h1>
        <p className="subtitle">Configure your Deduply workspace</p>
      </div>
    </div>

    <div className="settings-tabs">
      <button className={tab === 'account' ? 'active' : ''} onClick={() => setTab('account')}><Settings size={16} /> Account</button>
      <button className={tab === 'users' ? 'active' : ''} onClick={() => setTab('users')}><Users size={16} /> Team</button>
      <button className={tab === 'webhooks' ? 'active' : ''} onClick={() => setTab('webhooks')}><Webhook size={16} /> Integrations</button>
      <button className={tab === 'database' ? 'active' : ''} onClick={() => setTab('database')}><Database size={16} /> Database</button>
    </div>

    {tab === 'account' && (
      <div className="settings-section">
        <div className="section-header">
          <h2>Change Password</h2>
        </div>
        <form onSubmit={handleChangePassword} className="password-form">
          <div className="form-group">
            <label>Current Password</label>
            <input type="password" value={passwordForm.current} onChange={e => setPasswordForm({...passwordForm, current: e.target.value})} required placeholder="Enter current password" />
          </div>
          <div className="form-group">
            <label>New Password</label>
            <input type="password" value={passwordForm.new} onChange={e => setPasswordForm({...passwordForm, new: e.target.value})} required minLength={6} placeholder="Enter new password (min 6 chars)" />
          </div>
          <div className="form-group">
            <label>Confirm New Password</label>
            <input type="password" value={passwordForm.confirm} onChange={e => setPasswordForm({...passwordForm, confirm: e.target.value})} required placeholder="Confirm new password" />
          </div>
          <button type="submit" className="btn btn-primary" disabled={changingPassword}>
            {changingPassword ? <><Loader2 size={16} className="spin" /> Changing...</> : 'Change Password'}
          </button>
        </form>
      </div>
    )}

    {tab === 'users' && (
      <div className="settings-section">
        <div className="section-header">
          <h2>Team Members</h2>
          <button className="btn btn-primary" onClick={() => setShowAddUser(true)}><UserPlus size={16} /> Add User</button>
        </div>
        <div className="users-list">
          {users?.data?.map(u => (
            <div key={u.id} className="user-card">
              <div className="user-avatar-lg">{u.name?.[0] || u.email[0]}</div>
              <div className="user-info-lg">
                <span className="user-name-lg">{u.name || u.email}</span>
                <span className="user-email">{u.email}</span>
                <span className={`role-badge role-${u.role}`}>{u.role}</span>
              </div>
              <button className="btn-icon-small danger" onClick={() => handleDeleteUser(u.id)}><Trash2 size={14} /></button>
            </div>
          ))}
        </div>
        <Modal isOpen={showAddUser} onClose={() => setShowAddUser(false)} title="Add Team Member"><AddUserForm onSubmit={handleAddUser} onCancel={() => setShowAddUser(false)} /></Modal>
      </div>
    )}

    {tab === 'webhooks' && (
      <div className="settings-section">
        {/* Email Verification API Configuration */}
        <div className="api-config-section">
          <div className="section-header">
            <h2><Mail size={20} /> Email Verification API</h2>
          </div>
          <p className="help-text">
            Configure your BulkEmailChecker API key to verify emails during import.
            Each verification costs 1 credit. <a href="https://bulkemailchecker.com" target="_blank" rel="noopener noreferrer">Get your API key →</a>
          </p>

          <div className="api-key-form">
            <div className="api-key-status">
              {loadingApiKey ? (
                <span className="status-loading"><Loader2 size={14} className="spin" /> Checking...</span>
              ) : apiKeyConfigured ? (
                <span className="status-configured"><Check size={14} /> API Key Configured</span>
              ) : (
                <span className="status-not-configured"><AlertCircle size={14} /> Not Configured</span>
              )}
            </div>

            <div className="api-key-input-group">
              <input
                type="password"
                placeholder={apiKeyConfigured ? "Enter new API key to replace existing" : "Enter your BulkEmailChecker API key"}
                value={apiKey}
                onChange={e => setApiKey(e.target.value)}
                className="api-key-input"
              />
              <button
                className="btn btn-primary"
                onClick={saveApiKey}
                disabled={savingApiKey || !apiKey.trim()}
              >
                {savingApiKey ? <><Loader2 size={16} className="spin" /> Saving...</> : 'Save API Key'}
              </button>
            </div>
          </div>
        </div>

        {/* ReachInbox API Configuration */}
        <div className="api-config-section" style={{marginTop: '32px'}}>
          <div className="section-header">
            <h2><Send size={20} /> ReachInbox API Keys</h2>
          </div>
          <p className="help-text">
            Configure API keys for each ReachInbox workspace to enable pushing contacts directly from the contacts table.
            Use the <strong>Push to ReachInbox</strong> bulk action to send contacts to a campaign sequence.
          </p>
          <div className="ri-keys-grid">
            {['US', 'MX'].map(ws => (
              <div key={ws} className="ri-key-card">
                <div className="ri-key-header">
                  <span className="ri-workspace-badge">{ws}</span>
                  <span className="ri-key-label">{ws === 'US' ? 'United States Workspace' : 'Mexico Workspace'}</span>
                  {riKeys[ws].configured
                    ? <span className="status-configured"><Check size={13} /> Configured</span>
                    : <span className="status-not-configured"><AlertCircle size={13} /> Not set</span>}
                </div>
                <div className="api-key-input-group">
                  <input
                    type="password"
                    placeholder={riKeys[ws].configured ? 'Enter new key to replace existing' : `ReachInbox ${ws} API key`}
                    value={riKeys[ws].value}
                    onChange={e => setRiKeys(prev => ({ ...prev, [ws]: { ...prev[ws], value: e.target.value } }))}
                    className="api-key-input"
                  />
                  <button
                    className="btn btn-primary"
                    onClick={() => saveRiKey(ws)}
                    disabled={savingRiKey[ws] || !riKeys[ws].value.trim()}
                  >
                    {savingRiKey[ws] ? <><Loader2 size={14} className="spin" /> Saving...</> : 'Save'}
                  </button>
                  <button
                    className="btn btn-secondary"
                    onClick={() => testRiConnection(ws)}
                    disabled={!riKeys[ws].configured}
                    title="Test connection and list campaigns"
                  >
                    Test
                  </button>
                </div>
                {riTestResults[ws] && (
                  <div className="ri-test-result"><Check size={13} /> {riTestResults[ws]}</div>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* HubSpot CRM Integration */}
        <div className="api-config-section" style={{marginTop: '32px'}}>
          <div className="section-header">
            <h2><Zap size={20} /> HubSpot CRM</h2>
          </div>
          <p className="help-text">
            Sync contacts to HubSpot when they become Interested or book a meeting. A deal is automatically created and linked.
            {' '}<a href="https://developers.hubspot.com/docs/api/private-apps" target="_blank" rel="noopener noreferrer">Get your private app token →</a>
          </p>
          <div className="api-key-form">
            <div className="api-key-status">
              {hsConfigured
                ? <span className="status-configured"><Check size={14} /> Token Configured</span>
                : <span className="status-not-configured"><AlertCircle size={14} /> Not Configured</span>}
            </div>
            <div className="api-key-input-group">
              <input
                type="password"
                placeholder={hsConfigured ? 'Enter new token to replace existing' : 'pat-na-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'}
                value={hsToken}
                onChange={e => setHsToken(e.target.value)}
                className="api-key-input"
              />
              <button className="btn btn-primary" onClick={saveHsToken} disabled={savingHsToken || !hsToken.trim()}>
                {savingHsToken ? <><Loader2 size={16} className="spin" /> Saving...</> : 'Save Token'}
              </button>
              <button className="btn btn-secondary" onClick={testHsConnection} disabled={!hsConfigured}>
                Test
              </button>
            </div>
            {hsTestResult && <div className="ri-test-result"><Check size={13} /> {hsTestResult}</div>}
          </div>
          <div className="help-text" style={{marginTop: 8}}>
            <strong>Auto-push:</strong> When a contact receives a <code>lead_interested</code> or <code>meeting_booked</code> webhook event, they are automatically synced to HubSpot in the background.
          </div>
        </div>

        {/* BlitzAPI Configuration */}
        <div className="api-config-section" style={{marginTop: '32px'}}>
          <div className="section-header">
            <h2><Target size={20} /> BlitzAPI</h2>
          </div>
          <p className="help-text">
            API key for the Lead Generation engine — company search, employee finder, email enrichment.
            {' '}<a href="https://blitz-api.ai" target="_blank" rel="noopener noreferrer">Get your key at blitz-api.ai →</a>
          </p>
          <div className="api-key-form">
            <div className="api-key-status">
              {blitzConfigured
                ? <span className="status-configured"><Check size={14} /> Configured</span>
                : <span className="status-not-configured"><AlertCircle size={14} /> Not set</span>}
            </div>
            <div className="api-key-input-group">
              <input
                type="password"
                placeholder={blitzConfigured ? 'Enter new key to replace existing' : 'blitz-xxxxxxxxxxxxxxxx'}
                value={blitzKey}
                onChange={e => setBlitzKey(e.target.value)}
                className="api-key-input"
              />
              <button className="btn btn-primary" onClick={saveBlitzKey} disabled={savingBlitzKey || !blitzKey.trim()}>
                {savingBlitzKey ? <><Loader2 size={16} className="spin" /> Saving...</> : 'Save'}
              </button>
              <button className="btn btn-secondary" onClick={testBlitzConnection} disabled={!blitzConfigured}>
                Test
              </button>
            </div>
            {blitzTestResult && <div className="ri-test-result"><Check size={13} /> {blitzTestResult}</div>}
          </div>
        </div>

        <div className="section-header" style={{marginTop: '32px'}}>
          <h2>Webhook Integrations</h2>
          <button className="btn btn-secondary" onClick={refetchWebhooks}><RefreshCw size={16} /> Refresh</button>
        </div>
        <p className="help-text">Connect your cold email tools to automatically sync engagement data. Configure these webhook URLs in your email service provider.</p>

        <div className="webhooks-grid">
          {/* ReachInbox */}
          <div className="webhook-card-v2">
            <div className="webhook-card-header">
              <div className="webhook-icon reachinbox">RI</div>
              <div>
                <h4>ReachInbox</h4>
                <span className="webhook-status active">Active</span>
              </div>
            </div>
            <div className="webhook-url-box">
              <code>{API_BASE}/webhook/reachinbox</code>
              <button className="copy-btn" onClick={() => copyUrl(`${API_BASE}/webhook/reachinbox`)}><Copy size={14} /></button>
            </div>
            <div className="webhook-events">
              <span className="event-tag">sent</span>
              <span className="event-tag">opened</span>
              <span className="event-tag">clicked</span>
              <span className="event-tag">replied</span>
              <span className="event-tag">bounced</span>
              <span className="event-tag interest">lead_interested</span>
              <span className="event-tag interest">lead_not_interested</span>
            </div>
            <div className="webhook-info">
              <p><strong>What it does:</strong> Updates campaign metrics, contact status, and interest tags based on email engagement events.</p>
              <p><strong>Payload format:</strong></p>
              <pre>{`{
  "event": "LEAD_INTERESTED",
  "lead_email": "user@example.com",
  "campaign_name": "Campaign Name"
}`}</pre>
            </div>
          </div>

          {/* BulkEmailChecker */}
          <div className="webhook-card-v2">
            <div className="webhook-card-header">
              <div className="webhook-icon bulkemailchecker">BEC</div>
              <div>
                <h4>BulkEmailChecker</h4>
                <span className="webhook-status active">Active</span>
              </div>
            </div>
            <div className="webhook-url-box">
              <code>{API_BASE}/webhook/bulkemailchecker</code>
              <button className="copy-btn" onClick={() => copyUrl(`${API_BASE}/webhook/bulkemailchecker`)}><Copy size={14} /></button>
            </div>
            <div className="webhook-events">
              <span className="event-tag valid">valid</span>
              <span className="event-tag invalid">invalid</span>
              <span className="event-tag risky">risky</span>
            </div>
            <div className="webhook-info">
              <p><strong>What it does:</strong> Updates contact email validation status. Invalid emails are automatically marked as Bounced.</p>
              <p><strong>Payload format:</strong></p>
              <pre>{`{
  "results": [
    { "email": "a@b.com", "status": "valid" },
    { "email": "c@d.com", "status": "invalid" }
  ]
}`}</pre>
            </div>
          </div>
        </div>

        <div className="webhook-events-section">
          <div className="section-header">
            <h3>Recent Webhook Events</h3>
            <span className="events-count">{webhooks?.data?.length || 0} events</span>
          </div>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Source</th>
                  <th>Event Type</th>
                  <th>Email</th>
                  <th>Campaign</th>
                  <th>Status</th>
                  <th>Time</th>
                </tr>
              </thead>
              <tbody>
                {webhooks?.data?.slice(0, 15).map(e => (
                  <tr key={e.id}>
                    <td><span className={`source-badge ${e.source}`}>{e.source}</span></td>
                    <td><span className={`event-badge ${e.event_type}`}>{e.event_type}</span></td>
                    <td className="email-cell">{e.email || '—'}</td>
                    <td>{e.campaign_name || '—'}</td>
                    <td>{e.processed ? <span className="status-check"><Check size={14} /> Processed</span> : <Loader2 size={14} className="spin" />}</td>
                    <td className="time-cell">{new Date(e.created_at).toLocaleString()}</td>
                  </tr>
                ))}
                {(!webhooks?.data || webhooks.data.length === 0) && (
                  <tr><td colSpan={6} className="empty-cell">No webhook events yet. Connect your email tools to start receiving data.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    )}

    {tab === 'database' && (
      <div className="settings-section">
        <h2>Database Information</h2>
        <div className="db-info-card">
          <Database size={40} />
          <div className="db-details">
            <h3>SQLite Database</h3>
            <span className="db-file">deduply.db</span>
            <p>Your data is stored locally in a SQLite database. This is perfect for development and small teams.</p>
          </div>
        </div>
        <div className="db-recommendations">
          <h4>Production Recommendations</h4>
          <ul>
            <li><strong>PostgreSQL:</strong> For high-volume production workloads</li>
            <li><strong>Supabase:</strong> Managed PostgreSQL with real-time features</li>
            <li><strong>PlanetScale:</strong> Serverless MySQL with branching</li>
          </ul>
        </div>
        <div className="arkode-footer">
          <div className="arkode-brand-lg">
            <span>Built by</span>
            <strong>Arkode</strong>
          </div>
          <p>Deduply v5.1 - Cold Email Operations Platform</p>
        </div>
      </div>
    )}
  </div>);
};

const AddUserForm = ({ onSubmit, onCancel }) => {
  const [data, setData] = useState({ email: '', password: '', name: '', role: 'member' });
  return (<form onSubmit={e => { e.preventDefault(); onSubmit(data); }}><div className="form-group"><label>Email *</label><input type="email" value={data.email} onChange={e => setData({ ...data, email: e.target.value })} required /></div><div className="form-group"><label>Password *</label><input type="password" value={data.password} onChange={e => setData({ ...data, password: e.target.value })} required /></div><div className="form-group"><label>Name</label><input type="text" value={data.name} onChange={e => setData({ ...data, name: e.target.value })} /></div><div className="form-group"><label>Role</label><select value={data.role} onChange={e => setData({ ...data, role: e.target.value })}><option value="member">Member</option><option value="admin">Admin</option></select></div><div className="modal-actions"><button type="button" className="btn btn-secondary" onClick={onCancel}>Cancel</button><button type="submit" className="btn btn-primary">Add User</button></div></form>);
};

// ---------------------------------------------------------------------------
// Lead Gen Page — BlitzAPI Enrichment Engine
// ---------------------------------------------------------------------------
const LEAD_GEN_INDUSTRIES = [
  'Accounting', 'Advertising', 'Aerospace', 'Agriculture', 'Architecture',
  'Automotive', 'Banking', 'Biotechnology', 'Chemical', 'Civil Engineering',
  'Computer Software', 'Construction', 'Consumer Electronics', 'Consumer Goods',
  'Defense & Space', 'Education Management', 'Electrical & Electronic Manufacturing',
  'Entertainment', 'Environmental Services', 'Financial Services',
  'Food & Beverages', 'Government Administration', 'Health, Wellness & Fitness',
  'Healthcare', 'Hospital & Health Care', 'Human Resources',
  'Information Technology', 'Insurance', 'Internet', 'IT Services',
  'Logistics & Supply Chain', 'Management Consulting', 'Manufacturing',
  'Marketing & Advertising', 'Media', 'Medical Devices', 'Mining', 'Nonprofit',
  'Oil & Energy', 'Outsourcing', 'Pharmaceuticals', 'Public Relations',
  'Real Estate', 'Retail', 'Security', 'Software Development',
  'Staffing & Recruiting', 'Telecommunications', 'Transportation',
  'Utilities', 'Venture Capital',
];
const LEAD_GEN_COMPANY_TYPES = [
  'Privately Held', 'Public Company', 'Non Profit', 'Partnership', 'Self Owned',
];
const LEAD_GEN_COUNTRIES = [
  { code: 'US', label: 'United States' }, { code: 'MX', label: 'Mexico' },
  { code: 'CA', label: 'Canada' }, { code: 'GB', label: 'United Kingdom' },
  { code: 'DE', label: 'Germany' }, { code: 'ES', label: 'Spain' },
  { code: 'FR', label: 'France' }, { code: 'AU', label: 'Australia' },
  { code: 'BR', label: 'Brazil' }, { code: 'IN', label: 'India' },
  { code: 'AR', label: 'Argentina' }, { code: 'CL', label: 'Chile' },
  { code: 'CO', label: 'Colombia' }, { code: 'PE', label: 'Peru' },
];
const LEAD_GEN_EMP_RANGES = ['1-10', '11-50', '51-200', '201-500', '500+'];
const LEAD_GEN_JOB_LEVELS = ['C-Level', 'VP', 'Director', 'Manager'];

const WorkspaceBadge = ({ workspace }) => {
  const ws = (workspace || 'US').toUpperCase();
  return (
    <span className={`workspace-badge workspace-badge-${ws.toLowerCase()}`}>
      {ws === 'MX' ? 'MX' : 'US'}
    </span>
  );
};

// Multi-value text-tag input used in filter panels
const TagInput = ({ tags, onAdd, onRemove, placeholder = 'Type and press Enter' }) => {
  const [input, setInput] = useState('');
  const handleKeyDown = (e) => {
    if ((e.key === 'Enter' || e.key === ',') && input.trim()) {
      e.preventDefault();
      const val = input.trim().replace(/,+$/, '');
      if (val && !tags.includes(val)) onAdd(val);
      setInput('');
    }
    if (e.key === 'Backspace' && !input && tags.length > 0) {
      onRemove(tags[tags.length - 1]);
    }
  };
  return (
    <div className="tag-input-container">
      {tags.map(t => (
        <span key={t} className="tag-chip">
          {t}
          <button type="button" className="tag-chip-remove" onClick={() => onRemove(t)}><X size={10} /></button>
        </span>
      ))}
      <input
        className="tag-input-field"
        value={input}
        onChange={e => setInput(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={tags.length === 0 ? placeholder : ''}
      />
    </div>
  );
};

const LeadGenPage = () => {
  const { addToast } = useToast();
  const [activeTab, setActiveTab] = useState('search');

  // --- Search filters ---
  const [indInclude, setIndInclude] = useState([]);
  const [indExclude, setIndExclude] = useState([]);
  const [kwInclude, setKwInclude] = useState([]);
  const [kwExclude, setKwExclude] = useState([]);
  const [countries, setCountries] = useState([]);
  const [states, setStates] = useState([]);
  const [empRanges, setEmpRanges] = useState([]);
  const [companyTypes, setCompanyTypes] = useState([]);
  const [excludeDomains, setExcludeDomains] = useState('');
  const [maxResults, setMaxResults] = useState(25);

  // --- Search job state ---
  const [searchLoading, setSearchLoading] = useState(false);
  const [activeSearchJob, setActiveSearchJob] = useState(null);
  const [jobCompanies, setJobCompanies] = useState([]);
  const [jobTotal, setJobTotal] = useState(0);
  const [selectedCompanies, setSelectedCompanies] = useState([]);
  const [importLoading, setImportLoading] = useState(false);

  // --- Two-stage pipeline: Stage 2 ---
  const [findContactsJob, setFindContactsJob] = useState(null);
  const [stagedContacts, setStagedContacts] = useState([]);
  const [stagedTotal, setStagedTotal] = useState(0);
  const [selectedStagedContacts, setSelectedStagedContacts] = useState([]);
  const [approveLoading, setApproveLoading] = useState(false);
  const [showContactsPreview, setShowContactsPreview] = useState(false);

  // --- Waterfall state ---
  const [waterfallUrl, setWaterfallUrl] = useState('');
  const [waterfallLevels, setWaterfallLevels] = useState(['C-Level', 'VP']);
  const [waterfallMax, setWaterfallMax] = useState(3);
  const [waterfallLoading, setWaterfallLoading] = useState(false);
  const [waterfallResults, setWaterfallResults] = useState([]);

  // --- Credits & Jobs state ---
  const [credits, setCredits] = useState(null);
  const [creditsLoading, setCreditsLoading] = useState(false);
  const [jobs, setJobs] = useState([]);
  const [jobsLoading, setJobsLoading] = useState(false);

  // Poll active search job
  useEffect(() => {
    if (!activeSearchJob?.job_id || activeSearchJob?.status === 'completed' || activeSearchJob?.status === 'failed') return;
    const interval = setInterval(async () => {
      try {
        const data = await api.get(`/leadgen/jobs/${activeSearchJob.job_id}`);
        const job = data.job;
        setActiveSearchJob(prev => ({ ...prev, status: job.status, results_count: job.results_count, error: job.error }));
        if (job.status === 'completed') {
          setJobCompanies(data.companies || []);
          setJobTotal(job.results_count || 0);
          setSelectedCompanies([]);
          addToast(`Found ${job.results_count} companies`, 'success');
        } else if (job.status === 'failed') {
          addToast(`Search failed: ${job.error || 'Unknown error'}`, 'error');
        }
      } catch (e) { console.error('Poll error:', e); }
    }, 2000);
    return () => clearInterval(interval);
  }, [activeSearchJob?.job_id, activeSearchJob?.status]);

  const fetchJobs = async () => {
    setJobsLoading(true);
    try {
      const data = await api.get('/leadgen/jobs');
      setJobs(data.data || []);
    } catch (e) { addToast(e.message, 'error'); }
    setJobsLoading(false);
  };

  const fetchCredits = async () => {
    setCreditsLoading(true);
    try {
      const data = await api.get('/leadgen/credits');
      setCredits(data);
    } catch (e) { addToast(e.message, 'error'); }
    setCreditsLoading(false);
  };

  useEffect(() => {
    if (activeTab === 'credits') { fetchCredits(); fetchJobs(); }
  }, [activeTab]);

  const handleSearch = async (e) => {
    e.preventDefault();
    setSearchLoading(true);
    setJobCompanies([]);
    setSelectedCompanies([]);
    setJobTotal(0);
    try {
      const body = { max_results: maxResults };
      if (indInclude.length > 0) body.industries_include = indInclude;
      if (indExclude.length > 0) body.industries_exclude = indExclude;
      if (kwInclude.length > 0) body.keywords_include = kwInclude;
      if (kwExclude.length > 0) body.keywords_exclude = kwExclude;
      if (countries.length > 0) body.countries = countries;
      if (states.length > 0) body.states = states;
      if (empRanges.length > 0) body.employee_range = empRanges;
      if (companyTypes.length > 0) body.company_types = companyTypes;
      if (excludeDomains.trim()) body.exclude_domains = excludeDomains.split(/[\n,]+/).map(d => d.trim()).filter(Boolean);
      const result = await api.post('/leadgen/companies/search', body);
      setActiveSearchJob({ job_id: result.job_id, status: 'running', results_count: 0 });
      addToast('Search started...', 'info');
    } catch (e) { addToast(e.message, 'error'); }
    setSearchLoading(false);
  };

  const toggleCompany = (id) => setSelectedCompanies(prev => prev.includes(id) ? prev.filter(c => c !== id) : [...prev, id]);
  const toggleAllCompanies = () => {
    if (selectedCompanies.length === jobCompanies.length) setSelectedCompanies([]);
    else setSelectedCompanies(jobCompanies.map(c => c.id));
  };

  const handleImport = async () => {
    if (selectedCompanies.length === 0) { addToast('Select at least one company', 'error'); return; }
    setImportLoading(true);
    try {
      const result = await api.post('/leadgen/companies/import', { company_ids: selectedCompanies });
      addToast(`Import started (job ${result.job_id.slice(0, 8)}...) — check Credits & Jobs for progress`, 'success');
    } catch (e) { addToast(e.message, 'error'); }
    setImportLoading(false);
  };

  // Stage 2: Find contacts for selected companies (staging table)
  const handleFindContacts = async () => {
    if (selectedCompanies.length === 0) { addToast('Select at least one company first', 'error'); return; }
    setImportLoading(true);
    setStagedContacts([]);
    setShowContactsPreview(false);
    try {
      const result = await api.post('/leadgen/companies/find-contacts', {
        company_ids: selectedCompanies,
        job_levels: ['C-Level', 'VP', 'Director', 'Manager'],
        max_per_company: 5,
      });
      setFindContactsJob({ job_id: result.job_id, status: 'running', company_count: result.company_count });
      addToast(`Finding contacts for ${result.company_count} companies...`, 'info');
      // Poll until done
      const poll = setInterval(async () => {
        try {
          const jobs = await api.get('/leadgen/jobs');
          const job = (jobs.data || []).find(j => j.id === result.job_id);
          if (job && (job.status === 'awaiting_approval' || job.status === 'completed' || job.status === 'failed')) {
            clearInterval(poll);
            if (job.status === 'failed') {
              addToast('Contact search failed: ' + (job.error || 'Unknown'), 'error');
            } else {
              // Load preview
              const preview = await api.get('/leadgen/contacts/preview?job_id=' + result.job_id);
              setStagedContacts(preview.contacts || []);
              setStagedTotal(preview.total || 0);
              setSelectedStagedContacts((preview.contacts || []).map(c => c.id));
              setShowContactsPreview(true);
              addToast(`${preview.total} contacts ready for review`, 'success');
            }
            setFindContactsJob(prev => ({ ...prev, status: job.status }));
          }
        } catch (e) { clearInterval(poll); }
      }, 3000);
    } catch (e) { addToast(e.message, 'error'); }
    setImportLoading(false);
  };

  // Stage 3: Approve selected staged contacts → move to main contacts table
  const handleApproveContacts = async () => {
    if (selectedStagedContacts.length === 0) { addToast('Select contacts to approve', 'error'); return; }
    setApproveLoading(true);
    try {
      const result = await api.post('/leadgen/contacts/approve', { contact_ids: selectedStagedContacts });
      addToast(`✅ ${result.imported} contacts imported! ${result.skipped_duplicates} duplicates skipped.`, 'success');
      setStagedContacts(prev => prev.filter(c => !selectedStagedContacts.includes(c.id)));
      setSelectedStagedContacts([]);
      if (stagedContacts.length - selectedStagedContacts.length === 0) setShowContactsPreview(false);
    } catch (e) { addToast(e.message, 'error'); }
    setApproveLoading(false);
  };

  const toggleStagedContact = (id) => setSelectedStagedContacts(prev => prev.includes(id) ? prev.filter(c => c !== id) : [...prev, id]);
  const toggleAllStaged = () => {
    const pending = stagedContacts.filter(c => c.status === 'pending').map(c => c.id);
    if (selectedStagedContacts.length === pending.length) setSelectedStagedContacts([]);
    else setSelectedStagedContacts(pending);
  };

  const toggleWaterfallLevel = (level) => setWaterfallLevels(prev => prev.includes(level) ? prev.filter(l => l !== level) : [...prev, level]);

  const handleWaterfall = async (e) => {
    e.preventDefault();
    if (!waterfallUrl.trim()) { addToast('Enter a LinkedIn URL', 'error'); return; }
    setWaterfallLoading(true);
    setWaterfallResults([]);
    try {
      const result = await api.post('/leadgen/waterfall-direct', {
        company_linkedin_url: waterfallUrl.trim(),
        job_levels: waterfallLevels,
        max_per_company: waterfallMax,
      });
      setWaterfallResults(result.results || []);
      addToast(`Found ${result.total} decision makers, ${result.imported} imported`, 'success');
    } catch (e) { addToast(e.message, 'error'); }
    setWaterfallLoading(false);
  };

  const toggleEmpRange = (r) => setEmpRanges(prev => prev.includes(r) ? prev.filter(x => x !== r) : [...prev, r]);
  const toggleType = (t) => setCompanyTypes(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]);
  const toggleCountry = (code) => setCountries(prev => prev.includes(code) ? prev.filter(c => c !== code) : [...prev, code]);

  return (
    <div className="page-container">
      <div className="page-header">
        <div className="page-title">
          <Target size={24} />
          <div>
            <h1>Lead Generation</h1>
            <p className="page-subtitle">BlitzAPI enrichment engine — find and import decision makers</p>
          </div>
        </div>
      </div>

      <div className="dash-tabs" style={{ marginBottom: 24 }}>
        <button className={`dash-tab ${activeTab === 'search' ? 'active' : ''}`} onClick={() => setActiveTab('search')}>
          <Search size={16} /> Company Search
        </button>
        <button className={`dash-tab ${activeTab === 'waterfall' ? 'active' : ''}`} onClick={() => setActiveTab('waterfall')}>
          <Zap size={16} /> ICP Waterfall
        </button>
        <button className={`dash-tab ${activeTab === 'credits' ? 'active' : ''}`} onClick={() => setActiveTab('credits')}>
          <TrendingUp size={16} /> Credits &amp; Jobs
        </button>
      </div>

      {/* ---- Sub-tab 1: Company Search — split layout ---- */}
      {activeTab === 'search' && (
        <div className="leadgen-search-layout">

          {/* Left: Filters panel */}
          <form onSubmit={handleSearch} className="leadgen-form-panel">
            <div className="leadgen-filters-card">
              <div className="leadgen-filters-header">
                <Filter size={15} /><span>Filters</span>
              </div>

              <div className="filter-section">
                <label className="filter-label">Industries — Include</label>
                <TagInput
                  tags={indInclude}
                  onAdd={v => setIndInclude(p => [...p, v])}
                  onRemove={v => setIndInclude(p => p.filter(x => x !== v))}
                  placeholder="Type industry and press Enter"
                />
                <div className="filter-presets">
                  {['Manufacturing','Construction','Retail','Healthcare','Financial Services','Software Development','Logistics & Supply Chain','Real Estate'].map(ind => (
                    <button key={ind} type="button"
                      className={`preset-chip ${indInclude.includes(ind) ? 'active' : ''}`}
                      onClick={() => setIndInclude(p => p.includes(ind) ? p.filter(x => x !== ind) : [...p, ind])}
                    >{ind}</button>
                  ))}
                </div>
              </div>

              <div className="filter-section">
                <label className="filter-label">Industries — Exclude</label>
                <TagInput
                  tags={indExclude}
                  onAdd={v => setIndExclude(p => [...p, v])}
                  onRemove={v => setIndExclude(p => p.filter(x => x !== v))}
                  placeholder="e.g. SaaS, Franchise"
                />
              </div>

              <div className="filter-section">
                <label className="filter-label">Company Size</label>
                <div className="filter-checkboxes">
                  {LEAD_GEN_EMP_RANGES.map(r => (
                    <label key={r} className="filter-check-label">
                      <input type="checkbox" checked={empRanges.includes(r)} onChange={() => toggleEmpRange(r)} />
                      {r} employees
                    </label>
                  ))}
                </div>
              </div>

              <div className="filter-section">
                <label className="filter-label">Company Type</label>
                <div className="filter-checkboxes">
                  {LEAD_GEN_COMPANY_TYPES.map(t => (
                    <label key={t} className="filter-check-label">
                      <input type="checkbox" checked={companyTypes.includes(t)} onChange={() => toggleType(t)} />
                      {t}
                    </label>
                  ))}
                </div>
              </div>

              <div className="filter-section">
                <label className="filter-label">Countries (HQ)</label>
                <div className="filter-checkboxes filter-checkboxes-2col">
                  {LEAD_GEN_COUNTRIES.map(c => (
                    <label key={c.code} className="filter-check-label">
                      <input type="checkbox" checked={countries.includes(c.code)} onChange={() => toggleCountry(c.code)} />
                      {c.label}
                    </label>
                  ))}
                </div>
              </div>

              <div className="filter-section">
                <label className="filter-label">States / Cities</label>
                <TagInput
                  tags={states}
                  onAdd={v => setStates(p => [...p, v])}
                  onRemove={v => setStates(p => p.filter(x => x !== v))}
                  placeholder="e.g. Texas, California"
                />
              </div>

              <div className="filter-section">
                <label className="filter-label">Description Keywords — Include</label>
                <TagInput
                  tags={kwInclude}
                  onAdd={v => setKwInclude(p => [...p, v])}
                  onRemove={v => setKwInclude(p => p.filter(x => x !== v))}
                  placeholder="e.g. roofing, general contractor"
                />
              </div>

              <div className="filter-section">
                <label className="filter-label">Description Keywords — Exclude</label>
                <TagInput
                  tags={kwExclude}
                  onAdd={v => setKwExclude(p => [...p, v])}
                  onRemove={v => setKwExclude(p => p.filter(x => x !== v))}
                  placeholder="e.g. franchise, SaaS"
                />
              </div>

              <div className="filter-section">
                <label className="filter-label">Exclude Domains (one per line)</label>
                <textarea
                  className="filter-textarea"
                  rows={3}
                  placeholder={"acme.com\nexample.org"}
                  value={excludeDomains}
                  onChange={e => setExcludeDomains(e.target.value)}
                />
              </div>

              <div className="filter-section filter-section-row">
                <div>
                  <label className="filter-label">Max Results</label>
                  <select className="filter-select" value={maxResults} onChange={e => setMaxResults(parseInt(e.target.value, 10))}>
                    <option value={10}>10</option>
                    <option value={25}>25</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                  </select>
                </div>
                <div className="credit-estimate"><Zap size={13} /> ~{maxResults} credits</div>
              </div>

              <button type="submit" className="btn btn-primary leadgen-search-btn" disabled={searchLoading}>
                {searchLoading ? <Loader2 className="spin" size={16} /> : <Search size={16} />}
                Search
              </button>
            </div>
          </form>

          {/* Right: Results panel */}
          <div className="leadgen-results-panel">
            {activeSearchJob && (activeSearchJob.status === 'running' || activeSearchJob.status === 'pending') && (
              <div className="leadgen-job-progress">
                <Loader2 className="spin" size={16} />
                <span>Searching BlitzAPI... (job {activeSearchJob.job_id?.slice(0, 8)})</span>
              </div>
            )}
            {activeSearchJob?.status === 'failed' && (
              <div className="leadgen-job-error">
                <AlertCircle size={16} /> Search failed: {activeSearchJob.error || 'Unknown error'}
              </div>
            )}

            {selectedCompanies.length > 0 && (
              <div className="leadgen-bulk-bar">
                <span>{selectedCompanies.length} {selectedCompanies.length === 1 ? 'company' : 'companies'} selected</span>
                <button className="btn btn-primary btn-sm" disabled={importLoading} onClick={handleFindContacts}>
                  {importLoading ? <Loader2 className="spin" size={14} /> : <Users size={14} />}
                  {importLoading ? 'Finding contacts...' : `Find Contacts (${selectedCompanies.length})`}
                </button>
                <button className="btn btn-secondary btn-sm" onClick={() => setSelectedCompanies([])}>Clear</button>
              </div>
            )}

            {jobCompanies.length > 0 && (
              <div className="leadgen-results-table-wrap">
                <div className="leadgen-results-header">
                  <span className="results-count">Showing {jobCompanies.length} of {jobTotal} results</span>
                  <label className="select-all-label">
                    <input type="checkbox" checked={selectedCompanies.length === jobCompanies.length && jobCompanies.length > 0} onChange={toggleAllCompanies} />
                    Select all
                  </label>
                </div>
                <div className="table-container">
                  <table className="data-table leadgen-table">
                    <thead>
                      <tr>
                        <th style={{width:36}}>#</th>
                        <th style={{width:36}}><input type="checkbox" checked={selectedCompanies.length === jobCompanies.length && jobCompanies.length > 0} onChange={toggleAllCompanies} /></th>
                        <th>Name</th>
                        <th>Description</th>
                        <th>Primary Industry</th>
                        <th>Size</th>
                        <th>Type</th>
                        <th>Location</th>
                        <th>Country</th>
                        <th>LinkedIn</th>
                      </tr>
                    </thead>
                    <tbody>
                      {jobCompanies.map((c, i) => (
                        <tr key={c.id} className={selectedCompanies.includes(c.id) ? 'selected' : ''} onClick={() => toggleCompany(c.id)} style={{cursor:'pointer'}}>
                          <td className="row-num">{i + 1}</td>
                          <td onClick={e => e.stopPropagation()}><input type="checkbox" checked={selectedCompanies.includes(c.id)} onChange={() => toggleCompany(c.id)} /></td>
                          <td>
                            <div className="company-name-cell">
                              <strong>{c.name || '—'}</strong>
                              {c.domain && <span className="company-domain">{c.domain}</span>}
                            </div>
                          </td>
                          <td className="about-cell" title={c.about || ''}>{c.about ? c.about.slice(0, 90) + (c.about.length > 90 ? '…' : '') : '—'}</td>
                          <td>{c.industry || '—'}</td>
                          <td className="nowrap">{c.size || '—'}</td>
                          <td>{c.type || '—'}</td>
                          <td className="nowrap">{[c.hq_city, c.hq_country].filter(Boolean).join(', ') || '—'}</td>
                          <td>{c.hq_country || '—'}</td>
                          <td onClick={e => e.stopPropagation()}>
                            {c.linkedin_url
                              ? <a href={c.linkedin_url} target="_blank" rel="noopener noreferrer" className="link-text"><ArrowRight size={12} /> View</a>
                              : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {!activeSearchJob && jobCompanies.length === 0 && (
              <div className="leadgen-empty-state">
                <Building2 size={48} />
                <h3>Search for Companies</h3>
                <p>Configure your filters and click Search. Results will appear here.</p>
                <p className="empty-hint">BlitzAPI charges credits per search — not live.</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ---- Contacts Preview Panel (Stage 2 result) ---- */}
      {showContactsPreview && activeTab === 'search' && (
        <div className="card" style={{marginTop: 24}}>
          <div className="card-header" style={{display:'flex', alignItems:'center', justifyContent:'space-between'}}>
            <div style={{display:'flex', alignItems:'center', gap:8}}>
              <Users size={18} style={{color:'var(--coral)'}} />
              <h3 style={{margin:0}}>Contacts Preview — Review Before Importing</h3>
              <span className="badge badge-info">{stagedContacts.filter(c=>c.status==='pending').length} pending</span>
            </div>
            <div style={{display:'flex', gap:8}}>
              <button className="btn btn-secondary btn-sm" onClick={toggleAllStaged}>
                {selectedStagedContacts.length === stagedContacts.filter(c=>c.status==='pending').length ? 'Deselect All' : 'Select All'}
              </button>
              <button className="btn btn-primary btn-sm" disabled={approveLoading || selectedStagedContacts.length === 0} onClick={handleApproveContacts}>
                {approveLoading ? <Loader2 className="spin" size={14} /> : <Check size={14} />}
                Approve &amp; Import ({selectedStagedContacts.length})
              </button>
              <button className="btn btn-ghost btn-sm" onClick={() => setShowContactsPreview(false)}>
                <X size={14} /> Dismiss
              </button>
            </div>
          </div>
          <div style={{padding:'0 16px 8px', fontSize:12, color:'var(--text-secondary)'}}>
            Review these contacts before they enter your main contacts table. Uncheck anyone you want to skip.
          </div>
          <div className="table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th style={{width:36}}><input type="checkbox" checked={selectedStagedContacts.length === stagedContacts.filter(c=>c.status==='pending').length && stagedContacts.length > 0} onChange={toggleAllStaged} /></th>
                  <th>Name</th>
                  <th>Title</th>
                  <th>Email</th>
                  <th>Company</th>
                  <th>Workspace</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {stagedContacts.map(c => (
                  <tr key={c.id} style={{opacity: c.status !== 'pending' ? 0.5 : 1}}>
                    <td>
                      {c.status === 'pending' && (
                        <input type="checkbox" checked={selectedStagedContacts.includes(c.id)} onChange={() => toggleStagedContact(c.id)} />
                      )}
                    </td>
                    <td><strong>{c.first_name} {c.last_name}</strong></td>
                    <td style={{color:'var(--text-secondary)', fontSize:12}}>{c.title || '—'}</td>
                    <td style={{fontSize:12}}>{c.email || <span style={{color:'var(--text-muted)'}}>No email</span>}</td>
                    <td style={{fontSize:12}}>
                      {c.company_name}
                      {c.company_domain && <span style={{color:'var(--text-muted)'}}> ({c.company_domain})</span>}
                    </td>
                    <td>
                      <span className={`workspace-badge workspace-${(c.workspace||'US').toLowerCase()}`}>
                        {c.workspace === 'MX' ? '🇲🇽' : '🇺🇸'} {c.workspace || 'US'}
                      </span>
                    </td>
                    <td>
                      {c.status === 'approved' && <span style={{color:'var(--green)',fontSize:12}}>✅ Imported</span>}
                      {c.status === 'rejected' && <span style={{color:'var(--text-muted)',fontSize:12}}>Skipped</span>}
                      {c.status === 'pending' && <span style={{color:'var(--text-secondary)',fontSize:12}}>Pending</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ---- Sub-tab 2: ICP Waterfall ---- */}
      {activeTab === 'waterfall' && (
        <div className="leadgen-search-layout">
          <div className="leadgen-form-panel">
            <div className="card">
              <div className="card-header"><h3>Waterfall ICP Search</h3></div>
              <div className="card-body">
                <p className="form-hint">Paste a company LinkedIn URL to find decision makers in priority order.</p>
                <form onSubmit={handleWaterfall}>
                  <div className="form-group">
                    <label>Company LinkedIn URL *</label>
                    <input
                      type="url"
                      placeholder="https://www.linkedin.com/company/acme"
                      value={waterfallUrl}
                      onChange={e => setWaterfallUrl(e.target.value)}
                      required
                    />
                  </div>

                  <div className="form-group">
                    <label>Job Levels (priority order)</label>
                    <div className="checkbox-col">
                      {LEAD_GEN_JOB_LEVELS.map(level => (
                        <label key={level} className="checkbox-label">
                          <input
                            type="checkbox"
                            checked={waterfallLevels.includes(level)}
                            onChange={() => toggleWaterfallLevel(level)}
                          />
                          {level}
                        </label>
                      ))}
                    </div>
                  </div>

                  <div className="form-group">
                    <label>Max Results</label>
                    <select value={waterfallMax} onChange={e => setWaterfallMax(parseInt(e.target.value, 10))}>
                      <option value={1}>1</option>
                      <option value={3}>3</option>
                      <option value={5}>5</option>
                      <option value={10}>10</option>
                    </select>
                  </div>

                  <button type="submit" className="btn btn-primary" disabled={waterfallLoading}>
                    {waterfallLoading ? <Loader2 className="spin" size={16} /> : <Zap size={16} />}
                    Find Decision Makers
                  </button>
                </form>
              </div>
            </div>
          </div>

          <div className="leadgen-results-panel">
            {waterfallLoading && (
              <div className="leadgen-job-progress">
                <Loader2 className="spin" size={16} />
                <span>Running waterfall ICP search...</span>
              </div>
            )}

            {waterfallResults.length > 0 && (
              <div className="card">
                <div className="card-header">
                  <h3>{waterfallResults.length} Decision Makers Found</h3>
                </div>
                <div className="table-container">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>ICP</th>
                        <th>Name</th>
                        <th>Title</th>
                        <th>Email</th>
                        <th>LinkedIn</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {waterfallResults.map((p, i) => (
                        <tr key={i}>
                          <td><span className="icp-badge">#{p.icp_level || i + 1}</span></td>
                          <td><strong>{p.full_name || '—'}</strong></td>
                          <td>{p.job_title || '—'}</td>
                          <td>{p.email || <span className="text-muted">Not found</span>}</td>
                          <td>
                            {p.linkedin_url ? (
                              <a href={p.linkedin_url} target="_blank" rel="noopener noreferrer" className="link-text">
                                <User size={12} /> Profile
                              </a>
                            ) : '—'}
                          </td>
                          <td>
                            {p.imported ? (
                              <span className="status-badge status-valid"><CheckCircle size={12} /> Imported</span>
                            ) : p.email ? (
                              <span className="status-badge status-duplicate">Duplicate</span>
                            ) : (
                              <span className="status-badge status-unknown">No Email</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {!waterfallLoading && waterfallResults.length === 0 && (
              <div className="leadgen-empty-state">
                <Zap size={48} />
                <h3>ICP Waterfall Search</h3>
                <p>Enter a company LinkedIn URL to find and import decision makers in cascading priority order.</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ---- Sub-tab 3: Credits & Jobs ---- */}
      {activeTab === 'credits' && (
        <div>
          <div className="credits-panel">
            <div className="card credits-card">
              <div className="card-header">
                <h3>BlitzAPI Credits</h3>
                <button className="btn btn-secondary btn-sm" onClick={fetchCredits} disabled={creditsLoading}>
                  {creditsLoading ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />}
                  Refresh
                </button>
              </div>
              <div className="card-body">
                {creditsLoading && <div className="loading-text"><Loader2 className="spin" size={16} /> Loading...</div>}
                {credits && !creditsLoading && (
                  <div className="credits-info">
                    <div className="credits-stat">
                      <span className="credits-number">{(credits.remaining_credits || 0).toLocaleString()}</span>
                      <span className="credits-label">Remaining Credits</span>
                    </div>
                    <div className="credits-meta">
                      <div><strong>Status:</strong> {credits.valid ? <span className="text-success">Valid</span> : <span className="text-danger">Invalid</span>}</div>
                      {credits.next_reset_at && <div><strong>Next Reset:</strong> {new Date(credits.next_reset_at).toLocaleDateString()}</div>}
                      {credits.active_plans?.length > 0 && (
                        <div><strong>Plan:</strong> {credits.active_plans[0].name} ({credits.active_plans[0].status})</div>
                      )}
                      {credits.allowed_apis?.length > 0 && (
                        <div><strong>APIs:</strong> {credits.allowed_apis.join(', ')}</div>
                      )}
                    </div>
                  </div>
                )}
                {!credits && !creditsLoading && (
                  <p className="text-muted">Configure your BlitzAPI key in Settings to view credit balance.</p>
                )}
              </div>
            </div>
          </div>

          <div className="card" style={{ marginTop: 24 }}>
            <div className="card-header">
              <h3>Recent Jobs</h3>
              <button className="btn btn-secondary btn-sm" onClick={fetchJobs} disabled={jobsLoading}>
                {jobsLoading ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />}
                Refresh
              </button>
            </div>
            <div className="table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Job ID</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Results</th>
                    <th>Imported</th>
                    <th>Credits</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.length === 0 ? (
                    <tr><td colSpan={7} className="empty-state-cell">No jobs yet</td></tr>
                  ) : jobs.map(job => (
                    <tr key={job.id}>
                      <td className="font-mono text-sm">{job.id?.slice(0, 8)}...</td>
                      <td>{job.job_type}</td>
                      <td>
                        <span className={`status-badge status-${job.status === 'completed' ? 'valid' : job.status === 'failed' ? 'invalid' : 'pending'}`}>
                          {job.status}
                        </span>
                      </td>
                      <td>{job.results_count || 0}</td>
                      <td>{job.imported_count || 0}</td>
                      <td>{job.credits_used || 0}</td>
                      <td>{job.created_at ? new Date(job.created_at).toLocaleDateString() : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Main App
const validPages = ['inbox', 'pipeline', 'dashboard', 'contacts', 'duplicates', 'enrichment', 'leadgen', 'campaigns', 'templates', 'reports', 'settings'];
const getPageFromHash = () => {
  const hash = window.location.hash.slice(1);
  return validPages.includes(hash) ? hash : 'inbox';
};

function App() {
  const [user, setUser] = useState(null);
  const [page, setPageState] = useState(getPageFromHash);
  const [loading, setLoading] = useState(true);

  // Sync page state with URL hash
  const setPage = useCallback((newPage) => {
    setPageState(newPage);
    window.location.hash = newPage;
  }, []);

  // Handle browser back/forward buttons
  useEffect(() => {
    const handleHashChange = () => setPageState(getPageFromHash());
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  useEffect(() => { const check = async () => { if (api.token) { try { const u = await api.get('/auth/me'); setUser(u); } catch { api.setToken(null); } } setLoading(false); }; check(); }, []);

  const handleLogout = () => { api.setToken(null); setUser(null); };

  if (loading) return <div className="loading-screen"><Loader2 className="spin" size={32} /></div>;
  if (!user) return <ToastProvider><LoginPage onLogin={setUser} /></ToastProvider>;

  return (<ToastProvider><WorkspaceProvider><ImportJobProvider><div className="app"><Sidebar page={page} setPage={setPage} user={user} onLogout={handleLogout} /><main className="main-content" id="main-content">
    {page === 'inbox' && <InboxPage setPage={setPage} />}
    {page === 'pipeline' && <PipelinePage />}
    {page === 'contacts' && <ContactsPage />}
    {page === 'campaigns' && <CampaignsPage />}
    {page === 'templates' && <TemplatesPage />}
    {page === 'reports' && <ReportsPage />}
    {page === 'settings' && <SettingsPage />}
    {/* Legacy routes still accessible */}
    {page === 'dashboard' && <DashboardPage />}
    {page === 'duplicates' && <DuplicatesPage />}
    {page === 'enrichment' && <EnrichmentPage />}
    {page === 'leadgen' && <LeadGenPage />}
  </main></div></ImportJobProvider></WorkspaceProvider></ToastProvider>);
}


// ============================================================
// REPORTS PAGE — Pipeline health, campaign performance
// ============================================================
const ReportsPage = () => {
  const { addToast } = useToast();
  const [stats, setStats] = useState(null);
  const [campaigns, setCampaigns] = useState([]);
  const [learning, setLearning] = useState(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('overview');

  useEffect(() => {
    Promise.all([
      api.get('/stats'),
      api.get('/campaigns'),
      api.get('/analytics/learning').catch(() => null),
    ]).then(([s, c, l]) => {
      setStats(s);
      setCampaigns(c.data || []);
      setLearning(l);
      setLoading(false);
    }).catch(e => { addToast(e.message, 'error'); setLoading(false); });
  }, []);

  if (loading) return <div style={{padding:40,textAlign:'center'}}><Loader2 className="spin" size={28} /></div>;

  const totalSent = campaigns.reduce((a,c) => a+(c.emails_sent||0), 0);
  const totalOpened = campaigns.reduce((a,c) => a+(c.emails_opened||0), 0);
  const totalReplied = campaigns.reduce((a,c) => a+(c.emails_replied||0), 0);
  const totalBounced = campaigns.reduce((a,c) => a+(c.emails_bounced||0), 0);
  const openRate = totalSent > 0 ? ((totalOpened/totalSent)*100).toFixed(1) : 0;
  const replyRate = totalSent > 0 ? ((totalReplied/totalSent)*100).toFixed(1) : 0;
  const bounceRate = totalSent > 0 ? ((totalBounced/totalSent)*100).toFixed(1) : 0;

  const Metric = ({label, value, sub, color}) => (
    <div className="card" style={{padding:'20px 24px', flex:1, minWidth:160}}>
      <div style={{fontSize:28, fontWeight:700, color:color||'var(--text-primary)'}}>{value}</div>
      <div style={{fontWeight:600, fontSize:13, marginTop:4}}>{label}</div>
      {sub && <div style={{fontSize:12, color:'var(--text-secondary)', marginTop:2}}>{sub}</div>}
    </div>
  );

  const topCampaigns = [...campaigns]
    .sort((a,b) => (b.emails_replied||0)-(a.emails_replied||0))
    .slice(0,10);

  return (
    <div className="page-container">
      <div className="page-header">
        <div className="page-title">
          <TrendingUp size={24} style={{color:'var(--coral)'}} />
          <div>
            <h1>Reports</h1>
            <p className="page-subtitle">Pipeline health and campaign performance</p>
          </div>
        </div>
      </div>

      {/* Tab bar */}
      <div style={{display:'flex', gap:4, marginBottom:20, borderBottom:'1px solid var(--border)', paddingBottom:0}}>
        {[{id:'overview',label:'Overview'},{id:'campaigns',label:'Campaigns'},{id:'learning',label:"What's Working"}].map(t => (
          <button key={t.id} onClick={() => setActiveTab(t.id)}
            style={{padding:'8px 20px', border:'none', cursor:'pointer', fontWeight: activeTab===t.id ? 600 : 400,
              borderBottom: activeTab===t.id ? '2px solid var(--coral)' : '2px solid transparent',
              background:'transparent', color: activeTab===t.id ? 'var(--coral)' : 'var(--text-secondary)',
              fontSize:14, transition:'all 0.15s'}}>
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === 'overview' && (
        <div style={{display:'flex', flexDirection:'column', gap:20}}>
          {/* Contact pipeline */}
          <div>
            <h3 style={{fontSize:13, textTransform:'uppercase', letterSpacing:'0.05em', color:'var(--text-secondary)', marginBottom:12}}>Contact Pipeline</h3>
            <div style={{display:'flex', gap:12, flexWrap:'wrap'}}>
              <Metric label="Total Contacts" value={(stats?.total_contacts||0).toLocaleString()} />
              <Metric label="Pushed to ReachInbox" value={(stats?.pushed_contacts||0).toLocaleString()} color="var(--coral)" />
              <Metric label="HubSpot Synced" value={(stats?.hubspot_synced||0).toLocaleString()} color="#f59e0b" />
              <Metric label="Duplicates" value={(stats?.duplicate_contacts||0).toLocaleString()} color="var(--text-secondary)" />
            </div>
          </div>

          {/* Email stats */}
          <div>
            <h3 style={{fontSize:13, textTransform:'uppercase', letterSpacing:'0.05em', color:'var(--text-secondary)', marginBottom:12}}>Email Performance</h3>
            <div style={{display:'flex', gap:12, flexWrap:'wrap'}}>
              <Metric label="Emails Sent" value={totalSent.toLocaleString()} />
              <Metric label="Open Rate" value={`${openRate}%`} sub={`${totalOpened.toLocaleString()} opened`} color={openRate>=30?'var(--green)':openRate>=20?'#f59e0b':'#ef4444'} />
              <Metric label="Reply Rate" value={`${replyRate}%`} sub={`${totalReplied.toLocaleString()} replies`} color={replyRate>=5?'var(--green)':replyRate>=2?'#f59e0b':'#ef4444'} />
              <Metric label="Bounce Rate" value={`${bounceRate}%`} sub={`${totalBounced.toLocaleString()} bounced`} color={bounceRate<3?'var(--green)':bounceRate<8?'#f59e0b':'#ef4444'} />
            </div>
          </div>

          {/* Workspace split */}
          <div>
            <h3 style={{fontSize:13, textTransform:'uppercase', letterSpacing:'0.05em', color:'var(--text-secondary)', marginBottom:12}}>By Workspace</h3>
            <div style={{display:'flex', gap:12, flexWrap:'wrap'}}>
              <Metric label="🇺🇸 US Contacts" value={(stats?.us_contacts||0).toLocaleString()} />
              <Metric label="🇲🇽 MX Contacts" value={(stats?.mx_contacts||0).toLocaleString()} />
              <Metric label="US Campaigns" value={campaigns.filter(c=>c.country==='United States'||c.country==='US').length} />
              <Metric label="MX Campaigns" value={campaigns.filter(c=>c.country==='Mexico'||c.country==='MX').length} />
            </div>
          </div>
        </div>
      )}

      {activeTab === 'campaigns' && (
        <div className="card" style={{overflow:'hidden'}}>
          <div className="table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Campaign</th>
                  <th>Workspace</th>
                  <th>Sent</th>
                  <th>Open %</th>
                  <th>Reply %</th>
                  <th>Bounce %</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {topCampaigns.map(c => {
                  const sent = c.emails_sent||0;
                  const oRate = sent>0?((c.emails_opened||0)/sent*100).toFixed(1):0;
                  const rRate = sent>0?((c.emails_replied||0)/sent*100).toFixed(1):0;
                  const bRate = sent>0?((c.emails_bounced||0)/sent*100).toFixed(1):0;
                  return (
                    <tr key={c.id}>
                      <td><strong>{c.name}</strong></td>
                      <td>
                        <span className={`workspace-badge workspace-${(c.country==='Mexico'||c.country==='MX')?'mx':'us'}`}>
                          {(c.country==='Mexico'||c.country==='MX')?'🇲🇽 MX':'🇺🇸 US'}
                        </span>
                      </td>
                      <td>{sent.toLocaleString()}</td>
                      <td style={{color:oRate>=30?'var(--green)':oRate>=20?'#f59e0b':'inherit'}}>{oRate}%</td>
                      <td style={{color:rRate>=5?'var(--green)':rRate>=2?'#f59e0b':'inherit'}}>{rRate}%</td>
                      <td style={{color:bRate<3?'var(--green)':bRate<8?'#f59e0b':'#ef4444'}}>{bRate}%</td>
                      <td>
                        <span style={{fontSize:11, padding:'2px 8px', borderRadius:10,
                          background:c.status==='Active'?'#dcfce7':c.status==='Paused'?'#fef9c3':'#f3f4f6',
                          color:c.status==='Active'?'#166534':c.status==='Paused'?'#854d0e':'#6b7280', fontWeight:600}}>
                          {c.status}
                        </span>
                      </td>
                    </tr>
                  );
                })}
                {topCampaigns.length === 0 && (
                  <tr><td colSpan={7} style={{textAlign:'center', color:'var(--text-secondary)', padding:32}}>No campaigns yet</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === 'learning' && (
        <div>
          {!learning ? (
            <div className="card" style={{padding:40, textAlign:'center', color:'var(--text-secondary)'}}>
              <TrendingUp size={40} style={{marginBottom:16, opacity:0.3}} />
              <h3>Not enough data yet</h3>
              <p>Learning insights appear after your campaigns have enough send data.</p>
            </div>
          ) : (
            <div style={{display:'flex', flexDirection:'column', gap:16}}>
              {learning.best_performing_templates?.length > 0 && (
                <div className="card" style={{padding:20}}>
                  <h3 style={{marginBottom:12}}>🏆 Top Templates</h3>
                  {learning.best_performing_templates.map((t,i) => (
                    <div key={i} style={{display:'flex', justifyContent:'space-between', padding:'8px 0',
                      borderBottom:'1px solid var(--border)'}}>
                      <span>{t.template_name || `Template ${t.template_id}`}</span>
                      <span style={{color:'var(--green)', fontWeight:600}}>{t.reply_rate}% reply</span>
                    </div>
                  ))}
                </div>
              )}
              {learning.subject_line_patterns && (
                <div className="card" style={{padding:20}}>
                  <h3 style={{marginBottom:12}}>💡 Subject Line Patterns</h3>
                  <p style={{color:'var(--text-secondary)', fontSize:13}}>{learning.subject_line_patterns}</p>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};


// ============================================================
// INBOX PAGE — What needs your attention
// ============================================================
const InboxPage = ({ setPage }) => {
  const { addToast } = useToast();
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const { data: stats } = useData('/stats');

  const { workspace } = useWorkspace();

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const data = await api.get(`/leadgen/jobs?workspace=${workspace}`);
      setJobs((data.data || []).filter(j => j.status === 'awaiting_approval' || j.status === 'failed'));
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { fetchJobs(); }, [workspace]);

  const pendingJobs = jobs.filter(j => j.status === 'awaiting_approval');
  const pendingContacts = pendingJobs.reduce((acc, j) => acc + (j.results_count || 0), 0);

  return (
    <div className="page-v2">
      <div className="page-v2-header">
        <div>
          <h1 className="page-v2-title">Inbox</h1>
          <p className="page-v2-subtitle">Review and approve leads generated by your pipeline</p>
        </div>
      </div>

      {/* Quick Stats Row */}
      <div className="stats-row">
        <div className="stat-pill">
          <div className="stat-pill-value">{pendingJobs.length}</div>
          <div className="stat-pill-label">Pending Runs</div>
        </div>
        <div className="stat-pill">
          <div className="stat-pill-value">{pendingContacts}</div>
          <div className="stat-pill-label">Contacts to Review</div>
        </div>
        <div className="stat-pill">
          <div className="stat-pill-value">{(stats?.unique_contacts || 0).toLocaleString()}</div>
          <div className="stat-pill-label">Total Contacts</div>
        </div>
        <div className="stat-pill">
          <div className="stat-pill-value">{stats?.total_campaigns || 0}</div>
          <div className="stat-pill-label">Active Campaigns</div>
        </div>
      </div>

      {/* Approval Queue */}
      {loading ? (
        <div className="empty-state-v2"><Loader2 className="spin" size={24} /><p>Loading...</p></div>
      ) : pendingJobs.length === 0 ? (
        <div className="empty-state-v2">
          <div className="empty-state-icon"><CheckCircle size={32} /></div>
          <h3>You're all caught up</h3>
          <p>No pending approvals right now. New leads will appear here when the pipeline runs.</p>
          <div className="empty-state-actions">
            <button className="btn-v2 btn-v2-secondary" onClick={() => setPage('pipeline')}>View Pipeline</button>
            <button className="btn-v2 btn-v2-secondary" onClick={() => setPage('contacts')}>Browse Contacts</button>
          </div>
        </div>
      ) : (
        <div className="approval-queue">
          <div className="queue-header">
            <span className="queue-header-title">Approval Queue</span>
            <span className="queue-header-count">{pendingJobs.length} runs</span>
          </div>
          {pendingJobs.map(job => {
            const params = (() => { try { return JSON.parse(job.parameters || '{}'); } catch { return {}; } })();
            return (
              <div key={job.id} className="queue-item">
                <div className="queue-item-left">
                  <div className="queue-item-icon"><Target size={18} /></div>
                  <div className="queue-item-info">
                    <div className="queue-item-title">
                      {params.label || params.vertical || job.job_type}
                    </div>
                    <div className="queue-item-meta">
                      {job.results_count} contacts · {new Date(job.created_at).toLocaleDateString(undefined, {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})}
                    </div>
                  </div>
                </div>
                <div className="queue-item-right">
                  <button className="btn-v2 btn-v2-ghost" onClick={() => setPage('pipeline')}>Review</button>
                  <ApproveJobButton jobId={job.id} count={job.results_count} onDone={fetchJobs} />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

// ============================================================

const ApproveJobButton = ({ jobId, count, onDone }) => {
  const { addToast } = useToast();
  const [loading, setLoading] = useState(false);
  const handleApprove = async () => {
    setLoading(true);
    try {
      const result = await api.post('/leadgen/contacts/approve', { job_id: jobId });
      addToast(`${result.imported} contacts imported! ${result.skipped_duplicates} duplicates skipped.`, 'success');
      onDone();
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };
  return (
    <button className="btn-v2 btn-v2-primary" disabled={loading} onClick={handleApprove}>
      {loading ? <Loader2 className="spin" size={14} /> : <Check size={14} />}
      Approve ({count})
    </button>
  );
};

// PIPELINE PAGE — All runs, companies, staged contacts
// ============================================================
const PipelinePage = () => {
  const { addToast } = useToast();
  const [jobs, setJobs] = useState([]);
  const [selectedJob, setSelectedJob] = useState(null);
  const [jobDetail, setJobDetail] = useState(null);
  const [stagedContacts, setStagedContacts] = useState([]);
  const [selectedContacts, setSelectedContacts] = useState([]);
  const [activeTab, setActiveTab] = useState('companies');
  const [loading, setLoading] = useState(true);
  const [approveLoading, setApproveLoading] = useState(false);

  const { workspace } = useWorkspace();
  const fetchJobs = async () => {
    setLoading(true);
    try {
      const data = await api.get(`/leadgen/jobs?workspace=${workspace}`);
      setJobs(data.data || []);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { fetchJobs(); }, [workspace]);

  const selectJob = async (job) => {
    setSelectedJob(job);
    setActiveTab('companies');
    try {
      const detail = await api.get(`/leadgen/jobs/${job.id}`);
      setJobDetail(detail);
      const preview = await api.get(`/leadgen/contacts/preview?job_id=${job.id}`);
      setStagedContacts(preview.contacts || []);
      setSelectedContacts((preview.contacts || []).filter(c => c.status === 'pending').map(c => c.id));
    } catch (e) { addToast(e.message, 'error'); }
  };

  const handleApprove = async () => {
    if (selectedContacts.length === 0) return;
    setApproveLoading(true);
    try {
      const result = await api.post('/leadgen/contacts/approve', { contact_ids: selectedContacts });
      addToast(`${result.imported} imported, ${result.skipped_duplicates} dupes skipped`, 'success');
      const preview = await api.get(`/leadgen/contacts/preview?job_id=${selectedJob.id}`);
      setStagedContacts(preview.contacts || []);
      setSelectedContacts([]);
      fetchJobs();
    } catch (e) { addToast(e.message, 'error'); }
    setApproveLoading(false);
  };

  const pendingContacts = stagedContacts.filter(c => c.status === 'pending');

  const statusColor = (status) => ({
    'awaiting_approval': '#f59e0b',
    'completed': '#10b981',
    'running': '#3b82f6',
    'failed': '#ef4444',
    'pending': '#6b7280',
    'bulk_run': '#8b5cf6',
  }[status] || '#6b7280');

  const tierBadge = (tier) => {
    if (!tier) return null;
    const config = { 1: { bg: '#dcfce7', color: '#166534', label: 'Owner' }, 2: { bg: '#fef9c3', color: '#854d0e', label: 'GM' }, 3: { bg: '#f3f4f6', color: '#374151', label: 'Ops' } };
    const c = config[tier] || config[3];
    return <span className="tier-badge" style={{background: c.bg, color: c.color}}>T{tier} {c.label}</span>;
  };

  return (
    <div className="page-v2">
      <div className="page-v2-header">
        <div>
          <h1 className="page-v2-title">Pipeline</h1>
          <p className="page-v2-subtitle">Enrichment runs, staged companies and contacts</p>
        </div>
        <button className="btn-v2 btn-v2-ghost" onClick={fetchJobs}><RefreshCw size={14} /> Refresh</button>
      </div>

      <div className="pipeline-layout">
        {/* Runs Panel */}
        <div className="pipeline-runs">
          <div className="panel-header">
            <span>Runs</span>
            <span className="panel-count">{jobs.length}</span>
          </div>
          {loading ? (
            <div className="panel-empty"><Loader2 className="spin" size={18} /></div>
          ) : jobs.length === 0 ? (
            <div className="panel-empty"><p>No runs yet</p></div>
          ) : (
            <div className="runs-list">
              {jobs.map(job => {
                const params = (() => { try { return JSON.parse(job.parameters || '{}'); } catch { return {}; } })();
                const isActive = selectedJob?.id === job.id;
                return (
                  <button key={job.id} className={`run-item ${isActive ? 'run-item-active' : ''}`} onClick={() => selectJob(job)}>
                    <div className="run-item-top">
                      <span className="run-item-name">{params.label || params.vertical || job.job_type}</span>
                      <span className="run-status-dot" style={{background: statusColor(job.status)}} title={job.status}></span>
                    </div>
                    <div className="run-item-bottom">
                      <span>{job.results_count || 0} contacts</span>
                      <span>{new Date(job.created_at).toLocaleDateString(undefined, {month:'short', day:'numeric'})}</span>
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* Detail Panel */}
        <div className="pipeline-detail">
          {!selectedJob ? (
            <div className="pipeline-detail-empty">
              <Target size={32} style={{color:'var(--text-3)', marginBottom: 12}} />
              <p style={{color:'var(--text-2)'}}>Select a run to see details</p>
            </div>
          ) : (
            <>
              {/* Detail Header */}
              <div className="detail-header">
                <div className="detail-header-info">
                  <h2 className="detail-title">{(() => { try { return JSON.parse(selectedJob.parameters || '{}'); } catch { return {}; } })().label || selectedJob.job_type}</h2>
                  <div className="detail-meta">
                    <span className="detail-status" style={{color: statusColor(selectedJob.status)}}>{selectedJob.status?.replace('_', ' ')}</span>
                    <span className="detail-sep">·</span>
                    <span>{new Date(selectedJob.created_at).toLocaleString()}</span>
                  </div>
                </div>
                {pendingContacts.length > 0 && (
                  <button className="btn-v2 btn-v2-primary" disabled={approveLoading || selectedContacts.length === 0} onClick={handleApprove}>
                    {approveLoading ? <Loader2 className="spin" size={14} /> : <Check size={14} />}
                    Approve ({selectedContacts.length})
                  </button>
                )}
              </div>

              {/* Tabs */}
              <div className="detail-tabs">
                {['companies', 'contacts'].map(t => (
                  <button key={t} className={`detail-tab ${activeTab === t ? 'detail-tab-active' : ''}`} onClick={() => setActiveTab(t)}>
                    {t === 'companies' ? <Building2 size={14} /> : <Users size={14} />}
                    {t === 'companies' ? `Companies (${jobDetail?.companies?.length || 0})` : `Contacts (${stagedContacts.length})`}
                  </button>
                ))}
              </div>

              {/* Companies Tab */}
              {activeTab === 'companies' && (
                <div className="detail-table-wrap">
                  <table className="table-v2">
                    <thead>
                      <tr><th>Company</th><th>Industry</th><th>Size</th><th>Location</th><th>Domain</th></tr>
                    </thead>
                    <tbody>
                      {(jobDetail?.companies || []).map(c => (
                        <tr key={c.id}>
                          <td className="table-v2-primary">{c.name}</td>
                          <td>{c.industry || '—'}</td>
                          <td>{c.size || '—'}</td>
                          <td>{c.hq_city ? `${c.hq_city}, ${c.hq_country}` : c.hq_country || '—'}</td>
                          <td className="table-v2-mono">{c.domain || '—'}</td>
                        </tr>
                      ))}
                      {(!jobDetail?.companies || jobDetail.companies.length === 0) && (
                        <tr><td colSpan={5} className="table-v2-empty">No companies in this run</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Contacts Tab */}
              {activeTab === 'contacts' && (
                <div className="detail-table-wrap">
                  {pendingContacts.length > 0 && (
                    <div className="detail-bulk-bar">
                      <label className="bulk-select-all">
                        <input type="checkbox"
                          checked={selectedContacts.length === pendingContacts.length && pendingContacts.length > 0}
                          onChange={() => setSelectedContacts(selectedContacts.length === pendingContacts.length ? [] : pendingContacts.map(c=>c.id))} />
                        Select all ({pendingContacts.length})
                      </label>
                    </div>
                  )}
                  <table className="table-v2">
                    <thead>
                      <tr><th style={{width:36}}></th><th>Name</th><th>Title</th><th>ICP</th><th>Email</th><th>Company</th><th>Status</th></tr>
                    </thead>
                    <tbody>
                      {stagedContacts.map(c => (
                        <tr key={c.id} className={c.status !== 'pending' ? 'row-muted' : ''}>
                          <td>
                            {c.status === 'pending' && (
                              <input type="checkbox" className="check-v2" checked={selectedContacts.includes(c.id)}
                                onChange={() => setSelectedContacts(prev => prev.includes(c.id) ? prev.filter(x=>x!==c.id) : [...prev, c.id])} />
                            )}
                          </td>
                          <td className="table-v2-primary">{c.first_name} {c.last_name}</td>
                          <td>{c.title || '—'}</td>
                          <td>{tierBadge(c.icp_tier)}</td>
                          <td className="table-v2-mono">{c.email || <span className="no-data">No email</span>}</td>
                          <td>{c.company_name || '—'}</td>
                          <td>
                            <span className="status-pill" style={{color: statusColor(c.status), background: `${statusColor(c.status)}15`}}>
                              {c.status === 'approved' ? '✓ Imported' : c.status === 'rejected' ? 'Skipped' : 'Pending'}
                            </span>
                          </td>
                        </tr>
                      ))}
                      {stagedContacts.length === 0 && (
                        <tr><td colSpan={7} className="table-v2-empty">No contacts staged yet</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
};



export default App;
