import React, { useState, useEffect, useCallback, useRef, createContext, useContext } from 'react';
import { LayoutDashboard, Users, Mail, FileText, Settings, Search, Plus, Trash2, X, Check, ArrowUpDown, Filter, Download, Upload, Edit2, LogOut, UserPlus, RefreshCw, ChevronLeft, ChevronRight, ChevronDown, ChevronUp, Loader2, AlertCircle, CheckCircle, Copy, ArrowRight, Layers, Merge, Eye, Webhook, Database, Send, Target, MessageCircle, MessageSquare, Zap, GitMerge, AlertTriangle, Trophy, List, LayoutGrid, Sparkles, Building2, User, ArrowRightLeft, Bold, Italic, Type, TrendingUp } from 'lucide-react';
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
const Sidebar = ({ page, setPage, user, onLogout }) => {
  const { data: stats } = useData('/stats');
  const { importJob, clearImportJob } = useImportJob();
  const { addToast } = useToast();
  const nav = [{ id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard }, { id: 'contacts', label: 'Contacts', icon: Users }, { id: 'duplicates', label: 'Duplicates', icon: Layers }, { id: 'enrichment', label: 'Enrichment', icon: Sparkles }, { id: 'campaigns', label: 'Campaigns', icon: Mail }, { id: 'templates', label: 'Templates', icon: FileText }, { id: 'settings', label: 'Settings', icon: Settings }];

  // Show toast when import completes
  useEffect(() => {
    if (importJob?.status === 'completed') {
      addToast(`Import complete: ${importJob.imported_count} imported, ${importJob.merged_count} merged`, 'success');
    } else if (importJob?.status === 'failed') {
      addToast(`Import failed: ${importJob.error_message || 'Unknown error'}`, 'error');
    }
  }, [importJob?.status]);

  const progress = importJob?.total_rows > 0 ? Math.round((importJob.processed_count / importJob.total_rows) * 100) : 0;

  return (<aside className="sidebar">
    <div className="sidebar-header">
      <div className="logo">
        <div className="logo-icon">D</div>
        <div className="logo-content">
          <span className="logo-text">Deduply</span>
          <span className="logo-tagline">by Arkode</span>
        </div>
      </div>
    </div>
    <nav className="sidebar-nav">{nav.map(item => (<button key={item.id} className={`nav-item ${page === item.id ? 'active' : ''}`} onClick={() => setPage(item.id)}><item.icon size={20} /><span>{item.label}</span>{item.id === 'contacts' && stats && <span className="nav-badge">{stats.unique_contacts?.toLocaleString()}</span>}{item.id === 'duplicates' && stats?.duplicates > 0 && <span className="nav-badge danger">{stats.duplicates}</span>}{item.id === 'campaigns' && stats && <span className="nav-badge">{stats.total_campaigns}</span>}</button>))}</nav>

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
      {user && (<div className="user-info"><div className="user-avatar">{user.name?.[0] || user.email[0]}</div><div className="user-details"><span className="user-name">{user.name || user.email}</span><span className="user-role">{user.role}</span></div><button className="logout-btn" onClick={onLogout} title="Logout"><LogOut size={18} /></button></div>)}
      <div className="sidebar-brand">
        <span className="brand-powered">Powered by</span>
        <span className="brand-arkode">Arkode</span>
      </div>
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
  }, [activeTab, dbStats, perfStats, funnelStats]);

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
                {['Lead', 'Contacted', 'Replied', 'Scheduled', 'Show', 'Qualified', 'Client'].map((stage, idx, arr) => {
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
  const [showFilters, setShowFilters] = useState(false);
  const [showColumns, setShowColumns] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const [showAddContact, setShowAddContact] = useState(false);
  const [editingCell, setEditingCell] = useState(null);
  const [bulkField, setBulkField] = useState('');
  const [bulkAction, setBulkAction] = useState('set');
  const [bulkValue, setBulkValue] = useState('');
  const [bulkLoading, setBulkLoading] = useState(false);
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
    { id: 'country', label: 'Country', editable: true, default: false },
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
  const activeFiltersCount = Object.values(filters).filter(v => v).length;

  const fetchContacts = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ page, page_size: pageSize, sort_by: sortBy, sort_order: sortOrder });
      if (search) params.append('search', search);
      Object.entries(filters).forEach(([k, v]) => { if (v) params.append(k, v); });
      const r = await api.get(`/contacts?${params}`);
      setContacts(r.data); setTotal(r.total);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  }, [page, pageSize, search, filters, sortBy, sortOrder, addToast]);

  useEffect(() => { fetchContacts(); }, [fetchContacts]);

  const toggleSelect = (id) => { const n = new Set(selected); n.has(id) ? n.delete(id) : n.add(id); setSelected(n); };
  const toggleSelectAll = () => { if (selectAll) { setSelected(new Set()); setSelectAll(false); } else { setSelected(new Set(contacts.map(c => c.id))); setSelectAll(true); } };

  const executeBulkAction = async () => {
    if (!bulkField) return;
    if (bulkField !== 'delete' && !bulkValue && bulkAction !== 'remove') return;
    setBulkLoading(true);
    try {
      const payload = {
        field: bulkField === 'delete' ? 'id' : bulkField,
        action: bulkField === 'delete' ? 'delete' : bulkAction,
        value: bulkValue
      };
      // If selectAll is true, send filters instead of IDs
      if (selectAll) {
        payload.filters = { search, ...filters };
      } else {
        payload.contact_ids = Array.from(selected);
      }
      const result = await api.post('/contacts/bulk', payload);
      addToast(`Updated ${result.updated.toLocaleString()} contacts`, 'success');
      setSelected(new Set()); setSelectAll(false);
      setBulkField(''); setBulkAction('set'); setBulkValue('');
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
    setFilters(view.filters || {});
    setVisibleColumns(view.visibleColumns || allColumns.filter(c => c.default).map(c => c.id));
    setSearch(view.search || '');
    if (view.sortBy) setSortBy(view.sortBy);
    if (view.sortOrder) setSortOrder(view.sortOrder);
    setPage(1);
    addToast(`Loaded view: ${view.name}`, 'success');
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
    { id: 'country_strategy', label: 'Country Strategy', type: 'select', options: filterOptions?.country_strategies || [] },
    { id: 'campaigns_assigned', label: 'Campaign', type: 'list', options: filterOptions?.campaigns || [] },
    { id: 'outreach_lists', label: 'Outreach List', type: 'list', options: filterOptions?.outreach_lists || [] },
    { id: 'seniority', label: 'Seniority', type: 'select', options: filterOptions?.seniorities || [] },
    { id: 'industry', label: 'Industry', type: 'select', options: filterOptions?.industries || [] },
    { id: 'delete', label: 'Delete Contacts', type: 'action' }
  ];

  const selectedBulkField = bulkEditableFields.find(f => f.id === bulkField);

  const saveEdit = async (contactId, field, value) => {
    try { await api.put(`/contacts/${contactId}`, { [field]: value }); fetchContacts(); addToast('Updated', 'success'); } catch (e) { addToast(e.message, 'error'); }
    setEditingCell(null);
  };

  const handleSort = (col) => { if (sortBy === col) setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc'); else { setSortBy(col); setSortOrder('desc'); } };

  const selectedCount = selectAll ? total : selected.size;
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
                        <button className="view-load-btn" onClick={() => { loadView(v); setShowViewsDropdown(false); }}>
                          <Eye size={14} /><span>{v.name}</span>
                        </button>
                        <button className="view-delete-btn" onClick={() => deleteView(v.id)}><Trash2 size={14} /></button>
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
      <div className="filters-panel">
        <div className="filters-row">
          <div className="filter-group">
            <label>Status</label>
            <select value={filters.status || ''} onChange={e => setFilters({ ...filters, status: e.target.value })}>
              <option value="">All Statuses</option>
              {filterOptions.statuses?.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label>Country Strategy</label>
            <select value={filters.country_strategy || ''} onChange={e => setFilters({ ...filters, country_strategy: e.target.value })}>
              <option value="">All Strategies</option>
              {filterOptions.country_strategies?.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label>Campaign</label>
            <select value={filters.campaigns || ''} onChange={e => setFilters({ ...filters, campaigns: e.target.value })}>
              <option value="">All Campaigns</option>
              {filterOptions.campaigns?.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label>Outreach List</label>
            <select value={filters.outreach_lists || ''} onChange={e => setFilters({ ...filters, outreach_lists: e.target.value })}>
              <option value="">All Lists</option>
              {filterOptions.outreach_lists?.map(l => <option key={l} value={l}>{l}</option>)}
            </select>
          </div>
        </div>
        <div className="filters-row">
          <div className="filter-group">
            <label>Country</label>
            <select value={filters.country || ''} onChange={e => setFilters({ ...filters, country: e.target.value })}>
              <option value="">All Countries</option>
              {filterOptions.countries?.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label>Seniority</label>
            <select value={filters.seniority || ''} onChange={e => setFilters({ ...filters, seniority: e.target.value })}>
              <option value="">All Seniorities</option>
              {filterOptions.seniorities?.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label>Industry</label>
            <select value={filters.industry || ''} onChange={e => setFilters({ ...filters, industry: e.target.value })}>
              <option value="">All Industries</option>
              {filterOptions.industries?.map(i => <option key={i} value={i}>{i}</option>)}
            </select>
          </div>
          <div className="filter-group">
            <label>Email Status</label>
            <select value={filters.email_status || ''} onChange={e => setFilters({ ...filters, email_status: e.target.value })}>
              <option value="">All Statuses</option>
              <option value="Not Verified">Not Verified</option>
              <option value="Valid">Valid</option>
              <option value="Invalid">Invalid</option>
              <option value="Unknown">Unknown</option>
            </select>
          </div>
          <div className="filter-group filter-actions">
            <button className="btn btn-text" onClick={() => setFilters({})}>Clear All Filters</button>
          </div>
        </div>
      </div>
    )}

    {selectedCount > 0 && (<div className="bulk-bar">
      <span className="bulk-count">{selectedCount.toLocaleString()} selected {selectAll && <span className="bulk-all-note">(all matching filters)</span>}</span>
      <div className="bulk-actions">
        <div className="bulk-group">
          <select value={bulkField} onChange={e => { setBulkField(e.target.value); setBulkValue(''); setBulkAction('set'); }} className="bulk-field-select">
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
            <select value={bulkValue} onChange={e => setBulkValue(e.target.value)} className="bulk-value-select">
              <option value="">Select value...</option>
              {selectedBulkField.options.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
          )}
          {selectedBulkField && selectedBulkField.type === 'action' && (
            <span className="bulk-warning"><AlertTriangle size={16} /> This will permanently delete {selectedCount.toLocaleString()} contacts</span>
          )}
          <button className="btn btn-primary" onClick={executeBulkAction} disabled={!bulkField || bulkLoading || (bulkField !== 'delete' && !bulkValue)}>
            {bulkLoading ? <Loader2 className="spin" size={16} /> : 'Apply'}
          </button>
        </div>
      </div>
      <button className="btn btn-text" onClick={() => { setSelected(new Set()); setSelectAll(false); setBulkField(''); }}><X size={14} /> Clear</button>
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
      <div className="form-group"><label>Country Strategy *</label><select value={countryStrategy} onChange={e => setCountryStrategy(e.target.value)} required><option value="">Select strategy...</option>{strategyOptions.map(s => <option key={s} value={s}>{s}</option>)}</select>{!countryStrategy && <span className="field-hint error">Required</span>}</div>
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
    { id: 'city', label: 'City' }, { id: 'state', label: 'State' }, { id: 'country', label: 'Country' },
    { id: 'company_city', label: 'Company City' }, { id: 'company_state', label: 'Company State' }, { id: 'company_country', label: 'Company Country' },
    { id: 'company_street_address', label: 'Company Address' }, { id: 'company_postal_code', label: 'Postal Code' },
    { id: 'employees', label: 'Employees' }, { id: 'employee_bucket', label: 'Employee Bucket' }, { id: 'industry', label: 'Industry' },
    { id: 'annual_revenue', label: 'Revenue' }, { id: 'annual_revenue_text', label: 'Revenue (Text)' },
    { id: 'company_description', label: 'Company Desc' }, { id: 'company_seo_description', label: 'SEO Desc' },
    { id: 'company_technologies', label: 'Technologies' }, { id: 'company_founded_year', label: 'Founded' }, { id: 'keywords', label: 'Keywords' },
    { id: 'country_strategy', label: 'Country Strategy' }, { id: 'outreach_lists', label: 'Outreach Lists' }, { id: 'campaigns_assigned', label: 'Campaigns' },
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
  const [nameTotalCount, setNameTotalCount] = useState(0);
  const [companyTotalCount, setCompanyTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [applying, setApplying] = useState(false);
  const [selectedNames, setSelectedNames] = useState([]);
  const [selectedCompanies, setSelectedCompanies] = useState([]);
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
    const ids = type === 'names' ? selectedNames : selectedCompanies;
    if (ids.length === 0) { addToast('Select items to apply', 'error'); return; }
    setApplying(true);
    try {
      const endpoint = type === 'names' ? '/cleaning/names/apply' : '/cleaning/companies/apply';
      const r = await api.post(endpoint, { contact_ids: ids, field: type });
      addToast(r.message, 'success');
      if (type === 'names') { setSelectedNames([]); fetchNamePreview(); }
      else { setSelectedCompanies([]); fetchCompanyPreview(); }
      fetchStats();
    } catch (e) { addToast(e.message, 'error'); }
    setApplying(false);
  };

  const handleApplyAll = async (type) => {
    const count = type === 'names' ? nameTotalCount : companyTotalCount;
    if (!window.confirm(`Apply cleaning to ALL ${count.toLocaleString()} ${type}?\n\nThis will process all items in the database, not just the visible ones.\n\nThis action cannot be undone.`)) return;
    setApplying(true);
    try {
      const endpoint = type === 'names' ? '/cleaning/names/apply-all' : '/cleaning/companies/apply-all';
      const r = await api.post(endpoint);
      addToast(r.message, 'success');
      if (type === 'names') fetchNamePreview(); else fetchCompanyPreview();
      fetchStats();
    } catch (e) { addToast(e.message, 'error'); }
    setApplying(false);
  };

  const toggleSelectAll = (type) => {
    if (type === 'names') {
      setSelectedNames(selectedNames.length === nameChanges.length ? [] : nameChanges.map(c => c.id));
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
        <div className="dup-stat-card">
          <div className="dup-stat-icon"><AlertTriangle size={24} /></div>
          <div className="dup-stat-info">
            <span className="dup-stat-value">{stats?.companies?.has_parentheses?.toLocaleString() || 0}</span>
            <span className="dup-stat-label">With Parentheses</span>
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
        <button className={`tab-btn ${activeTab === 'verify' ? 'active' : ''}`} onClick={() => setActiveTab('verify')}>
          <Mail size={18} /> Email Verification
          {verifyStatus?.unverified_count > 0 && <span className="tab-badge">{verifyStatus.unverified_count.toLocaleString()}</span>}
        </button>
      </div>

      {/* Action Bar */}
      {((activeTab === 'names' && nameChanges.length > 0) || (activeTab === 'companies' && companyChanges.length > 0)) && (
        <div className="duplicates-action-bar">
          <div className="action-bar-info">
            <input
              type="checkbox"
              checked={activeTab === 'names' ? selectedNames.length === nameChanges.length : selectedCompanies.length === companyChanges.length}
              onChange={() => toggleSelectAll(activeTab)}
            />
            <span>
              {activeTab === 'names' ? selectedNames.length : selectedCompanies.length} of {activeTab === 'names' ? nameChanges.length : companyChanges.length} selected
              {activeTab === 'names' && nameTotalCount > nameChanges.length && <span style={{opacity: 0.7}}> (showing {nameChanges.length} of {nameTotalCount} total)</span>}
              {activeTab === 'companies' && companyTotalCount > companyChanges.length && <span style={{opacity: 0.7}}> (showing {companyChanges.length} of {companyTotalCount} total)</span>}
            </span>
          </div>
          <div className="action-bar-buttons">
            <button
              className="btn btn-secondary"
              onClick={() => handleApplySelected(activeTab)}
              disabled={applying || (activeTab === 'names' ? selectedNames.length === 0 : selectedCompanies.length === 0)}
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
              Apply All ({activeTab === 'names' ? nameTotalCount.toLocaleString() : companyTotalCount.toLocaleString()})
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
                          <div className="contact-name">{c.first_name} {c.last_name}</div>
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
  const [campaigns, setCampaigns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [editData, setEditData] = useState({});
  const [expandedCampaign, setExpandedCampaign] = useState(null);
  const [campaignDetails, setCampaignDetails] = useState(null);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [filterStatus, setFilterStatus] = useState('');

  const fetchCampaigns = async () => {
    setLoading(true);
    try {
      const r = await api.get(`/campaigns${search ? `?search=${search}` : ''}`);
      setCampaigns(r.data);
    } catch (e) { addToast(e.message, 'error'); }
    setLoading(false);
  };

  useEffect(() => { fetchCampaigns(); }, [search]);

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

  const handleCreate = async (data) => { try { await api.post('/campaigns', data); addToast('Campaign created!', 'success'); setShowCreate(false); fetchCampaigns(); } catch (e) { addToast(e.message, 'error'); } };
  const handleUpdate = async (id) => { try { await api.put(`/campaigns/${id}`, editData); addToast('Campaign updated!', 'success'); setEditingId(null); fetchCampaigns(); if (expandedCampaign === id) fetchCampaignDetails(id); } catch (e) { addToast(e.message, 'error'); } };
  const handleDelete = async (id) => { if (!window.confirm('Delete this campaign?')) return; try { await api.delete(`/campaigns/${id}`); addToast('Campaign deleted', 'success'); fetchCampaigns(); } catch (e) { addToast(e.message, 'error'); } };

  const filteredCampaigns = filterStatus ? campaigns.filter(c => c.status === filterStatus) : campaigns;
  const totalSent = campaigns.reduce((s, c) => s + (c.emails_sent || 0), 0);
  const totalOpened = campaigns.reduce((s, c) => s + (c.emails_opened || 0), 0);
  const totalReplied = campaigns.reduce((s, c) => s + (c.emails_replied || 0), 0);
  const avgOpenRate = totalSent > 0 ? Math.round(100 * totalOpened / totalSent) : 0;
  const avgReplyRate = totalSent > 0 ? Math.round(100 * totalReplied / totalSent) : 0;

  return (<div className="page campaigns-page">
    <div className="page-header">
      <div>
        <h1>Campaigns</h1>
        <p className="subtitle">Manage and track your cold email campaigns</p>
      </div>
      <div className="header-actions">
        <button className="btn btn-secondary" onClick={fetchCampaigns}><RefreshCw size={16} /> Refresh</button>
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}><Plus size={16} /> New Campaign</button>
      </div>
    </div>

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
        <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)} className="filter-select">
          <option value="">All Status</option>
          <option value="Active">Active</option>
          <option value="Paused">Paused</option>
          <option value="Completed">Completed</option>
        </select>
      </div>
    </div>

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
                <button className="btn-icon-small" onClick={() => { setEditingId(camp.id); setEditData({...camp}); }} title="Edit Metrics"><Edit2 size={14} /></button>
                <button className="btn-icon-small danger" onClick={() => handleDelete(camp.id)} title="Delete"><Trash2 size={14} /></button>
              </div>
            </div>

            {expandedCampaign === camp.id && (
              <div className="campaign-expanded-content">
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
              </div>
            )}
          </div>
        ))}
      </div>
    )}

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
  </div>);
};

// Campaign Template Breakdown Component - Editable metrics
const CampaignTemplateBreakdown = ({ breakdown, campaignId, onUpdate, addToast }) => {
  const [editingId, setEditingId] = useState(null);
  const [editData, setEditData] = useState({});
  const [saving, setSaving] = useState(false);

  const startEdit = (variant) => {
    setEditingId(variant.id);
    setEditData({
      times_sent: variant.sent || 0,
      times_opened: variant.opened || 0,
      times_replied: variant.replied || 0,
      opportunities: variant.opportunities || 0,
      meetings: variant.meetings || 0
    });
  };

  const cancelEdit = () => {
    setEditingId(null);
    setEditData({});
  };

  const saveEdit = async (templateId) => {
    setSaving(true);
    try {
      const res = await fetch(`${API}/campaigns/${campaignId}/templates/${templateId}/metrics`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(editData)
      });
      if (!res.ok) throw new Error('Failed to save');
      addToast('Metrics updated successfully', 'success');
      setEditingId(null);
      setEditData({});
      if (onUpdate) onUpdate();
    } catch (err) {
      addToast('Failed to update metrics', 'error');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="template-breakdown">
      <div className="breakdown-header">
        <h4>Template Performance Breakdown</h4>
        <span className="breakdown-hint">Click on a row to edit metrics</span>
      </div>
      {breakdown.map((step, idx) => (
        <div key={idx} className="breakdown-step">
          <div className="breakdown-step-header">
            <div>
              <h5>{step.step_type}</h5>
              <span className="step-variant-count">{step.variants.length} variant{step.variants.length !== 1 ? 's' : ''}</span>
            </div>
            <div className="step-totals">
              <span className="step-total">Sent: {step.step_metrics.sent}</span>
              <span className="step-total">Opened: {step.step_metrics.opened}</span>
              <span className="step-total">Replied: {step.step_metrics.replied}</span>
              <span className="step-total highlight">Opps: {step.step_metrics.opportunities || 0}</span>
              <span className="step-total highlight">Meetings: {step.step_metrics.meetings || 0}</span>
            </div>
          </div>
          <div className="breakdown-variants">
            <table className="variants-table">
              <thead>
                <tr>
                  <th>Template</th>
                  <th>Variant</th>
                  <th>Sent</th>
                  <th>Opened</th>
                  <th>Replied</th>
                  <th>Opps</th>
                  <th>Meetings</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {step.variants.map(variant => (
                  <tr key={variant.id} className={editingId === variant.id ? 'editing-row' : ''}>
                    <td><strong>{variant.name}</strong></td>
                    <td><span className={`variant-badge variant-${variant.variant}`}>{variant.variant}</span></td>
                    {editingId === variant.id ? (
                      <>
                        <td><input type="number" className="cell-input-sm" value={editData.times_sent} onChange={e => setEditData({...editData, times_sent: parseInt(e.target.value) || 0})} min="0" /></td>
                        <td><input type="number" className="cell-input-sm" value={editData.times_opened} onChange={e => setEditData({...editData, times_opened: parseInt(e.target.value) || 0})} min="0" /></td>
                        <td><input type="number" className="cell-input-sm" value={editData.times_replied} onChange={e => setEditData({...editData, times_replied: parseInt(e.target.value) || 0})} min="0" /></td>
                        <td><input type="number" className="cell-input-sm highlight" value={editData.opportunities} onChange={e => setEditData({...editData, opportunities: parseInt(e.target.value) || 0})} min="0" /></td>
                        <td><input type="number" className="cell-input-sm highlight" value={editData.meetings} onChange={e => setEditData({...editData, meetings: parseInt(e.target.value) || 0})} min="0" /></td>
                        <td className="action-cell">
                          <button className="btn-icon-small success" onClick={() => saveEdit(variant.id)} disabled={saving} title="Save"><Check size={14} /></button>
                          <button className="btn-icon-small" onClick={cancelEdit} disabled={saving} title="Cancel"><X size={14} /></button>
                        </td>
                      </>
                    ) : (
                      <>
                        <td>{variant.sent || 0}</td>
                        <td>{variant.opened || 0}</td>
                        <td>{variant.replied || 0}</td>
                        <td className="metric-cell highlight">{variant.opportunities || 0}</td>
                        <td className="metric-cell highlight">{variant.meetings || 0}</td>
                        <td className="action-cell">
                          <button className="btn-icon-small" onClick={() => startEdit(variant)} title="Edit Metrics"><Edit2 size={14} /></button>
                        </td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
};

const CampaignForm = ({ onSubmit, onCancel, initial = {} }) => {
  const [data, setData] = useState({ name: '', description: '', country: '', status: 'Active', ...initial });
  const countries = ['Mexico', 'United States', 'Germany', 'Spain'];
  return (<form onSubmit={e => { e.preventDefault(); onSubmit(data); }}>
    <div className="form-group"><label>Campaign Name *</label><input type="text" value={data.name} onChange={e => setData({ ...data, name: e.target.value })} required placeholder="E.g., SaaS Outreach - Q1" /></div>
    <div className="form-group"><label>Description</label><textarea value={data.description || ''} onChange={e => setData({ ...data, description: e.target.value })} rows={2} placeholder="Brief description of this campaign..." /></div>
    <div className="form-row">
      <div className="form-group"><label>Country Strategy</label><select value={data.country || ''} onChange={e => setData({ ...data, country: e.target.value })}><option value="">Select country...</option>{countries.map(c => <option key={c} value={c}>{c}</option>)}</select></div>
      <div className="form-group"><label>Status</label><select value={data.status} onChange={e => setData({ ...data, status: e.target.value })}><option>Active</option><option>Paused</option><option>Completed</option></select></div>
    </div>
    <div className="modal-actions"><button type="button" className="btn btn-secondary" onClick={onCancel}>Cancel</button><button type="submit" className="btn btn-primary">{initial.id ? 'Save Changes' : 'Create Campaign'}</button></div>
  </form>);
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
  const [viewMode, setViewMode] = useState('list'); // 'list' or 'cards'
  const [filterStep, setFilterStep] = useState('');
  const [filterVariant, setFilterVariant] = useState('');
  const { data: campaigns } = useData('/campaigns');

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
    } else {
      fetchGroupedTemplates();
    }
  }, [search, activeTab]);

  const handleCreate = async (data) => { try { await api.post('/templates', data); addToast('Template created successfully!', 'success'); setShowCreate(false); fetchTemplates(); if (activeTab === 'data') fetchGroupedTemplates(); } catch (e) { addToast(e.message, 'error'); } };
  const handleUpdate = async (id, data) => { try { await api.put(`/templates/${id}`, data); addToast('Template updated!', 'success'); setShowEdit(null); fetchTemplates(); if (activeTab === 'data') fetchGroupedTemplates(); } catch (e) { addToast(e.message, 'error'); } };
  const handleDelete = async (id) => { if (!window.confirm('Are you sure you want to delete this template?')) return; try { await api.delete(`/templates/${id}`); addToast('Template deleted', 'success'); fetchTemplates(); if (activeTab === 'data') fetchGroupedTemplates(); } catch (e) { addToast(e.message, 'error'); } };
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
    if (filterStep && t.step_type !== filterStep) return false;
    if (filterVariant && t.variant !== filterVariant) return false;
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
            <select value={filterStep} onChange={e => setFilterStep(e.target.value)} className="filter-select">
              <option value="">All Steps</option>
              <option value="Main">Main</option>
              <option value="Step 1">Step 1</option>
              <option value="Step 2">Step 2</option>
              <option value="Step 3">Step 3</option>
              <option value="Follow-up">Follow-up</option>
            </select>
            <select value={filterVariant} onChange={e => setFilterVariant(e.target.value)} className="filter-select">
              <option value="">All Variants</option>
              <option value="A">Variant A</option>
              <option value="B">Variant B</option>
              <option value="C">Variant C</option>
              <option value="D">Variant D</option>
            </select>
            <div className="view-toggle">
              <button className={`view-toggle-btn ${viewMode === 'list' ? 'active' : ''}`} onClick={() => setViewMode('list')} title="List View"><List size={18} /></button>
              <button className={`view-toggle-btn ${viewMode === 'cards' ? 'active' : ''}`} onClick={() => setViewMode('cards')} title="Card View"><LayoutGrid size={18} /></button>
            </div>
          </div>
        </div>

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
              <div key={t.id} className={`template-list-item ${t.is_winner ? 'winner' : ''}`}>
                <div className="template-list-left">
                  <span className={`variant-badge variant-${t.variant}`}>{t.variant}</span>
                  <div className="template-list-info">
                    <div className="template-list-header">
                      <h4 className="template-list-name">{t.name}</h4>
                      {t.is_winner && <span className="winner-badge-sm"><Trophy size={12} /> Winner</span>}
                    </div>
                    <div className="template-list-meta">
                      <span className="step-tag">{t.step_type}</span>
                      {t.subject && <span className="subject-preview">Subject: {t.subject.length > 50 ? t.subject.substring(0, 50) + '...' : t.subject}</span>}
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
                <div className="template-list-actions">
                  <button className="btn-icon-small" onClick={() => copyToClipboard(t)} title="Copy"><Copy size={14} /></button>
                  <button className="btn-icon-small" onClick={() => setShowEdit(t)} title="Edit"><Edit2 size={14} /></button>
                  <button className="btn-icon-small danger" onClick={() => handleDelete(t.id)} title="Delete"><Trash2 size={14} /></button>
                </div>
              </div>
            ))}
          </div>
        ) : (
          /* Card View */
          <div className="templates-grid">
            {filteredTemplates.map(t => (
              <div key={t.id} className={`template-card-v2 ${t.is_winner ? 'winner' : ''}`}>
                <div className="template-card-top">
                  <div className="template-badges">
                    <span className={`variant-badge-lg variant-${t.variant}`}>{t.variant}</span>
                    <span className="step-badge">{t.step_type}</span>
                    {t.is_winner && <span className="winner-badge-sm"><Trophy size={12} /> Winner</span>}
                  </div>
                  <div className="template-actions-menu">
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
        )}
      </>
    ) : (
      <TemplateDataView data={groupedData} loading={loading} onEdit={setShowEdit} onDelete={handleDelete} copyToClipboard={copyToClipboard} />
    )}

    <Modal isOpen={showCreate} onClose={() => setShowCreate(false)} title="Create New Template" size="xl"><TemplateForm campaigns={campaigns?.data || []} onSubmit={handleCreate} onCancel={() => setShowCreate(false)} variables={variables} /></Modal>
    <Modal isOpen={!!showEdit} onClose={() => setShowEdit(null)} title="Edit Template" size="xl">{showEdit && <TemplateForm campaigns={campaigns?.data || []} initial={showEdit} onSubmit={(d) => handleUpdate(showEdit.id, d)} onCancel={() => setShowEdit(null)} variables={variables} />}</Modal>
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
  const [data, setData] = useState({ name: '', variant: 'A', step_type: 'Main', subject: '', body: '', campaign_ids: initialCampaignIds, ...initial, campaign_ids: initialCampaignIds });
  const insertVariable = (v, field) => { setData({ ...data, [field]: (data[field] || '') + v }); };

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
            {['A', 'B', 'C', 'D'].map(v => (
              <button key={v} type="button" className={`variant-btn ${data.variant === v ? 'active' : ''}`} onClick={() => setData({ ...data, variant: v })}>{v}</button>
            ))}
          </div>
        </div>
        <div className="form-group">
          <label>Step</label>
          <select value={data.step_type} onChange={e => setData({ ...data, step_type: e.target.value })}>
            <option>Main</option><option>Step 1</option><option>Step 2</option><option>Step 3</option><option>Follow-up</option>
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
    loadApiKeyStatus();
  }, []);

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
            </div>
            <div className="webhook-info">
              <p><strong>What it does:</strong> Automatically updates campaign metrics and contact status based on email engagement events.</p>
              <p><strong>Payload format:</strong></p>
              <pre>{`{
  "event": "opened",
  "email": "user@example.com",
  "campaign_name": "Campaign Name",
  "template_id": 123 // optional
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

// Main App
function App() {
  const [user, setUser] = useState(null);
  const [page, setPage] = useState('contacts');
  const [loading, setLoading] = useState(true);

  useEffect(() => { const check = async () => { if (api.token) { try { const u = await api.get('/auth/me'); setUser(u); } catch { api.setToken(null); } } setLoading(false); }; check(); }, []);

  const handleLogout = () => { api.setToken(null); setUser(null); };

  if (loading) return <div className="loading-screen"><Loader2 className="spin" size={32} /></div>;
  if (!user) return <ToastProvider><LoginPage onLogin={setUser} /></ToastProvider>;

  return (<ToastProvider><ImportJobProvider><div className="app"><Sidebar page={page} setPage={setPage} user={user} onLogout={handleLogout} /><main className="main-content">
    {page === 'dashboard' && <DashboardPage />}
    {page === 'contacts' && <ContactsPage />}
    {page === 'duplicates' && <DuplicatesPage />}
    {page === 'enrichment' && <EnrichmentPage />}
    {page === 'campaigns' && <CampaignsPage />}
    {page === 'templates' && <TemplatesPage />}
    {page === 'settings' && <SettingsPage />}
  </main></div></ImportJobProvider></ToastProvider>);
}

export default App;
