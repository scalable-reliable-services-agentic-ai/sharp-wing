import React, { useState, useEffect, useMemo } from 'react';
import {
  ShieldAlert, ShieldCheck, CheckCircle, AlertTriangle, Activity,
  Database, Users, Loader2, MapPin, Lock, User,
  ChevronLeft, ChevronRight, ArrowDown, ArrowUp, Minus
} from 'lucide-react';

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loginError, setLoginError] = useState('');

  const [queue, setQueue] = useState([]);
  const [kpis, setKpis] = useState({ total_processed: 0, auto_denied: 0, auto_approved: 0, automation_rate: "0%" });
  const [selectedTx, setSelectedTx] = useState(null);
  const [country, setCountry] = useState("Locating...");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // --- NEW STATES FOR UI UPGRADES ---
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  // Array of { key: string, direction: 'asc' | 'desc' }
  const [sortConfigs, setSortConfigs] = useState([]);

  // 1. FETCH DATA ON LOAD
  useEffect(() => {
    if (token) fetchData();
  }, [token]);

  const fetchData = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:8000/api/queue', {
        headers: { 'Authorization': `Bearer ${token}` }
      });

      if (response.status === 401) {
        handleLogout();
        throw new Error('Session expired. Please log in again.');
      }
      if (!response.ok) throw new Error('Failed to connect to backend API');

      const data = await response.json();
      setQueue(data.queue);
      setKpis(data.kpis);

      if (data.queue.length > 0 && !selectedTx) {
        setSelectedTx(data.queue[0]);
      }
      setError(null);
    } catch (err) {
      console.error(err);
      setError(err.message || "Cannot connect to server.");
    } finally {
      setLoading(false);
    }
  };

  // 2. AUTH HANDLERS
  const handleLogin = async (e) => {
    e.preventDefault();
    setLoginError('');
    const formData = new URLSearchParams();
    formData.append('username', username);
    formData.append('password', password);

    try {
      const response = await fetch('http://localhost:8000/api/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: formData
      });
      if (!response.ok) throw new Error('Invalid username or password');
      const data = await response.json();
      localStorage.setItem('token', data.access_token);
      setToken(data.access_token);
    } catch (err) {
      setLoginError(err.message);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('token');
    setToken('');
    setQueue([]);
    setSelectedTx(null);
  };

  // 3. FETCH COUNTRY
  useEffect(() => {
    if (selectedTx && selectedTx.location) {
      setCountry("Locating...");
      const [lat, lon] = selectedTx.location.split(',').map(coord => coord.trim());
      if (lat && lon) {
        fetch(`https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${lat}&longitude=${lon}&localityLanguage=en`)
          .then(res => res.json())
          .then(data => setCountry(data.countryName || "Unknown Region"))
          .catch(() => setCountry("Unknown Region"));
      }
    }
  }, [selectedTx]);

  // 4. ACTION BUTTONS
  const handleDecision = async (id, decision) => {
    const updatedQueue = queue.filter(tx => tx.id !== id);
    setQueue(updatedQueue);
    setKpis(prev => ({ ...prev, total_processed: prev.total_processed + 1 }));

    // Select next item based on current sorted view
    const currentSortedIdx = sortedQueue.findIndex(tx => tx.id === id);
    if (updatedQueue.length > 0) {
      const nextTx = sortedQueue[currentSortedIdx + 1] || sortedQueue[currentSortedIdx - 1] || updatedQueue[0];
      setSelectedTx(nextTx);
    } else {
      setSelectedTx(null);
    }

    try {
      await fetch(`http://localhost:8000/api/resolve/${id}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ decision })
      });
    } catch (err) {
      console.error("Failed to update database:", err);
    }
  };

  // --- NEW MULTI-SORT LOGIC ---
  const handleSortToggle = (key) => {
    setSortConfigs(prev => {
      const existingIdx = prev.findIndex(s => s.key === key);
      if (existingIdx >= 0) {
        const existing = prev[existingIdx];
        if (existing.direction === 'desc') {
          // Cycle to Ascending
          const next = [...prev];
          next[existingIdx] = { ...existing, direction: 'asc' };
          return next;
        } else {
          // Cycle to Off (remove from sort array)
          return prev.filter(s => s.key !== key);
        }
      } else {
        // Add new sort (defaults to Descending first)
        return [...prev, { key, direction: 'desc' }];
      }
    });
  };

  const getSortIcon = (key) => {
    const config = sortConfigs.find(s => s.key === key);
    if (!config) return <Minus className="w-3 h-3 text-slate-600" />;
    return config.direction === 'desc' ? <ArrowDown className="w-3 h-3 text-blue-400" /> : <ArrowUp className="w-3 h-3 text-blue-400" />;
  };

  const sortedQueue = useMemo(() => {
    if (sortConfigs.length === 0) return queue; // Unsorted
    return [...queue].sort((a, b) => {
      for (let sort of sortConfigs) {
        const valA = a[sort.key];
        const valB = b[sort.key];
        if (valA < valB) return sort.direction === 'asc' ? -1 : 1;
        if (valA > valB) return sort.direction === 'asc' ? 1 : -1;
      }
      return 0;
    });
  }, [queue, sortConfigs]);


  // --- RENDERS ---
  if (!token) {
    // ... Login UI remains exactly the same
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-950 text-slate-300 p-6">
        <div className="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl p-8">
          <div className="flex flex-col items-center mb-8">
            <div className="bg-blue-500/10 p-4 rounded-full border border-blue-500/20 mb-3">
              <ShieldAlert className="text-blue-500 w-8 h-8" />
            </div>
            <h1 className="text-2xl font-bold text-white tracking-tight">System Access</h1>
            <p className="text-sm text-slate-500 mt-1">Transaction Fraud Detection System</p>
          </div>
          <form onSubmit={handleLogin} className="space-y-4">
            {loginError && (
              <div className="bg-red-950/40 border border-red-900/50 text-red-400 p-3 rounded-lg text-sm flex items-center gap-2">
                <AlertTriangle className="w-4 h-4 shrink-0" /><span>{loginError}</span>
              </div>
            )}
            <div>
              <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Username</label>
              <div className="relative">
                <User className="absolute left-3 top-3.5 w-5 h-5 text-slate-600" />
                <input type="text" value={username} onChange={(e) => setUsername(e.target.value)} className="w-full bg-slate-950 border border-slate-800 rounded-xl py-3 pl-11 pr-4 text-white focus:outline-none focus:border-blue-500 transition-colors" placeholder="admin" required />
              </div>
            </div>
            <div>
              <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Password</label>
              <div className="relative">
                <Lock className="absolute left-3 top-3.5 w-5 h-5 text-slate-600" />
                <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} className="w-full bg-slate-950 border border-slate-800 rounded-xl py-3 pl-11 pr-4 text-white focus:outline-none focus:border-blue-500 transition-colors" placeholder="••••••••" required />
              </div>
            </div>
            <button type="submit" className="w-full bg-blue-600 hover:bg-blue-500 text-white font-semibold py-3.5 rounded-xl mt-6 transition-all shadow-[0_0_20px_rgba(59,130,246,0.15)] active:scale-[0.98]">Authenticate</button>
          </form>
        </div>
      </div>
    );
  }

  if (loading && queue.length === 0) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-slate-950 text-slate-400">
        <Loader2 className="w-12 h-12 animate-spin mb-4 text-blue-500" />
        <h2 className="text-xl font-semibold text-slate-300">Retrieving Secure Logs...</h2>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-950">
        <div className="bg-red-950/50 text-red-400 p-8 rounded-xl border border-red-900/50 max-w-lg text-center">
          <AlertTriangle className="w-12 h-12 mx-auto mb-4" />
          <h2 className="text-xl font-bold mb-2 text-red-300">Session Error</h2>
          <p className="mb-4">{error}</p>
          <div className="flex gap-4 justify-center">
            <button onClick={fetchData} className="bg-blue-600 text-white px-6 py-2 rounded-lg font-semibold hover:bg-blue-500 transition">Retry Connection</button>
            <button onClick={handleLogout} className="bg-slate-800 text-slate-300 px-6 py-2 rounded-lg font-semibold hover:bg-slate-700 transition">Log Out</button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen p-8 font-sans text-slate-300 bg-slate-950 selection:bg-blue-500/30">

      {/* Header */}
      <header className="mb-8 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <ShieldAlert className="text-blue-500 w-8 h-8" />
          <h1 className="text-3xl font-bold text-white tracking-tight">Transaction Fraud Detection System</h1>
        </div>
        <button onClick={handleLogout} className="bg-slate-900 hover:bg-slate-800 text-slate-400 hover:text-white border border-slate-800 px-4 py-2 rounded-lg text-sm font-semibold transition">
          Sign Out
        </button>
      </header>

      {/* KPI Cards */}
      <div className="grid grid-cols-5 gap-4 mb-8">
        {[
          { label: "Total Processed", value: Math.max(0, kpis.total_processed - 15000), icon: Database, color: "text-blue-500" },
          { label: "Automation Rate", value: kpis.automation_rate, icon: Activity, color: "text-blue-400" },
          { label: "System 2 Approved", value: kpis.auto_approved?.toLocaleString() || "0", icon: ShieldCheck, color: "text-emerald-500" },
          { label: "System 2 Denied", value: kpis.auto_denied?.toLocaleString() || "0", icon: ShieldAlert, color: "text-red-500" },
          { label: "Pending HITL Review", value: queue.length, icon: Users, color: "text-orange-500" }
        ].map((kpi, idx) => (
          <div key={idx} className="bg-slate-900 p-5 rounded-xl border border-slate-800 shadow-xl flex items-center justify-between">
            <div>
              <p className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">{kpi.label}</p>
              <p className="text-2xl font-bold mt-1 text-white">{kpi.value}</p>
            </div>
            <kpi.icon className={`w-8 h-8 ${kpi.color} opacity-80`} />
          </div>
        ))}
      </div>

      {queue.length === 0 ? (
        <div className="bg-emerald-950/20 border border-emerald-900/50 text-emerald-400 p-12 rounded-xl text-center flex flex-col items-center">
          <CheckCircle className="w-16 h-16 mb-4 text-emerald-500" />
          <h2 className="text-3xl font-bold mb-2 text-emerald-300">Inbox Zero!</h2>
          <p className="text-lg opacity-80">All pending transactions have been resolved. Great job.</p>
        </div>
      ) : (
        <div className="flex gap-6">

          {/* Left Column: Expandable Queue */}
          <div className={`bg-slate-900 rounded-xl border border-slate-800 shadow-xl flex flex-col h-[700px] transition-all duration-300 ease-in-out ${isSidebarOpen ? 'w-1/3' : 'w-16'}`}>

            {/* Queue Header & Controls */}
            <div className="bg-slate-900 p-4 border-b border-slate-800 flex justify-between items-center whitespace-nowrap overflow-hidden">
              {isSidebarOpen ? (
                <>
                  <div className="flex items-center gap-3">
                    <h2 className="font-semibold text-slate-300">Pending Review Queue</h2>
                    <span className="bg-orange-500/20 text-orange-400 text-xs font-bold px-2.5 py-0.5 rounded-full border border-orange-500/20">
                      {queue.length}
                    </span>
                  </div>
                  <button onClick={() => setIsSidebarOpen(false)} className="p-1 hover:bg-slate-800 rounded-md text-slate-400 hover:text-white transition">
                    <ChevronLeft className="w-5 h-5" />
                  </button>
                </>
              ) : (
                <button onClick={() => setIsSidebarOpen(true)} className="p-1 hover:bg-slate-800 rounded-md text-slate-400 hover:text-white transition mx-auto">
                  <ChevronRight className="w-5 h-5" />
                </button>
              )}
            </div>

            {/* Sorting Toolbar (Only visible when expanded) */}
            {isSidebarOpen && (
              <div className="px-4 py-2 bg-slate-950/50 border-b border-slate-800 flex gap-2 overflow-x-auto custom-scrollbar">
                {[
                  { label: "Grade", key: "observer_grade" },
                  { label: "Amount", key: "amount" },
                  { label: "Conf.", key: "confidence" }
                ].map(sortCol => (
                  <button
                    key={sortCol.key}
                    onClick={() => handleSortToggle(sortCol.key)}
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold border transition-all ${
                      sortConfigs.some(s => s.key === sortCol.key) 
                      ? 'bg-blue-900/30 border-blue-500/50 text-blue-300' 
                      : 'bg-slate-900 border-slate-700 text-slate-400 hover:border-slate-600 hover:bg-slate-800'
                    }`}
                  >
                    {sortCol.label}
                    {getSortIcon(sortCol.key)}
                  </button>
                ))}
              </div>
            )}

            {/* Queue List */}
            {isSidebarOpen ? (
              <div className="overflow-y-auto p-3 flex-grow space-y-2 custom-scrollbar">
                {sortedQueue.map((tx) => (
                  <button
                    key={tx.id}
                    onClick={() => setSelectedTx(tx)}
                    className={`w-full text-left p-4 rounded-xl border transition-all duration-200 ${
                      selectedTx?.id === tx.id 
                      ? 'bg-blue-900/20 border-blue-500/50 shadow-[0_0_15px_rgba(59,130,246,0.1)]' 
                      : 'bg-slate-950 border-slate-800 hover:border-slate-700'
                    }`}
                  >
                    <div className="flex justify-between font-semibold mb-2">
                      <span className="truncate w-2/3 text-slate-200">{tx.id}</span>
                      <span className="text-white">${tx.amount.toLocaleString()}</span>
                    </div>
                    <div className="text-sm text-slate-500 flex justify-between items-center">
                      <span>Conf: <span className="text-slate-400">{tx.confidence.toFixed(2)}</span></span>
                      <span className={`font-medium px-2 py-0.5 rounded ${tx.observer_grade <= 3 ? "bg-red-950/50 text-red-400" : "bg-emerald-950/50 text-emerald-400"}`}>
                        Grade: {tx.observer_grade}/5
                      </span>
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <div className="flex-grow flex items-center justify-center p-2">
                <div className="h-full w-full rounded border border-dashed border-slate-800 flex items-center justify-center opacity-30">
                  <Users className="w-6 h-6 text-slate-500" />
                </div>
              </div>
            )}
          </div>

          {/* Right Column: Detailed Review (Now expands when left sidebar shrinks) */}
          <div className={`bg-slate-900 rounded-xl border border-slate-800 shadow-xl p-6 flex flex-col h-[700px] transition-all duration-300 ease-in-out ${isSidebarOpen ? 'w-2/3' : 'flex-1'}`}>
            {selectedTx ? (
              <>
                <div className="flex justify-between items-start mb-6 pb-6 border-b border-slate-800">
                  <div>
                    <h2 className="text-2xl font-bold text-white mb-3 tracking-tight">TX: {selectedTx.id}</h2>
                    <div className="flex items-center gap-4 text-sm text-slate-400">
                      <span className="flex items-center gap-1 bg-slate-950 px-3 py-1.5 rounded-lg border border-slate-800">
                        <strong className="text-slate-300">Amount:</strong> <span className="text-white">${selectedTx.amount.toLocaleString()}</span>
                      </span>
                      <span className="flex items-center gap-1 bg-slate-950 px-3 py-1.5 rounded-lg border border-slate-800">
                        <MapPin className="w-4 h-4 text-blue-400" />
                        <strong className="text-slate-300">Location:</strong>
                        <span className="text-white">{country}</span>
                      </span>
                    </div>
                  </div>
                  <div className="bg-slate-950 px-5 py-3 rounded-xl border border-slate-800 text-center shadow-inner">
                    <p className="text-[10px] uppercase text-slate-500 font-bold tracking-widest mb-1">AI Confidence</p>
                    <p className="text-3xl font-black text-white">{selectedTx.confidence.toFixed(2)}</p>
                  </div>
                </div>

                <div className="space-y-4 flex-grow overflow-y-auto pr-2 custom-scrollbar">
                  <div className="bg-blue-950/20 border border-blue-900/50 rounded-xl p-5">
                    <h3 className="font-semibold text-blue-400 flex items-center gap-2 mb-3 pb-2 border-b border-blue-900/50">
                      <Activity className="w-5 h-5" /> Primary Agent Reasoning
                    </h3>
                    <p className="text-slate-300 leading-relaxed text-sm">{selectedTx.ai_reasoning}</p>
                  </div>
                  <div className={`rounded-xl p-5 border ${selectedTx.observer_grade <= 3 ? 'bg-red-950/20 border-red-900/50' : 'bg-emerald-950/20 border-emerald-900/50'}`}>
                    <h3 className={`font-semibold flex items-center gap-2 mb-3 pb-2 border-b ${selectedTx.observer_grade <= 3 ? 'text-red-400 border-red-900/50' : 'text-emerald-400 border-emerald-900/50'}`}>
                      <AlertTriangle className="w-5 h-5" /> Observer Audit (Grade {selectedTx.observer_grade}/5)
                    </h3>
                    <p className={`leading-relaxed text-sm ${selectedTx.observer_grade <= 3 ? 'text-red-200/80' : 'text-emerald-200/80'}`}>
                      {selectedTx.observer_critique}
                    </p>
                  </div>
                </div>

                <div className="flex gap-4 pt-6 mt-4 border-t border-slate-800">
                  <button onClick={() => handleDecision(selectedTx.id, 'Safe')} className="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white py-4 rounded-xl font-bold flex justify-center items-center gap-2 transition-all hover:shadow-[0_0_20px_rgba(16,185,129,0.3)] active:scale-[0.98]">
                    <CheckCircle className="w-5 h-5" /> Mark Safe (Approve)
                  </button>
                  <button onClick={() => handleDecision(selectedTx.id, 'Fraud')} className="flex-1 bg-red-600 hover:bg-red-500 text-white py-4 rounded-xl font-bold flex justify-center items-center gap-2 transition-all hover:shadow-[0_0_20px_rgba(239,68,68,0.3)] active:scale-[0.98]">
                    <ShieldAlert className="w-5 h-5" /> Confirm Fraud (Deny)
                  </button>
                </div>
              </>
            ) : (
              <div className="flex-grow flex items-center justify-center text-slate-500">
                Select a transaction from the queue to review details.
              </div>
            )}
          </div>

        </div>
      )}
    </div>
  );
}