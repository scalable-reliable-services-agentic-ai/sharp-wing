import React, { useState, useEffect } from 'react';
import { ShieldAlert, CheckCircle, AlertTriangle, Activity, Database, Users, Loader2, MapPin } from 'lucide-react';

export default function App() {
  const [queue, setQueue] = useState([]);
  const [kpis, setKpis] = useState({ total_processed: 0, auto_denied: 0, automation_rate: "0%" });
  const [selectedTx, setSelectedTx] = useState(null);
  const [country, setCountry] = useState("Locating..."); // New state for Country name

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // 1. FETCH DATA ON LOAD
  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/queue');
      if (!response.ok) throw new Error('Failed to connect to backend API');

      const data = await response.json();
      setQueue(data.queue);
      setKpis(data.kpis);

      if (data.queue.length > 0) {
        setSelectedTx(data.queue[0]);
      }
      setLoading(false);
    } catch (err) {
      console.error(err);
      setError("Cannot connect to Python server. Make sure FastAPI is running on port 8000.");
      setLoading(false);
    }
  };

  // 2. FETCH COUNTRY NAME WHEN TRANSACTION IS SELECTED
  useEffect(() => {
    if (selectedTx && selectedTx.location) {
      setCountry("Locating...");
      // Split the location string "lat, lon" into two variables
      const [lat, lon] = selectedTx.location.split(',').map(coord => coord.trim());

      if (lat && lon) {
        // Free client-side API to convert GPS to Country
        fetch(`https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${lat}&longitude=${lon}&localityLanguage=en`)
          .then(res => res.json())
          .then(data => {
            setCountry(data.countryName || "Unknown Region");
          })
          .catch(() => setCountry("Unknown Region"));
      }
    }
  }, [selectedTx]); // This runs every time 'selectedTx' changes!

  // 3. HANDLE BUTTON CLICKS
  const handleDecision = async (id, decision) => {
    const updatedQueue = queue.filter(tx => tx.id !== id);
    setQueue(updatedQueue);
    setKpis(prev => ({ ...prev, total_processed: prev.total_processed + 1 }));

    if (updatedQueue.length > 0) {
      setSelectedTx(updatedQueue[0]);
    } else {
      setSelectedTx(null);
    }

    try {
      await fetch(`http://localhost:8000/api/resolve/${id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ decision })
      });
    } catch (err) {
      console.error("Failed to update database:", err);
    }
  };

  // LOADING STATE UI
  if (loading) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-slate-950 text-slate-400">
        <Loader2 className="w-12 h-12 animate-spin mb-4 text-blue-500" />
        <h2 className="text-xl font-semibold text-slate-300">Connecting to Data Pipeline...</h2>
      </div>
    );
  }

  // ERROR STATE UI
  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-950">
        <div className="bg-red-950/50 text-red-400 p-8 rounded-xl border border-red-900/50 max-w-lg text-center">
          <AlertTriangle className="w-12 h-12 mx-auto mb-4" />
          <h2 className="text-xl font-bold mb-2 text-red-300">Connection Error</h2>
          <p>{error}</p>
          <button onClick={fetchData} className="mt-6 bg-red-600 text-white px-6 py-2 rounded-lg font-semibold hover:bg-red-700 transition">Try Again</button>
        </div>
      </div>
    );
  }

  // MAIN DASHBOARD UI
  return (
    <div className="min-h-screen p-8 font-sans text-slate-300 bg-slate-950 selection:bg-blue-500/30">

      {/* Header */}
      <header className="mb-8 flex items-center gap-3">
        <ShieldAlert className="text-blue-500 w-8 h-8" />
        <h1 className="text-3xl font-bold text-white tracking-tight">Transaction Fraud Detection System</h1>
      </header>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-6 mb-8">
        {[
          { label: "Total Processed", value: kpis.total_processed.toLocaleString(), icon: Database, color: "text-blue-500" },
          { label: "Automation Rate", value: kpis.automation_rate, icon: Activity, color: "text-emerald-500" },
          { label: "System 2 Auto-Denied", value: kpis.auto_denied.toLocaleString(), icon: ShieldAlert, color: "text-red-500" },
          { label: "Pending HITL Review", value: queue.length, icon: Users, color: "text-orange-500" }
        ].map((kpi, idx) => (
          <div key={idx} className="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-xl flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-slate-500 uppercase tracking-wider">{kpi.label}</p>
              <p className="text-3xl font-bold mt-1 text-white">{kpi.value}</p>
            </div>
            <kpi.icon className={`w-8 h-8 ${kpi.color} opacity-80`} />
          </div>
        ))}
      </div>

      {/* Main Content Area */}
      {queue.length === 0 ? (
        <div className="bg-emerald-950/20 border border-emerald-900/50 text-emerald-400 p-12 rounded-xl text-center flex flex-col items-center">
          <CheckCircle className="w-16 h-16 mb-4 text-emerald-500" />
          <h2 className="text-3xl font-bold mb-2 text-emerald-300">Inbox Zero!</h2>
          <p className="text-lg opacity-80">All pending transactions have been resolved. Great job.</p>
        </div>
      ) : (
        <div className="flex gap-6">

          {/* Left Column: Queue List */}
          <div className="w-1/3 bg-slate-900 rounded-xl border border-slate-800 shadow-xl overflow-hidden flex flex-col h-[700px]">
            <div className="bg-slate-900 p-5 border-b border-slate-800 flex justify-between items-center">
              <h2 className="font-semibold text-slate-300">Pending Review Queue</h2>
              <span className="bg-orange-500/20 text-orange-400 text-xs font-bold px-3 py-1 rounded-full border border-orange-500/20">
                {queue.length} items
              </span>
            </div>
            <div className="overflow-y-auto p-3 flex-grow space-y-2">
              {queue.map((tx) => (
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
          </div>

          {/* Right Column: Detailed Review */}
          <div className="w-2/3 bg-slate-900 rounded-xl border border-slate-800 shadow-xl p-6 flex flex-col h-[700px]">
            {selectedTx && (
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
                        <span className="text-slate-500 text-xs ml-1">({selectedTx.location})</span>
                      </span>
                    </div>
                  </div>
                  <div className="bg-slate-950 px-5 py-3 rounded-xl border border-slate-800 text-center shadow-inner">
                    <p className="text-[10px] uppercase text-slate-500 font-bold tracking-widest mb-1">AI Confidence</p>
                    <p className="text-3xl font-black text-white">{selectedTx.confidence.toFixed(2)}</p>
                  </div>
                </div>

                <div className="space-y-4 flex-grow overflow-y-auto pr-2 custom-scrollbar">
                  {/* Primary Agent Reasoning */}
                  <div className="bg-blue-950/20 border border-blue-900/50 rounded-xl p-5">
                    <h3 className="font-semibold text-blue-400 flex items-center gap-2 mb-3 pb-2 border-b border-blue-900/50">
                      <Activity className="w-5 h-5" /> Primary Agent Reasoning
                    </h3>
                    <p className="text-slate-300 leading-relaxed text-sm">{selectedTx.ai_reasoning}</p>
                  </div>

                  {/* Observer Agent Evaluation */}
                  <div className={`rounded-xl p-5 border ${selectedTx.observer_grade <= 3 ? 'bg-red-950/20 border-red-900/50' : 'bg-emerald-950/20 border-emerald-900/50'}`}>
                    <h3 className={`font-semibold flex items-center gap-2 mb-3 pb-2 border-b ${selectedTx.observer_grade <= 3 ? 'text-red-400 border-red-900/50' : 'text-emerald-400 border-emerald-900/50'}`}>
                      <AlertTriangle className="w-5 h-5" /> Observer Audit (Grade {selectedTx.observer_grade}/5)
                    </h3>
                    <p className={`leading-relaxed text-sm ${selectedTx.observer_grade <= 3 ? 'text-red-200/80' : 'text-emerald-200/80'}`}>
                      {selectedTx.observer_critique}
                    </p>
                  </div>
                </div>

                {/* Action Buttons */}
                <div className="flex gap-4 pt-6 mt-4">
                  <button
                    onClick={() => handleDecision(selectedTx.id, 'Safe')}
                    className="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white py-4 rounded-xl font-bold flex justify-center items-center gap-2 transition-all hover:shadow-[0_0_20px_rgba(16,185,129,0.3)] active:scale-[0.98]"
                  >
                    <CheckCircle className="w-5 h-5" /> Mark Safe (Approve)
                  </button>
                  <button
                    onClick={() => handleDecision(selectedTx.id, 'Fraud')}
                    className="flex-1 bg-red-600 hover:bg-red-500 text-white py-4 rounded-xl font-bold flex justify-center items-center gap-2 transition-all hover:shadow-[0_0_20px_rgba(239,68,68,0.3)] active:scale-[0.98]"
                  >
                    <ShieldAlert className="w-5 h-5" /> Confirm Fraud (Deny)
                  </button>
                </div>
              </>
            )}
          </div>

        </div>
      )}
    </div>
  );
}