import React, { useState, useEffect } from 'react';
import { ShieldAlert, CheckCircle, AlertTriangle, Activity, Database, Users, Loader2 } from 'lucide-react';

export default function App() {
  // State for our live data
  const [queue, setQueue] = useState([]);
  const [kpis, setKpis] = useState({ total_processed: 0, auto_denied: 0, automation_rate: "0%" });
  const [selectedTx, setSelectedTx] = useState(null);

  // State for UI experience
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // FETCH DATA ON LOAD
  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/queue');
      if (!response.ok) throw new Error('Failed to connect to the backend API');

      const data = await response.json();
      setQueue(data.queue);
      setKpis(data.kpis);

      // Auto-select the first item in the queue if it exists
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

  // HANDLE BUTTON CLICKS
  const handleDecision = async (id, decision) => {
    // 1. Optimistic UI Update (Makes the app feel instantly fast to the human)
    const updatedQueue = queue.filter(tx => tx.id !== id);
    setQueue(updatedQueue);
    setKpis(prev => ({ ...prev, total_processed: prev.total_processed + 1 }));

    if (updatedQueue.length > 0) {
      setSelectedTx(updatedQueue[0]);
    } else {
      setSelectedTx(null);
    }

    // 2. Send the actual decision to the Python API
    try {
      await fetch(`http://localhost:8000/api/resolve/${id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ decision })
      });
    } catch (err) {
      console.error("Failed to update database:", err);
      // In a real app, you might show a toast error and revert the UI change here
    }
  };

  // LOADING STATE UI
  if (loading) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-slate-50 text-slate-500">
        <Loader2 className="w-12 h-12 animate-spin mb-4 text-blue-500" />
        <h2 className="text-xl font-semibold">Connecting to Data Pipeline...</h2>
      </div>
    );
  }

  // ERROR STATE UI
  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <div className="bg-red-50 text-red-700 p-8 rounded-xl border border-red-200 max-w-lg text-center">
          <AlertTriangle className="w-12 h-12 mx-auto mb-4" />
          <h2 className="text-xl font-bold mb-2">Connection Error</h2>
          <p>{error}</p>
          <button onClick={fetchData} className="mt-6 bg-red-600 text-white px-6 py-2 rounded-lg font-semibold hover:bg-red-700 transition">Try Again</button>
        </div>
      </div>
    );
  }

  // MAIN DASHBOARD UI (Exactly the same beautiful UI as before!)
  return (
    <div className="min-h-screen p-8 font-sans text-slate-800 bg-slate-50">

      {/* Header */}
      <header className="mb-8 flex items-center gap-3">
        <ShieldAlert className="text-blue-600 w-8 h-8" />
        <h1 className="text-3xl font-bold text-slate-900">Fraud Triage Operations</h1>
      </header>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-6 mb-8">
        {[
          { label: "Total Processed", value: kpis.total_processed.toLocaleString(), icon: Database, color: "text-blue-600" },
          { label: "Automation Rate", value: kpis.automation_rate, icon: Activity, color: "text-green-600" },
          { label: "System 2 Auto-Denied", value: kpis.auto_denied.toLocaleString(), icon: ShieldAlert, color: "text-red-600" },
          { label: "Pending HITL Review", value: queue.length, icon: Users, color: "text-orange-500" }
        ].map((kpi, idx) => (
          <div key={idx} className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-slate-500">{kpi.label}</p>
              <p className="text-2xl font-bold mt-1">{kpi.value}</p>
            </div>
            <kpi.icon className={`w-8 h-8 ${kpi.color} opacity-80`} />
          </div>
        ))}
      </div>

      {/* Main Content Area */}
      {queue.length === 0 ? (
        <div className="bg-emerald-50 border border-emerald-200 text-emerald-800 p-12 rounded-xl text-center flex flex-col items-center">
          <CheckCircle className="w-16 h-16 mb-4 text-emerald-500" />
          <h2 className="text-3xl font-bold mb-2">Inbox Zero!</h2>
          <p className="text-lg opacity-80">All pending transactions have been resolved. Great job.</p>
        </div>
      ) : (
        <div className="flex gap-6">

          {/* Left Column: Queue List */}
          <div className="w-1/3 bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden flex flex-col h-[700px]">
            <div className="bg-slate-50 p-4 border-b border-slate-200 flex justify-between items-center">
              <h2 className="font-semibold text-slate-700">Pending Review Queue</h2>
              <span className="bg-orange-100 text-orange-700 text-xs font-bold px-2 py-1 rounded-full">{queue.length} items</span>
            </div>
            <div className="overflow-y-auto p-2 flex-grow">
              {queue.map((tx) => (
                <button
                  key={tx.id}
                  onClick={() => setSelectedTx(tx)}
                  className={`w-full text-left p-4 mb-2 rounded-lg border transition-all ${
                    selectedTx?.id === tx.id 
                    ? 'bg-blue-50 border-blue-200 shadow-sm' 
                    : 'bg-white border-slate-100 hover:border-slate-300'
                  }`}
                >
                  <div className="flex justify-between font-semibold mb-1">
                    <span className="truncate w-2/3">{tx.id}</span>
                    <span>${tx.amount.toLocaleString()}</span>
                  </div>
                  <div className="text-sm text-slate-500 flex justify-between">
                    <span>Conf: {tx.confidence.toFixed(2)}</span>
                    <span className={tx.observer_grade <= 3 ? "text-red-500 font-medium" : "text-emerald-600 font-medium"}>
                      Grade: {tx.observer_grade}/5
                    </span>
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Right Column: Detailed Review */}
          <div className="w-2/3 bg-white rounded-xl border border-slate-200 shadow-sm p-6 flex flex-col h-[700px]">
            {selectedTx && (
              <>
                <div className="flex justify-between items-start mb-6">
                  <div>
                    <h2 className="text-2xl font-bold text-slate-900 mb-2 truncate">TX: {selectedTx.id}</h2>
                    <p className="text-slate-600"><strong>Amount:</strong> ${selectedTx.amount.toLocaleString()} &nbsp;|&nbsp; <strong>Location:</strong> {selectedTx.location}</p>
                  </div>
                  <div className="bg-slate-100 px-4 py-2 rounded-lg text-center shadow-inner">
                    <p className="text-xs uppercase text-slate-500 font-bold tracking-wider">AI Confidence</p>
                    <p className="text-2xl font-black text-slate-800">{selectedTx.confidence.toFixed(2)}</p>
                  </div>
                </div>

                <div className="space-y-4 mb-6 flex-grow overflow-y-auto">
                  {/* Primary Agent Reasoning */}
                  <div className="bg-blue-50 border border-blue-100 rounded-xl p-5">
                    <h3 className="font-bold text-blue-900 flex items-center gap-2 mb-3 border-b border-blue-200 pb-2">
                      <Activity className="w-5 h-5" /> Primary Agent Reasoning
                    </h3>
                    <p className="text-blue-800 leading-relaxed">{selectedTx.ai_reasoning}</p>
                  </div>

                  {/* Observer Agent Evaluation */}
                  <div className={`border rounded-xl p-5 ${selectedTx.observer_grade <= 3 ? 'bg-red-50 border-red-200' : 'bg-emerald-50 border-emerald-200'}`}>
                    <h3 className={`font-bold flex items-center gap-2 mb-3 border-b pb-2 ${selectedTx.observer_grade <= 3 ? 'text-red-900 border-red-200' : 'text-emerald-900 border-emerald-200'}`}>
                      <AlertTriangle className="w-5 h-5" /> Observer Audit (Grade {selectedTx.observer_grade}/5)
                    </h3>
                    <p className={`${selectedTx.observer_grade <= 3 ? 'text-red-800' : 'text-emerald-800'} leading-relaxed`}>
                      {selectedTx.observer_critique}
                    </p>
                  </div>
                </div>

                {/* Action Buttons */}
                <div className="flex gap-4 pt-4 border-t border-slate-100 mt-auto">
                  <button
                    onClick={() => handleDecision(selectedTx.id, 'Safe')}
                    className="flex-1 bg-emerald-600 hover:bg-emerald-700 text-white py-4 rounded-xl font-bold flex justify-center items-center gap-2 transition-all hover:shadow-lg active:scale-[0.98]"
                  >
                    <CheckCircle className="w-6 h-6" /> Mark Safe (Approve)
                  </button>
                  <button
                    onClick={() => handleDecision(selectedTx.id, 'Fraud')}
                    className="flex-1 bg-red-600 hover:bg-red-700 text-white py-4 rounded-xl font-bold flex justify-center items-center gap-2 transition-all hover:shadow-lg active:scale-[0.98]"
                  >
                    <ShieldAlert className="w-6 h-6" /> Confirm Fraud (Deny)
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