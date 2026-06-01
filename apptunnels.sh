#!/bin/bash

echo "🚀 Initializing network tunnels to the Kubernetes cluster..."

# Step 1: Clean up potentially hanging tunnels from previous sessions
killall kubectl 2>/dev/null

# Step 2: Open new tunnels in the background and save their Process IDs (PIDs)
echo "🔌 Opening port 8000 for Dashboard API..."           # local-port:port-on-K8s
kubectl port-forward svc/transaction-fraud-detection-dashboard-api 8000:8000 > /dev/null 2>&1 &
PID_API=$!

echo "🔌 Opening port 30300 for Grafana..."
kubectl port-forward svc/transaction-fraud-detection-grafana 30300:80 > /dev/null 2>&1 &
PID_GRAFANA=$!

echo "🔌 Opening port 30090 for Prometheus..."
kubectl port-forward svc/transaction-fraud-detection-prometheus 30090:9090 > /dev/null 2>&1 &
PID_PROM=$!

echo ""
echo "✅ Tunnels are active!"
echo "🌐 API:         http://127.0.0.1:8000"
echo "📊 Grafana:     http://127.0.0.1:30300"
echo "📈 Prometheus:  http://127.0.0.1:30090"
echo ""
echo "🛑 Press [Ctrl+C] to close tunnels and exit."

# Step 3: Trap to catch the Ctrl+C signal and gracefully kill background processes
trap "echo -e '\nClosing tunnels...'; kill $PID_API $PID_GRAFANA $PID_PROM 2>/dev/null; echo 'Done!'; exit" INT

# The script waits indefinitely until the user presses Ctrl+C
wait