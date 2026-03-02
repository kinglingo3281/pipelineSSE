#!/bin/bash
# Start ngrok tunnel + SSE Server (Linux equivalent of start-sse-server.bat)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting ngrok tunnel and SSE Server with Wrapper..."

# Start ngrok in background
echo "Starting ngrok tunnel..."
ngrok http 8080 --domain=your-domain.ngrok.app &
NGROK_PID=$!
echo "ngrok started (PID: $NGROK_PID)"

# Wait for ngrok to initialize
sleep 3

# Start SSE Server via Wrapper (includes cleanup + crash recovery)
echo "Starting SSE Server Wrapper with auto-restart and cleanup..."
export JSON_CLEANUP_DAYS=1
echo "Cleanup threshold set to $JSON_CLEANUP_DAYS days"

cd "$SCRIPT_DIR"
node sse-server-wrapper.js
