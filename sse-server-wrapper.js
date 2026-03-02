import 'dotenv/config';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const SSE_SERVER_PATH = './electron-client/src/backend/pure-sse-server.js';
const HEALTH_CHECK_URL = 'http://localhost:8080/health';
const RESTART_CHECK_INTERVAL = 6 * 60 * 60 * 1000; // Check every 6 hours
const MIN_RESTART_HOURS = 24;
const MAX_RESTART_HOURS = 48;

let serverProcess = null;
let nextScheduledRestart = null;
let startupTime = Date.now();

// Import cleanup service
async function runJsonCleanup() {
    console.log('[Wrapper] Running JSON cleanup...');
    try {
        const cleanup = await import('./json-cleanup-service.js');
        await cleanup.default.run();
        console.log('[Wrapper] JSON cleanup completed');
    } catch (error) {
        console.error('[Wrapper] JSON cleanup failed:', error.message);
    }
}

// Health check
async function checkHealth() {
    try {
        const response = await fetch(HEALTH_CHECK_URL, { timeout: 5000 });
        const data = await response.json();
        return data.status === 'healthy';
    } catch (error) {
        console.error('[Wrapper] Health check failed:', error.message);
        return false;
    }
}

// Schedule random restart
function scheduleNextRestart() {
    const hoursUntilRestart = MIN_RESTART_HOURS + Math.random() * (MAX_RESTART_HOURS - MIN_RESTART_HOURS);
    const msUntilRestart = hoursUntilRestart * 60 * 60 * 1000;
    
    nextScheduledRestart = Date.now() + msUntilRestart;
    
    console.log(`[Wrapper] Next scheduled restart in ${hoursUntilRestart.toFixed(1)} hours`);
    console.log(`[Wrapper] Restart scheduled for: ${new Date(nextScheduledRestart).toLocaleString()}`);
    
    return msUntilRestart;
}

// Start SSE server
async function startServer() {
    console.log('[Wrapper] Starting SSE server with 15GB memory allocation...');
    
    // Ensure JSON_CLEANUP_DAYS is set (from PM2 env or default)
    if (!process.env.JSON_CLEANUP_DAYS) {
        process.env.JSON_CLEANUP_DAYS = '7';
    }
    console.log(`[Wrapper] Cleanup threshold: ${process.env.JSON_CLEANUP_DAYS} days`);
    console.log(`[Wrapper] CORS_ORIGIN: ${process.env.CORS_ORIGIN || 'NOT SET'}`);
    console.log(`[Wrapper] PORT: ${process.env.PORT || '8080'}`);
    
    // Run cleanup before starting
    await runJsonCleanup();
    
    serverProcess = spawn('node', [
        '--max-old-space-size=15360',  // 15GB RAM
        '--expose-gc',                   // Enable garbage collection
        SSE_SERVER_PATH
    ], {
        stdio: 'inherit',
        env: { ...process.env }
    });
    
    serverProcess.on('exit', (code, signal) => {
        console.error(`[Wrapper] SSE server exited with code ${code}, signal ${signal}`);
        
        // Wait 5 seconds before restart
        setTimeout(async () => {
            console.log('[Wrapper] Attempting restart...');
            const healthy = await checkHealth();
            if (!healthy) {
                await startServer();
            }
        }, 5000);
    });
    
    serverProcess.on('error', (error) => {
        console.error('[Wrapper] Failed to start SSE server:', error);
    });
    
    startupTime = Date.now();
    scheduleNextRestart();
}

// Graceful restart
async function gracefulRestart(reason = 'scheduled') {
    console.log(`[Wrapper] Performing graceful restart (${reason})...`);
    
    if (serverProcess) {
        serverProcess.kill('SIGTERM');
        
        // Wait for graceful shutdown (max 10 seconds)
        await new Promise(resolve => {
            const timeout = setTimeout(() => {
                console.log('[Wrapper] Forcefully killing server (timeout)');
                serverProcess.kill('SIGKILL');
                resolve();
            }, 10000);
            
            serverProcess.on('exit', () => {
                clearTimeout(timeout);
                resolve();
            });
        });
    }
    
    await startServer();
}

// Periodic restart check
function startRestartChecker() {
    setInterval(async () => {
        if (Date.now() >= nextScheduledRestart) {
            await gracefulRestart('scheduled');
        }
    }, RESTART_CHECK_INTERVAL);
}

// Handle process signals
process.on('SIGTERM', () => {
    console.log('[Wrapper] Received SIGTERM, shutting down...');
    if (serverProcess) {
        serverProcess.kill('SIGTERM');
    }
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('[Wrapper] Received SIGINT, shutting down...');
    if (serverProcess) {
        serverProcess.kill('SIGTERM');
    }
    process.exit(0);
});

// Start everything
console.log('[Wrapper] SSE Server Wrapper starting...');
startServer();
startRestartChecker();
