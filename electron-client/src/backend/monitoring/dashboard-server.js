// Admin Dashboard Server - Serves monitoring UI
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import MetricsCollector from './metrics-collector.js';
import MemoryWatchdog from './memory-watchdog.js';
import logger from './logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class DashboardServer {
    constructor() {
        this.router = express.Router();
        this.setupRoutes();
    }

    setupRoutes() {
        // Serve dashboard HTML
        this.router.get('/admin/dashboard', (req, res) => {
            res.sendFile(path.join(__dirname, '..', 'admin', 'dashboard.html'));
        });

        // Metrics snapshot endpoint
        this.router.get('/admin/metrics', (req, res) => {
            const snapshot = MetricsCollector.getSnapshot();
            const watchdogStatus = MemoryWatchdog.getStatus();
            
            res.json({
                ...snapshot,
                watchdog: watchdogStatus
            });
        });

        // SSE stream for real-time updates
        this.router.get('/admin/stream', (req, res) => {
            res.writeHead(200, {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            });

            // Send initial data
            const sendUpdate = () => {
                const snapshot = MetricsCollector.getSnapshot();
                const watchdogStatus = MemoryWatchdog.getStatus();
                
                res.write(`data: ${JSON.stringify({
                    ...snapshot,
                    watchdog: watchdogStatus
                })}\n\n`);
            };

            // Send updates every 5 seconds
            sendUpdate();
            const intervalId = setInterval(sendUpdate, 5000);

            // Cleanup on disconnect
            req.on('close', () => {
                clearInterval(intervalId);
            });
        });

        // Connection details
        this.router.get('/admin/connections', (req, res) => {
            const sessions = Array.from(MetricsCollector.connections.sessions.entries())
                .map(([clientId, session]) => ({
                    clientId,
                    ip: session.ip,
                    duration: Math.round((Date.now() - session.startTime) / 1000),
                    bytesSent: Math.round(session.bytesSent / 1024)
                }))
                .sort((a, b) => b.duration - a.duration);

            res.json({
                active: MetricsCollector.connections.active,
                sessions: sessions.slice(0, 100) // Top 100
            });
        });

        // Recent errors
        this.router.get('/admin/errors', (req, res) => {
            res.json({
                last1min: MetricsCollector.errors.last1min,
                last5min: MetricsCollector.errors.last5min,
                last15min: MetricsCollector.errors.last15min,
                byType: Object.fromEntries(MetricsCollector.errors.byType)
            });
        });

        // Broadcast history
        this.router.get('/admin/broadcasts', (req, res) => {
            res.json({
                count: MetricsCollector.broadcasts.count,
                lastTimestamp: MetricsCollector.broadcasts.lastTimestamp,
                recentDurations: MetricsCollector.broadcasts.durations.slice(-20),
                totalDataSent: MetricsCollector.broadcasts.totalDataSent
            });
        });

        // Health check
        this.router.get('/admin/health', (req, res) => {
            const snapshot = MetricsCollector.getSnapshot();
            const watchdog = MemoryWatchdog.getStatus();
            
            const isHealthy = 
                parseFloat(watchdog.percentUsed) < watchdog.criticalThreshold &&
                !watchdog.restartScheduled &&
                snapshot.errors.last1min < 5;

            res.json({
                healthy: isHealthy,
                status: isHealthy ? 'ok' : 'degraded',
                ...snapshot,
                watchdog
            });
        });

        logger.info('Dashboard routes registered');
    }

    getRouter() {
        return this.router;
    }
}

export default new DashboardServer();
