// Metrics Collection System
import logger from './logger.js';
import TelegramAlerter from './telegram-alerter.js';

class MetricsCollector {
    constructor() {
        this.startTime = Date.now();
        this.restartCount = 0;
        
        // Connection tracking
        this.connections = {
            active: 0,
            peak: 0,
            total: 0,
            disconnects: 0,
            byIP: new Map(),
            sessions: new Map() // clientId -> { startTime, ip, bytessentMetrics }
        };
        
        // Broadcast metrics
        this.broadcasts = {
            count: 0,
            durations: [],
            lastTimestamp: null,
            lastDuration: 0,
            totalDataSent: 0
        };
        
        // Error tracking
        this.errors = {
            last1min: [],
            last5min: [],
            last15min: [],
            total: 0,
            byType: new Map()
        };
        
        // Memory snapshots
        this.memory = {
            samples: [],
            peak: 0
        };
        
        // Alert thresholds
        this.thresholds = {
            memoryWarning: parseInt(process.env.MEMORY_WARNING_PERCENT || '80'),
            memoryCritical: parseInt(process.env.MEMORY_CRITICAL_PERCENT || '90'),
            connectionSpike: parseInt(process.env.CONNECTION_SPIKE_THRESHOLD || '800'),
            errorRate: parseInt(process.env.ERROR_RATE_THRESHOLD || '10'),
            broadcastLag: parseInt(process.env.BROADCAST_LAG_THRESHOLD || '25000')
        };
        
        // Start periodic metric snapshots
        this.startSnapshots();
        
        // Start summary reports (every 12 hours)
        this.startSummaryReports();
    }

    // Connection tracking
    recordConnection(clientId, ip) {
        this.connections.active++;
        this.connections.total++;
        this.connections.peak = Math.max(this.connections.peak, this.connections.active);
        
        this.connections.sessions.set(clientId, {
            startTime: Date.now(),
            ip,
            bytesSent: 0
        });
        
        // Track unique IPs
        const ipCount = this.connections.byIP.get(ip) || 0;
        this.connections.byIP.set(ip, ipCount + 1);
        
        // Alert on connection spike
        if (this.connections.active > this.thresholds.connectionSpike) {
            TelegramAlerter.alertConnectionSpike(this.connections.active);
        }
        
        logger.debug(`Connection recorded: ${clientId} from ${ip}`);
    }

    recordDisconnection(clientId) {
        const session = this.connections.sessions.get(clientId);
        if (session) {
            const duration = Date.now() - session.startTime;
            logger.debug(`Disconnection: ${clientId}, duration: ${Math.round(duration / 1000)}s`);
            
            // Cleanup IP tracking
            const ipCount = this.connections.byIP.get(session.ip);
            if (ipCount && ipCount > 1) {
                this.connections.byIP.set(session.ip, ipCount - 1);
            } else {
                this.connections.byIP.delete(session.ip);
            }
            
            this.connections.sessions.delete(clientId);
        }
        
        this.connections.active--;
        this.connections.disconnects++;
    }

    // Broadcast tracking
    recordBroadcast(duration, clientCount, dataSize) {
        this.broadcasts.count++;
        this.broadcasts.lastTimestamp = Date.now();
        this.broadcasts.lastDuration = duration;
        this.broadcasts.totalDataSent += dataSize;
        
        // Keep last 100 durations for averaging
        this.broadcasts.durations.push(duration);
        if (this.broadcasts.durations.length > 100) {
            this.broadcasts.durations.shift();
        }
        
        // Track data sent per session
        this.connections.sessions.forEach((session) => {
            session.bytesSent += Math.round(dataSize / clientCount);
        });
        
        // Alert on lag
        if (duration > this.thresholds.broadcastLag) {
            TelegramAlerter.alertBroadcastLag(duration);
        }
        
        logger.debug(`Broadcast recorded: ${duration}ms, ${clientCount} clients, ${Math.round(dataSize / 1024)}KB`);
    }

    // Error tracking
    recordError(error, type = 'general') {
        const now = Date.now();
        const errorEntry = { timestamp: now, error: error.message || error, type };
        
        this.errors.last1min.push(errorEntry);
        this.errors.last5min.push(errorEntry);
        this.errors.last15min.push(errorEntry);
        this.errors.total++;
        
        // Track by type
        const typeCount = this.errors.byType.get(type) || 0;
        this.errors.byType.set(type, typeCount + 1);
        
        // Clean old errors
        this.errors.last1min = this.errors.last1min.filter(e => now - e.timestamp < 60000);
        this.errors.last5min = this.errors.last5min.filter(e => now - e.timestamp < 300000);
        this.errors.last15min = this.errors.last15min.filter(e => now - e.timestamp < 900000);
        
        // Alert on high error rate
        if (this.errors.last1min.length > this.thresholds.errorRate) {
            TelegramAlerter.alertHighErrorRate(
                this.errors.last1min.length,
                this.errors.total
            );
        }
        
        logger.error(`Error recorded: ${type}`, { error: error.message || error });
    }

    // Memory tracking
    recordMemory(usage) {
        this.memory.samples.push({
            timestamp: Date.now(),
            ...usage
        });
        
        // Keep last hour of samples (120 samples at 30s intervals)
        if (this.memory.samples.length > 120) {
            this.memory.samples.shift();
        }
        
        this.memory.peak = Math.max(this.memory.peak, usage.heapUsed);
    }

    // Restart tracking
    recordRestart() {
        this.restartCount++;
        TelegramAlerter.alertCrash('Memory threshold exceeded or manual restart');
    }

    // Get current snapshot
    getSnapshot() {
        const memUsage = process.memoryUsage();
        const uptime = Date.now() - this.startTime;
        
        return {
            timestamp: new Date().toISOString(),
            uptime: Math.round(uptime / 1000),
            memory: {
                heapUsed: memUsage.heapUsed,
                heapTotal: memUsage.heapTotal,
                rss: memUsage.rss,
                external: memUsage.external,
                percentUsed: ((memUsage.heapUsed / memUsage.heapTotal) * 100).toFixed(1)
            },
            connections: {
                active: this.connections.active,
                peak: this.connections.peak,
                total: this.connections.total,
                disconnects: this.connections.disconnects,
                uniqueIPs: this.connections.byIP.size
            },
            broadcasts: {
                count: this.broadcasts.count,
                lastDuration: this.broadcasts.lastDuration,
                avgDuration: this.broadcasts.durations.length > 0 
                    ? Math.round(this.broadcasts.durations.reduce((a, b) => a + b, 0) / this.broadcasts.durations.length)
                    : 0,
                lastTimestamp: this.broadcasts.lastTimestamp,
                totalDataSent: this.broadcasts.totalDataSent
            },
            errors: {
                last1min: this.errors.last1min.length,
                last5min: this.errors.last5min.length,
                last15min: this.errors.last15min.length,
                total: this.errors.total
            },
            restarts: this.restartCount
        };
    }

    // Periodic snapshots (every 30s)
    startSnapshots() {
        setInterval(() => {
            const snapshot = this.getSnapshot();
            this.recordMemory(process.memoryUsage());
            
            // Log metrics
            logger.debug('Metrics snapshot', snapshot);
            
            // Check for stuck broadcasts
            if (this.broadcasts.lastTimestamp) {
                const timeSinceLastBroadcast = Date.now() - this.broadcasts.lastTimestamp;
                if (timeSinceLastBroadcast > 120000) { // 2 minutes
                    TelegramAlerter.alertBroadcastStuck(this.broadcasts.lastTimestamp);
                }
            }
        }, 30000);
    }

    // Summary reports (every 12 hours)
    startSummaryReports() {
        const send12hReport = () => {
            const metrics = this.getSummaryMetrics();
            TelegramAlerter.sendSummaryReport(metrics, '12h');
        };
        
        const send24hReport = () => {
            const metrics = this.getSummaryMetrics();
            TelegramAlerter.sendSummaryReport(metrics, '24h');
        };
        
        // Every 12 hours
        setInterval(send12hReport, 12 * 60 * 60 * 1000);
        
        // Every 24 hours (offset by 12h)
        setTimeout(() => {
            send24hReport();
            setInterval(send24hReport, 24 * 60 * 60 * 1000);
        }, 12 * 60 * 60 * 1000);
        
        logger.info('Summary reports scheduled: 12h and 24h intervals');
    }

    // Get summary metrics for reports
    getSummaryMetrics() {
        const uptime = Date.now() - this.startTime;
        const memUsage = process.memoryUsage();
        
        // Calculate averages
        const avgMemory = this.memory.samples.length > 0
            ? this.memory.samples.reduce((sum, s) => sum + s.heapUsed, 0) / this.memory.samples.length
            : 0;
        
        const avgBroadcast = this.broadcasts.durations.length > 0
            ? this.broadcasts.durations.reduce((a, b) => a + b, 0) / this.broadcasts.durations.length / 1000
            : 0;
        
        const avgConnectionTime = this.connections.total > 0
            ? uptime / this.connections.total / 1000
            : 0;
        
        return {
            uniqueUsers: this.connections.byIP.size,
            peakConnections: this.connections.peak,
            totalConnections: this.connections.total,
            avgConnectionDuration: `${Math.round(avgConnectionTime)}s`,
            totalBroadcasts: this.broadcasts.count,
            avgBroadcastDuration: avgBroadcast.toFixed(2),
            totalDataSent: `${Math.round(this.broadcasts.totalDataSent / 1024 / 1024)} MB`,
            currentMemory: Math.round(memUsage.heapUsed / 1024 / 1024),
            peakMemory: Math.round(this.memory.peak / 1024 / 1024),
            avgMemory: Math.round(avgMemory / 1024 / 1024),
            totalErrors: this.errors.total,
            criticalErrors: this.errors.byType.get('critical') || 0,
            warnings: this.errors.byType.get('warning') || 0,
            uptime: this.formatUptime(uptime),
            restarts: this.restartCount
        };
    }

    formatUptime(ms) {
        const days = Math.floor(ms / 86400000);
        const hours = Math.floor((ms % 86400000) / 3600000);
        const minutes = Math.floor((ms % 3600000) / 60000);
        return `${days}d ${hours}h ${minutes}m`;
    }
}

export default new MetricsCollector();
