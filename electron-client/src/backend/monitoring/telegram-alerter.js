// Telegram Alert System
import axios from 'axios';
import logger from './logger.js';

class TelegramAlerter {
    constructor() {
        this.botToken = process.env.TELEGRAM_BOT_TOKEN;
        this.chatId = process.env.TELEGRAM_CHAT_ID;
        this.enabled = process.env.TELEGRAM_ALERTS_ENABLED === 'true';
        this.apiUrl = `https://api.telegram.org/bot${this.botToken}/sendMessage`;
        
        // Rate limiting: max 1 alert per 60s for same type
        this.lastAlerts = new Map();
        this.alertCooldown = 60000; // 60 seconds
        
        if (this.enabled && (!this.botToken || !this.chatId)) {
            logger.warn('Telegram alerts enabled but BOT_TOKEN or CHAT_ID missing');
            this.enabled = false;
        }
    }

    async sendAlert(type, message, severity = 'warning') {
        if (!this.enabled) return;

        // Check rate limit
        const lastAlert = this.lastAlerts.get(type);
        if (lastAlert && Date.now() - lastAlert < this.alertCooldown) {
            logger.debug(`Alert throttled: ${type}`);
            return;
        }

        const emoji = severity === 'critical' ? '🔴' : severity === 'warning' ? '🟡' : 'ℹ️';
        const formattedMessage = `${emoji} SSE SERVER ALERT\n━━━━━━━━━━━━━━━━━━\n${message}\n━━━━━━━━━━━━━━━━━━\nTime: ${new Date().toISOString()}\nServer: ${process.env.SERVER_NAME || 'sse-prod'}`;

        try {
            await axios.post(this.apiUrl, {
                chat_id: this.chatId,
                text: formattedMessage,
                parse_mode: 'HTML'
            }, { timeout: 5000 });

            this.lastAlerts.set(type, Date.now());
            logger.info(`Telegram alert sent: ${type}`);
        } catch (error) {
            logger.error('Failed to send Telegram alert', { error: error.message });
        }
    }

    // Critical alerts
    async alertCrash(reason) {
        await this.sendAlert('crash', `Issue: Server Crash\nReason: ${reason}\nAction: Auto-restart in progress`, 'critical');
    }

    async alertMemoryCritical(usage) {
        const percent = ((usage.heapUsed / usage.heapTotal) * 100).toFixed(1);
        await this.sendAlert('memory-critical', 
            `Issue: Memory Critical\nValue: ${Math.round(usage.heapUsed / 1024 / 1024)} MB / ${Math.round(usage.heapTotal / 1024 / 1024)} MB (${percent}%)\nAction: Auto-restart in 60s`, 
            'critical'
        );
    }

    async alertMemoryWarning(usage) {
        const percent = ((usage.heapUsed / usage.heapTotal) * 100).toFixed(1);
        await this.sendAlert('memory-warning', 
            `Issue: Memory Warning\nValue: ${Math.round(usage.heapUsed / 1024 / 1024)} MB / ${Math.round(usage.heapTotal / 1024 / 1024)} MB (${percent}%)\nAction: Monitoring closely`, 
            'warning'
        );
    }

    async alertConnectionSpike(count) {
        await this.sendAlert('connection-spike', 
            `Issue: Connection Spike\nValue: ${count} active connections\nAction: Monitoring for DOS`, 
            'warning'
        );
    }

    async alertHighErrorRate(rate, count) {
        await this.sendAlert('error-rate', 
            `Issue: High Error Rate\nValue: ${rate} errors/min (${count} total)\nAction: Check logs for issues`, 
            'critical'
        );
    }

    async alertDatabaseFailure(error) {
        await this.sendAlert('database', 
            `Issue: Database Connection Failed\nError: ${error}\nAction: Retrying connection`, 
            'critical'
        );
    }

    async alertBroadcastLag(duration) {
        await this.sendAlert('broadcast-lag', 
            `Issue: Broadcast Lag\nValue: ${(duration / 1000).toFixed(1)}s (threshold: 25s)\nAction: Monitoring performance`, 
            'warning'
        );
    }

    async alertBroadcastStuck(lastTime) {
        await this.sendAlert('broadcast-stuck', 
            `Issue: No Broadcasts\nValue: Last broadcast ${Math.round((Date.now() - lastTime) / 1000 / 60)} min ago\nAction: Check server health`, 
            'critical'
        );
    }

    // Daily/12-hour summary reports
    async sendSummaryReport(metrics, period = '12h') {
        if (!this.enabled) return;

        const report = `
📊 SSE SERVER REPORT (${period})
━━━━━━━━━━━━━━━━━━━━━━━━

👥 USERS
• Total Unique: ${metrics.uniqueUsers}
• Peak Concurrent: ${metrics.peakConnections}
• Total Connections: ${metrics.totalConnections}
• Avg Connection Time: ${metrics.avgConnectionDuration}

📡 BROADCASTS  
• Total Sent: ${metrics.totalBroadcasts}
• Avg Duration: ${metrics.avgBroadcastDuration}s
• Data Transferred: ${metrics.totalDataSent}

💾 MEMORY
• Current: ${metrics.currentMemory} MB
• Peak: ${metrics.peakMemory} MB
• Avg: ${metrics.avgMemory} MB

❌ ERRORS
• Total: ${metrics.totalErrors}
• Critical: ${metrics.criticalErrors}
• Warnings: ${metrics.warnings}

⏱️ UPTIME
• Server: ${metrics.uptime}
• Restarts: ${metrics.restarts}

━━━━━━━━━━━━━━━━━━━━━━━━
Time: ${new Date().toISOString()}
`;

        try {
            await axios.post(this.apiUrl, {
                chat_id: this.chatId,
                text: report,
                parse_mode: 'HTML'
            }, { timeout: 5000 });

            logger.info(`Summary report sent: ${period}`);
        } catch (error) {
            logger.error('Failed to send summary report', { error: error.message });
        }
    }
}

export default new TelegramAlerter();
