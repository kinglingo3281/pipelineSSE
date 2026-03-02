// Memory Watchdog - Monitors memory and prevents OOM crashes
import logger from './logger.js';
import TelegramAlerter from './telegram-alerter.js';
import MetricsCollector from './metrics-collector.js';

class MemoryWatchdog {
    constructor() {
        this.checkInterval = 30000; // 30 seconds
        this.warningThreshold = parseInt(process.env.MEMORY_WARNING_PERCENT || '80');
        this.criticalThreshold = parseInt(process.env.MEMORY_CRITICAL_PERCENT || '90');
        this.maxMemoryMB = parseInt(process.env.MAX_MEMORY_MB || '900');
        
        this.warningAlertSent = false;
        this.criticalAlertSent = false;
        this.restartScheduled = false;
        
        this.isRunning = false;
        this.intervalId = null;
    }

    start() {
        if (this.isRunning) {
            logger.warn('Memory watchdog already running');
            return;
        }

        this.isRunning = true;
        logger.info('Memory watchdog started', {
            checkInterval: `${this.checkInterval / 1000}s`,
            warningThreshold: `${this.warningThreshold}%`,
            criticalThreshold: `${this.criticalThreshold}%`,
            maxMemory: `${this.maxMemoryMB}MB`
        });

        this.intervalId = setInterval(() => this.check(), this.checkInterval);
        
        // Initial check
        this.check();
    }

    stop() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
        this.isRunning = false;
        logger.info('Memory watchdog stopped');
    }

    check() {
        const usage = process.memoryUsage();
        const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
        const heapTotalMB = Math.round(usage.heapTotal / 1024 / 1024);
        const rssMB = Math.round(usage.rss / 1024 / 1024);
        const percentUsed = (usage.heapUsed / usage.heapTotal) * 100;

        // Record metrics
        MetricsCollector.recordMemory(usage);

        logger.debug('Memory check', {
            heapUsed: `${heapUsedMB}MB`,
            heapTotal: `${heapTotalMB}MB`,
            rss: `${rssMB}MB`,
            percent: `${percentUsed.toFixed(1)}%`
        });

        // Check thresholds
        if (percentUsed >= this.criticalThreshold) {
            this.handleCritical(usage, percentUsed);
        } else if (percentUsed >= this.warningThreshold) {
            this.handleWarning(usage, percentUsed);
        } else {
            // Reset alert flags when back to normal
            this.warningAlertSent = false;
            this.criticalAlertSent = false;
        }

        // Check absolute memory limit
        if (rssMB > this.maxMemoryMB) {
            logger.error('Max memory limit exceeded', {
                rss: `${rssMB}MB`,
                limit: `${this.maxMemoryMB}MB`
            });
            this.scheduleRestart('Max memory limit exceeded');
        }
    }

    handleWarning(usage, percentUsed) {
        if (!this.warningAlertSent) {
            logger.warn('Memory warning threshold reached', {
                percent: `${percentUsed.toFixed(1)}%`,
                heapUsed: `${Math.round(usage.heapUsed / 1024 / 1024)}MB`,
                heapTotal: `${Math.round(usage.heapTotal / 1024 / 1024)}MB`
            });
            
            TelegramAlerter.alertMemoryWarning(usage);
            this.warningAlertSent = true;
        }
    }

    handleCritical(usage, percentUsed) {
        if (!this.criticalAlertSent) {
            logger.error('Memory critical threshold reached', {
                percent: `${percentUsed.toFixed(1)}%`,
                heapUsed: `${Math.round(usage.heapUsed / 1024 / 1024)}MB`,
                heapTotal: `${Math.round(usage.heapTotal / 1024 / 1024)}MB`
            });
            
            TelegramAlerter.alertMemoryCritical(usage);
            this.criticalAlertSent = true;
            
            // Schedule restart in 60 seconds
            this.scheduleRestart('Memory critical threshold exceeded');
        }
    }

    scheduleRestart(reason) {
        if (this.restartScheduled) {
            return;
        }

        this.restartScheduled = true;
        logger.error(`Server restart WOULD BE scheduled: ${reason} (auto-restart disabled)`);
        
        // DISABLED: Let it crash naturally if OOM, don't force restart
        // Just alert and log, PM2 will handle actual crashes
        
        // Force garbage collection if available
        if (global.gc) {
            logger.info('Running garbage collection...');
            global.gc();
        }
        
        // Reset flag after 5 minutes
        setTimeout(() => {
            this.restartScheduled = false;
        }, 300000);
    }

    getStatus() {
        const usage = process.memoryUsage();
        return {
            isRunning: this.isRunning,
            heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
            heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
            rss: Math.round(usage.rss / 1024 / 1024),
            percentUsed: ((usage.heapUsed / usage.heapTotal) * 100).toFixed(1),
            warningThreshold: this.warningThreshold,
            criticalThreshold: this.criticalThreshold,
            restartScheduled: this.restartScheduled
        };
    }
}

export default new MemoryWatchdog();
