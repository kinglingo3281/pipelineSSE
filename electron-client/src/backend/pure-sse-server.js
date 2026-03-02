// Pure SSE Server - Streams all data, no API endpoints
import 'dotenv/config';
import express from 'express';
import { Pool } from 'pg';
import cors from 'cors';
import crypto from 'crypto';
import https from 'https';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import zlib from 'zlib';
// Enhanced data watcher (external to electron-client)
import EnhancedDataWatcher from '../../../enhanced-data-watcher.js';
// Monitoring system
import logger from './monitoring/logger.js';
import MemoryWatchdog from './monitoring/memory-watchdog.js';
import MetricsCollector from './monitoring/metrics-collector.js';
import DashboardServer from './monitoring/dashboard-server.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Auth configuration
const CLIENT_AUTH_KEY = process.env.SSE_MASTER_KEY || "change_me_in_production";
const AUTH_TIMEOUT = 300000; // 5 minutes

// Security limits - DISABLED (to prevent 429 errors)
const MAX_CONNECTIONS_PER_IP = 999999;  // Unlimited connections per IP
const MAX_GLOBAL_CONNECTIONS = 999999; // Unlimited global connections
const MAX_FAILED_ATTEMPTS = 999999;  // Unlimited auth attempts
const FAILED_ATTEMPT_WINDOW = 1000; // 1 second - failures expire quickly

// Startup grace period to prevent old data broadcasts
const SERVER_STARTUP_TIME = Date.now();
const STARTUP_GRACE_PERIOD = 15 * 60 * 1000; // 15 minutes

function isInStartupGracePeriod() {
    return Date.now() - SERVER_STARTUP_TIME < STARTUP_GRACE_PERIOD;
}

// Security tracking
const failedAttempts = new Map();
const connectionsByIP = new Map();

const app = express();
const PORT = process.env.PORT || 8080;

// Separate pools for different operations
const ssePool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:password@localhost:5432/sse_dashboard',
    max: 250,                   // Dedicated to SSE operations
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
    acquireTimeoutMillis: 60000,
    allowExitOnIdle: true
});

const fileProcessingPool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:password@localhost:5432/sse_dashboard',
    max: 150,                    // Dedicated to file processing
    idleTimeoutMillis: 60000,   // Longer idle time for file operations
    connectionTimeoutMillis: 60000,
    acquireTimeoutMillis: 120000, // Longer acquire timeout
    allowExitOnIdle: true
});

// Legacy pool reference for backward compatibility (points to ssePool)
const pool = ssePool;

// Prevent unhandled pool errors from crashing the process
ssePool.on('error', (err) => {
    log('error', 'SSE PG pool error', { error: err?.message });
});
fileProcessingPool.on('error', (err) => {
    log('error', 'FileProcessing PG pool error', { error: err?.message });
});

// Initialize Enhanced Data Watcher with dedicated pool
let dataWatcher = null;
try {
    dataWatcher = new EnhancedDataWatcher(
        fileProcessingPool,  // Use dedicated pool instead of shared
        process.env.VISUALIZATIONS_DIR || './data/visualizations/'
    );
    // Start watching for enhanced analysis files
    dataWatcher.start();
    log('info', 'EnhancedDataWatcher started');
} catch (err) {
    log('warn', 'EnhancedDataWatcher failed to initialize', { error: err?.message });
}

// Certificate pinning middleware for production
function validateCertificatePin(req, res, next) {
    if (process.env.NODE_ENV === 'production' && process.env.CERT_FINGERPRINT) {
        const clientCert = req.connection.getPeerCertificate();
        if (clientCert && clientCert.fingerprint256) {
            const clientFingerprint = clientCert.fingerprint256.replace(/:/g, '').toLowerCase();
            if (clientFingerprint !== process.env.CERT_FINGERPRINT.toLowerCase()) {
                log('warn', 'Certificate pinning validation failed', {
                    expected: process.env.CERT_FINGERPRINT,
                    received: clientFingerprint,
                    ip: req.ip
                });
                return res.status(403).json({ error: 'Certificate validation failed' });
            }
        }
    }
    next();
}

// Enhanced CORS configuration for ngrok
const corsOptions = {
    origin: function (origin, callback) {
        const allowedOrigins = process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : [];
        
        // Allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);
        
        // Check for exact matches or ngrok patterns
        const isAllowed = allowedOrigins.some(allowed => {
            if (allowed.includes('*')) {
                // Handle wildcard patterns like https://*.ngrok.io
                const pattern = allowed.replace(/\*/g, '.*');
                const regex = new RegExp(`^${pattern}$`);
                return regex.test(origin);
            }
            return allowed === origin;
        });
        
        if (isAllowed) {
            callback(null, true);
        } else {
            log('warn', 'CORS blocked origin:', origin);
            callback(new Error('Not allowed by CORS'));
        }
    },
    credentials: true,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
};

// Middleware
app.use(cors(corsOptions));
app.use(express.static('public'));

// Security headers (minimal for API/stream server)
app.use((req, res, next) => {
    // Prevent MIME type sniffing - important for JSON endpoints
    res.setHeader('X-Content-Type-Options', 'nosniff');
    
    // Control referrer information leakage
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    
    next();
});

// Enable JSON body parsing for webhook
app.use(express.json());
// Apply certificate pinning middleware to all routes
app.use(validateCertificatePin);
// Mount admin dashboard routes
app.use(DashboardServer.getRouter());

// Client tracking maps
const sseConnections = new Map(); // Active SSE connections
const clientsNeedingInitialData = new Set(); // Clients waiting for initial historical data batch

// Logging utility (legacy wrapper for backward compatibility)
function log(level, message, data = null) {
    if (data) {
        logger[level](message, data);
    } else {
        logger[level](message);
    }
}

// Build compressed SSE message (gzip + base64 encoded)
function buildCompressedMessage(type, data) {
    try {
        // Serialize to JSON
        const json = JSON.stringify({ 
            type, 
            data, 
            timestamp: new Date().toISOString() 
        });
        
        // Compress with gzip
        const compressed = zlib.gzipSync(json);
        
        // Base64 encode for SSE transport
        const encoded = compressed.toString('base64');
        
        // Debug: Log compression stats for large messages
        if (json.length > 100000) {
            const compressionRatio = ((1 - (compressed.length / json.length)) * 100).toFixed(1);
            log('debug', `Compression: ${type} | Original: ${(json.length / 1024).toFixed(0)}KB → Compressed: ${(compressed.length / 1024).toFixed(0)}KB → Base64: ${(encoded.length / 1024).toFixed(0)}KB | Ratio: ${compressionRatio}%`);
        }
        
        // Prefix with GZIP marker for client-side detection
        return `data: GZIP:${encoded}\n\n`;
    } catch (error) {
        log('error', 'Failed to compress message', error.message);
        // Fallback to uncompressed
        return `data: ${JSON.stringify({ type, data, timestamp: new Date().toISOString() })}\n\n`;
    }
}

// Build uncompressed SSE message (for connection confirmation only)
function buildMessage(type, data) {
    return `data: ${JSON.stringify({ type, data, timestamp: new Date().toISOString() })}\n\n`;
}

// Async broadcast to all clients with backpressure handling and compression
async function broadcastToAllAsync(type, data) {
    if (sseConnections.size === 0) return;
    
    const broadcastStart = Date.now();
    const message = buildCompressedMessage(type, data);
    const clients = Array.from(sseConnections.entries());
    const CHUNK_SIZE = 10; // Process 10 clients at a time
    
    let successCount = 0;
    let failCount = 0;
    
    // Process clients in chunks to handle backpressure
    for (let i = 0; i < clients.length; i += CHUNK_SIZE) {
        const chunk = clients.slice(i, i + CHUNK_SIZE);
        
        await Promise.all(
            chunk.map(async ([clientId, res]) => {
                try {
                    const canWrite = res.write(message);
                    
                    // If TCP buffer is full, wait for drain event
                    if (!canWrite) {
                        await new Promise((resolve) => {
                            res.once('drain', resolve);
                            // Timeout after 5 seconds to prevent hanging
                            setTimeout(resolve, 5000);
                        });
                    }
                    
                    successCount++;
                } catch (error) {
                    log('error', `Broadcast failed for ${clientId}`, error.message);
                    MetricsCollector.recordError(error, 'broadcast');
                    sseConnections.delete(clientId);
                    failCount++;
                }
            })
        );
    }
    
    if (successCount > 0) {
        log('info', `Broadcasted ${type} to ${successCount} clients (${failCount} failed, compressed)`);
    }
    
    // Track broadcast metrics
    const broadcastDuration = Date.now() - broadcastStart;
    const messageSize = message.length;
    MetricsCollector.recordBroadcast(broadcastDuration, successCount, messageSize);
}

// Async broadcast to specific clients with backpressure and compression
async function broadcastToClientsAsync(clientIds, type, data) {
    if (clientIds.length === 0) return 0;
    
    const message = buildCompressedMessage(type, data);
    const CHUNK_SIZE = 10;
    let successCount = 0;
    
    for (let i = 0; i < clientIds.length; i += CHUNK_SIZE) {
        const chunk = clientIds.slice(i, i + CHUNK_SIZE);
        
        await Promise.all(
            chunk.map(async (clientId) => {
                const res = sseConnections.get(clientId);
                if (res) {
                    try {
                        const canWrite = res.write(message);
                        
                        if (!canWrite) {
                            await new Promise((resolve) => {
                                res.once('drain', resolve);
                                setTimeout(resolve, 5000);
                            });
                        }
                        
                        successCount++;
                    } catch (error) {
                        log('error', `Broadcast failed for ${clientId}`, error.message);
                        sseConnections.delete(clientId);
                    }
                }
            })
        );
    }
    
    return successCount;
}

// Send to individual client (uncompressed, for connection confirmation only)
function sendToClient(clientId, type, data) {
    const res = sseConnections.get(clientId);
    if (res) {
        try {
            res.write(buildMessage(type, data));
        } catch (error) {
            log('error', `Failed to send to ${clientId}`, error.message);
            sseConnections.delete(clientId);
        }
    }
}

// Prepare broadcast data - strip full enhanced_data to reduce size
function prepareBroadcastData(trades) {
    if (!Array.isArray(trades)) return trades;
    
    return trades.map(trade => {
        const { enhanced_data, ...tradeWithoutEnhanced } = trade;
        
        // Return trade with essential columns only (enhanced_data stripped)
        // Individual columns (dominant_bias, etc.) are already present from SQL SELECT
        return {
            ...tradeWithoutEnhanced,
            // Add minimal enhanced_data marker for client detection
            enhanced_data: {
                available: !!enhanced_data,
                has_clusters: !!(enhanced_data?.clusters),
                session_id: enhanced_data?.session_id
            }
        };
    });
}

// Time-based pagination cache system
const recentTradesCache = { data: [], lastUpdate: 0 };
const historicalTradesCache = { data: [], lastUpdate: 0 };
const assetsCache = { data: [], lastUpdate: 0 };
const marketContextCache = { data: [], lastUpdate: 0 };

// Cache configuration - Enhanced responsiveness with randomized intervals
function getRandomCacheInterval(base, variance = 0.3) {
    const min = base * (1 - variance);
    const max = base * (1 + variance);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

const RECENT_REFRESH_BASE = 2500;         // 2.5 seconds base (1.75-3.25s range)
const HISTORICAL_REFRESH_INTERVAL = 300000; // 5 minutes  
const ASSETS_REFRESH_INTERVAL = 60000;    // 1 minute
const MARKET_REFRESH_INTERVAL = 120000;   // 2 minutes
const VISIBILITY_THRESHOLD = 600000;      // 10 minutes

// In-memory cache for optimized queries (legacy - will be replaced)
const queryCache = new Map();
const CACHE_TTL = 30000; // 30 seconds

// Cache helper functions
function getCacheKey(queryType, params = {}) {
    return `${queryType}_${JSON.stringify(params)}`;
}

function getCachedData(key) {
    const cached = queryCache.get(key);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
        return cached.data;
    }
    queryCache.delete(key);
    return null;
}

function setCachedData(key, data) {
    queryCache.set(key, { data, timestamp: Date.now() });
    
    // CRITICAL FIX: Limit cache size to prevent memory leak
    if (queryCache.size > 5000) {
        // Remove oldest entries
        const entries = Array.from(queryCache.entries());
        entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
        // Remove oldest 20%
        const toRemove = Math.floor(entries.length * 0.2);
        for (let i = 0; i < toRemove; i++) {
            queryCache.delete(entries[i][0]);
        }
    }
}

// Time-based cache update functions
async function updateRecentTradesCache() {
    let client;
    try {
        client = await ssePool.connect();
        
        const tenMinutesAgo = new Date(Date.now() - VISIBILITY_THRESHOLD);
        const recentQuery = `
            SELECT t.id, t.asset, t.direction, t.entry_price, t.target_price, t.stop_price, 
                   t.confidence, t.signal_type, t.risk_reward, t.file_timestamp, t.created_at,
                   t.tp_hours, t.sl_hours, t.tp_range, t.sl_range, t.duration_confidence,
                   t.source_file, t.current_price,
                   t.dominant_bias, t.risk_assessment, t.long_quality_score, t.short_quality_score,
                   t.long_cascade_probability, t.short_cascade_probability, t.overall_cascade_probability,
                   t.directional_strength, t.total_clusters, t.long_cluster_count, t.short_cluster_count,
                   t.volatility_level, t.trend_direction, t.market_bias,
                   t.enhanced_data
            FROM comprehensive_trades t
            WHERE t.created_at > $1
            ORDER BY t.created_at DESC
            LIMIT 500;
        `;
        
        const result = await client.query(recentQuery, [tenMinutesAgo]);
        
        // Transform rows: keep full enhanced_data in cache, but prepare for lightweight broadcast
        recentTradesCache.data = result.rows;
        recentTradesCache.lastUpdate = Date.now();
        
        log('info', `Updated recent trades cache: ${result.rows.length} trades (0-10min)`);
        
    } catch (error) {
        MetricsCollector.recordError(error, 'cache-update');
        log('error', 'Failed to update recent trades cache', {
            message: error.message,
            code: error.code,
            detail: error.detail,
            hint: error.hint,
            stack: error.stack?.split('\n')[0] // First line of stack trace
        });
    } finally {
        if (client) client.release();
    }
}

async function updateHistoricalTradesCache() {
    let client;
    try {
        client = await ssePool.connect();
        
        const tenMinutesAgo = new Date(Date.now() - VISIBILITY_THRESHOLD);
        const historicalQuery = `
            SELECT 
                t.id, t.asset, t.direction, t.entry_price, t.target_price, t.stop_price,
                t.current_price, t.confidence, t.risk_reward, t.tp_hours, t.sl_hours,
                t.tp_range, t.sl_range, t.duration_confidence, t.signal_type,
                t.source_file, t.file_timestamp, t.created_at,
                t.dominant_bias, t.risk_assessment, t.long_quality_score, t.short_quality_score,
                t.long_cascade_probability, t.short_cascade_probability, t.overall_cascade_probability,
                t.directional_strength, t.total_clusters, t.long_cluster_count, t.short_cluster_count,
                t.volatility_level, t.trend_direction, t.market_bias,
                t.enhanced_data
            FROM comprehensive_trades t
            WHERE t.created_at <= $1
            ORDER BY t.created_at DESC
            LIMIT 2500;
        `;
        
        const result = await client.query(historicalQuery, [tenMinutesAgo]);
        
        // Transform rows: keep full enhanced_data in cache, but prepare for lightweight broadcast
        historicalTradesCache.data = result.rows;
        historicalTradesCache.lastUpdate = Date.now();
        
        log('info', `Updated historical trades cache: ${result.rows.length} trades (10min+)`);
        
    } catch (error) {
        log('error', 'Failed to update historical trades cache', {
            message: error.message,
            code: error.code,
            detail: error.detail,
            hint: error.hint,
            stack: error.stack?.split('\n')[0]
        });
    } finally {
        if (client) client.release();
    }
}

async function updateAssetsCache() {
    let client;
    try {
        client = await ssePool.connect();
        
        const assetsQuery = `
            SELECT asset, COUNT(*) as trade_count
            FROM comprehensive_trades
            GROUP BY asset
            ORDER BY trade_count DESC;
        `;
        
        const result = await client.query(assetsQuery);
        
        assetsCache.data = result.rows;
        assetsCache.lastUpdate = Date.now();
        
        log('info', `Updated assets cache: ${result.rows.length} assets`);
        
    } catch (error) {
        log('error', 'Failed to update assets cache', {
            message: error.message,
            code: error.code,
            detail: error.detail,
            hint: error.hint,
            stack: error.stack?.split('\n')[0]
        });
    } finally {
        if (client) client.release();
    }
}

async function updateMarketContextCache() {
    let client;
    try {
        client = await ssePool.connect();
        
        const marketQuery = `
            SELECT asset, atr_percent, volatility_level, trend_direction, created_at
            FROM market_context
            WHERE created_at > NOW() - INTERVAL '2 hours'
            ORDER BY created_at DESC
            LIMIT 500;
        `;
        
        const result = await client.query(marketQuery);
        
        marketContextCache.data = result.rows;
        marketContextCache.lastUpdate = Date.now();
        
        log('info', `Updated market context cache: ${result.rows.length} contexts`);
        
    } catch (error) {
        log('error', 'Failed to update market context cache', {
            message: error.message,
            code: error.code,
            detail: error.detail,
            hint: error.hint,
            stack: error.stack?.split('\n')[0]
        });
    } finally {
        if (client) client.release();
    }
}

// Database connection test function
async function testDatabaseConnection() {
    let client;
    try {
        client = await ssePool.connect();
        
        // Test basic connection
        await client.query('SELECT 1');
        
        // Test if comprehensive_trades table exists
        const tableCheck = await client.query(`
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'comprehensive_trades'
            );
        `);
        
        if (!tableCheck.rows[0].exists) {
            throw new Error('comprehensive_trades table does not exist');
        }
        
        // Test if market_context table exists
        const marketTableCheck = await client.query(`
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'market_context'
            );
        `);
        
        if (!marketTableCheck.rows[0].exists) {
            log('warn', 'market_context table does not exist - market context cache will be disabled');
        }
        
        log('info', 'Database connection test successful');
        return true;
        
    } catch (error) {
        MetricsCollector.recordError(error, 'database');
        log('error', 'Database connection test failed', {
            message: error.message,
            code: error.code,
            detail: error.detail,
            hint: error.hint
        });
        return false;
    } finally {
        if (client) client.release();
    }
}

// Enhanced cache update with retry logic
async function updateCacheWithRetry(updateFunction, cacheName, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            await updateFunction();
            return; // Success, exit retry loop
        } catch (error) {
            MetricsCollector.recordError(error, 'cache-retry');
            log('warn', `Cache update attempt ${attempt}/${maxRetries} failed for ${cacheName}`, {
                message: error.message,
                code: error.code,
                stack: error.stack?.split('\n')[0]
            });
            
            if (attempt === maxRetries) {
                log('error', `All ${maxRetries} attempts failed for ${cacheName} cache update - cache will be stale`);
                return; // Give up after max retries, don't crash
            }
            
            // Wait before retry (exponential backoff)
            const delay = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

// Async batch broadcast initial data to waiting clients (called every 2 seconds)
async function broadcastInitialDataToWaiting() {
    if (clientsNeedingInitialData.size === 0) return;
    
    // CRITICAL FIX: Don't send empty data - wait for cache to be populated
    if (historicalTradesCache.data.length === 0) {
        log('warn', `Skipping initial data broadcast - cache not yet populated (${clientsNeedingInitialData.size} clients waiting)`);
        return; // Keep clients in queue, will retry in 2 seconds
    }
    
    const clients = Array.from(clientsNeedingInitialData);
    log('info', `Broadcasting initial data to ${clients.length} waiting clients`);
    
    // Prepare lightweight broadcast data (strip full enhanced_data)
    const lightweightTrades = prepareBroadcastData(historicalTradesCache.data);
    
    // Broadcast historical trades with compression and backpressure
    const sentTrades = await broadcastToClientsAsync(clients, 'all-trades', lightweightTrades);
    
    // Broadcast assets
    await broadcastToClientsAsync(clients, 'all-assets', assetsCache.data);
    
    // Broadcast market context
    await broadcastToClientsAsync(clients, 'market-context', marketContextCache.data);
    
    // Clear the set - all clients have received initial data
    clientsNeedingInitialData.clear();
    
    log('info', `Initial data sent: ${historicalTradesCache.data.length} trades (lightweight), ${assetsCache.data.length} assets to ${sentTrades} clients (compressed)`);
}

// Async broadcast new trades to all connected clients (called every 30 seconds)
async function broadcastNewTrades() {
    // Skip broadcasts during startup grace period to prevent old data spam
    if (isInStartupGracePeriod()) {
        log('debug', 'Skipping new trades broadcast (startup grace period)');
        return;
    }
    
    if (sseConnections.size === 0) return;
    
    // Prepare lightweight broadcast data (strip full enhanced_data)
    const lightweightTrades = prepareBroadcastData(recentTradesCache.data);
    
    await broadcastToAllAsync('new-trades', {
        trades: lightweightTrades,
        newCount: recentTradesCache.data.length,
        lastUpdate: recentTradesCache.lastUpdate
    });
}
// Async broadcast heartbeat to all connected clients (called every 30 seconds)
async function broadcastHeartbeat() {
    if (sseConnections.size === 0) return;
    
    await broadcastToAllAsync('heartbeat', {
        activeConnections: sseConnections.size,
        timestamp: new Date().toISOString(),
        startupMode: isInStartupGracePeriod() // Inform clients server is warming up
    });
}

// Background enhancement loading
async function loadEnhancedDataAsync(clientId, trades) {
    let client;
    try {
        // Check cache first
        const cacheKey = getCacheKey('enhanced_data', { tradeIds: trades.map(t => t.id) });
        const cached = getCachedData(cacheKey);
        if (cached) {
            sendToClient(clientId, 'enhanced-data', cached);
            return;
        }
        
        client = await ssePool.connect();
        
        // Load cluster data for specific trades only
        const tradeIds = trades.map(t => t.id).slice(0, 10); // Limit to 10 trades
        if (tradeIds.length > 0) {
            const clusterQuery = `
                SELECT t.id as trade_id, 
                       COUNT(cd.id) as cluster_count,
                       AVG(cd.trigger_probability) as avg_trigger_prob
                FROM comprehensive_trades t
                WHERE t.id = ANY($1)
                GROUP BY t.id;
            `;
            
            const clusters = await client.query(clusterQuery, [tradeIds]);
            
            // Cache and send enhanced data
            const enhancedData = { clusters: clusters.rows };
            setCachedData(cacheKey, enhancedData);
            sendToClient(clientId, 'enhanced-data', enhancedData);
        }
        
    } catch (error) {
        log('error', 'Failed to load enhanced data', error.message);
    } finally {
        if (client) client.release();
    }
}

// REST API REMOVED - Pure SSE architecture only
// Pagination now handled via cached data in SSE streams

// Get trade details with clusters
async function streamTradeDetails(clientId, tradeId) {
    let client;
    try {
        log('info', `Streaming trade details for ${tradeId} to client ${clientId}`);
        
        client = await ssePool.connect();
        
        const tradeQuery = `
            SELECT 
                t.*,
                t.enhanced_data
            FROM comprehensive_trades t
            WHERE t.id = $1
        `;
        
        const result = await client.query(tradeQuery, [tradeId]);
        
        if (result.rows.length > 0) {
            sendToClient(clientId, 'trade-details', result.rows[0]);
        } else {
            sendToClient(clientId, 'error', { message: 'Trade not found' });
        }
        
    } catch (error) {
        log('error', 'Failed to get trade details', error.message);
        sendToClient(clientId, 'error', { message: 'Failed to load trade details', error: error.message });
    } finally {
        if (client) client.release();
    }
}

// Security functions
function checkFailedAuthLimit(ip) {
    const now = Date.now();
    const attempts = failedAttempts.get(ip) || [];
    const recent = attempts.filter(time => now - time < FAILED_ATTEMPT_WINDOW);
    
    if (recent.length >= MAX_FAILED_ATTEMPTS) {
        log('warn', `IP ${ip} blocked: ${recent.length} failed auth attempts`);
        return false;
    }
    
    return true;
}

function recordFailedAuth(ip) {
    const now = Date.now();
    const attempts = failedAttempts.get(ip) || [];
    attempts.push(now);
    
    // CRITICAL FIX: Remove old attempts to prevent memory leak
    const recentAttempts = attempts.filter(time => now - time < FAILED_ATTEMPT_WINDOW);
    failedAttempts.set(ip, recentAttempts);
    
    // Cleanup: If no recent attempts, remove entry entirely
    if (recentAttempts.length === 0) {
        failedAttempts.delete(ip);
    }
}

function checkConnectionLimits(ip) {
    // Production: Enforce limits on ALL IPs (no exemptions)
    // Only allow localhost exemption in development mode
    const isDevelopment = process.env.NODE_ENV === 'development';
    const isLocalhost = ip === '127.0.0.1' || ip === '::1' || ip === '::ffff:127.0.0.1';
    const shouldExempt = isDevelopment && isLocalhost;
    
    // Count current connections per IP
    let ipConnections = 0;
    for (const [clientId, res] of sseConnections) {
        if (res.socket && res.socket.remoteAddress === ip) {
            ipConnections++;
        }
    }
    
    // DISABLED: No connection limits to prevent 429 errors
    // if (!shouldExempt && ipConnections >= MAX_CONNECTIONS_PER_IP) {
    //     return { allowed: false, reason: `IP connection limit reached (${MAX_CONNECTIONS_PER_IP} connections per user)` };
    // }
    
    // DISABLED: No global connection limit
    // if (sseConnections.size >= MAX_GLOBAL_CONNECTIONS) {
    //     return { allowed: false, reason: `Server capacity reached (${MAX_GLOBAL_CONNECTIONS})` };
    // }
    
    return { allowed: true };
}

// Auth validation function
function validateAuth(signature, timestamp, clientId) {
    try {
        // Check timestamp (5 minute window)
        const now = Date.now();
        const requestTime = parseInt(timestamp);
        if (now - requestTime > AUTH_TIMEOUT) {
            log('warn', `Auth expired for ${clientId}: ${now - requestTime}ms old`);
            return false;
        }

        // Generate expected signature
        const challenge = `${timestamp}:${clientId}`;
        const expected = crypto.createHmac('sha256', CLIENT_AUTH_KEY)
            .update(challenge)
            .digest('hex');
        
        const isValid = signature === expected;
        if (!isValid) {
            log('warn', `Invalid signature for ${clientId}`);
        }
        
        return isValid;
    } catch (error) {
        log('error', 'Auth validation error:', error.message);
        return false;
    }
}

// V3 Signal Webhook Endpoint - Authenticated
app.post('/api/v3-signal', async (req, res) => {
    const clientIP = req.ip || req.connection.remoteAddress;
    
    try {
        // STEP 1: Authentication - Validate Bearer token (OPTIONAL)
        const authHeader = req.headers.authorization;
        const webhookSecret = process.env.V3_WEBHOOK_SECRET;
        
        // Only check auth if V3_WEBHOOK_SECRET is configured
        if (webhookSecret) {
            const expectedAuth = `Bearer ${webhookSecret}`;
            if (!authHeader || authHeader !== expectedAuth) {
                log('warn', `V3 webhook: Unauthorized attempt from ${clientIP}`, {
                    provided: authHeader ? 'Bearer ***' : 'none'
                });
                return res.status(401).json({ 
                    error: 'Unauthorized',
                    message: 'Invalid or missing authentication token'
                });
            }
            log('info', `V3 webhook: Auth passed from ${clientIP}`);
        } else {
            // No auth required if V3_WEBHOOK_SECRET not set
            log('info', `V3 webhook: Auth bypassed (V3_WEBHOOK_SECRET not configured) from ${clientIP}`);
        }
        
        // STEP 2: Validate payload format
        const { signal } = req.body;
        
        if (!signal || typeof signal !== 'string') {
            log('warn', `V3 webhook: Invalid payload from ${clientIP}`, { body: req.body });
            return res.status(400).json({ 
                error: 'Bad Request',
                message: 'Missing or invalid "signal" field in payload'
            });
        }
        
        // STEP 3: Parse signal text
        // Expected format: "🟢 BTC/USDT LONG\nEntry: 45000.50\nTP: 48000.00\nSL: 43500.00"
        
        // Extract trading pair (e.g., "BTC/USDT")
        const pairMatch = signal.match(/([A-Z0-9]{2,10})\/([A-Z]{2,5})/);
        if (!pairMatch) {
            log('warn', `V3 webhook: No valid pair found in signal from ${clientIP}`, { signal });
            return res.status(400).json({ 
                error: 'Invalid Signal Format',
                message: 'Could not extract trading pair (expected format: BTC/USDT)'
            });
        }
        const asset = pairMatch[1]; // Just base asset: "BTC" from "BTC/USDT"
        
        // Extract direction (case-insensitive, word boundary)
        const direction = /\blong\b/i.test(signal) ? 'long' : /\bshort\b/i.test(signal) ? 'short' : null;
        if (!direction) {
            log('warn', `V3 webhook: No direction found in signal from ${clientIP}`, { signal });
            return res.status(400).json({ 
                error: 'Invalid Signal Format',
                message: 'Could not extract direction (expected LONG or SHORT in message)'
            });
        }
        
        // Extract prices using regex (handles "- Entry:" format)
        const entryMatch = signal.match(/[-\s]*Entry[:\s]+(\d+\.?\d*)/i);
        const tpMatch = signal.match(/[-\s]*TP[:\s]+(\d+\.?\d*)/i);
        const slMatch = signal.match(/[-\s]*SL[:\s]+(\d+\.?\d*)/i);
        const confidenceMatch = signal.match(/Confidence[:\s]+(\d+)%?/i);
        
        if (!entryMatch || !tpMatch || !slMatch) {
            log('warn', `V3 webhook: Missing price data from ${clientIP}`, { 
                signal,
                found: { entry: !!entryMatch, tp: !!tpMatch, sl: !!slMatch }
            });
            return res.status(400).json({ 
                error: 'Invalid Signal Format',
                message: 'Could not extract Entry, TP, or SL prices'
            });
        }
        
        const entry_price = parseFloat(entryMatch[1]);
        const target_price = parseFloat(tpMatch[1]);
        const stop_price = parseFloat(slMatch[1]);
        const confidence = confidenceMatch ? parseFloat(confidenceMatch[1]) / 100 : 0.75;
        
        // STEP 4: Validate price logic
        if (direction === 'long') {
            if (target_price <= entry_price) {
                return res.status(400).json({ 
                    error: 'Invalid Prices',
                    message: 'LONG signal: TP must be > Entry'
                });
            }
            if (stop_price >= entry_price) {
                return res.status(400).json({ 
                    error: 'Invalid Prices',
                    message: 'LONG signal: SL must be < Entry'
                });
            }
        } else { // short
            if (target_price >= entry_price) {
                return res.status(400).json({ 
                    error: 'Invalid Prices',
                    message: 'SHORT signal: TP must be < Entry'
                });
            }
            if (stop_price <= entry_price) {
                return res.status(400).json({ 
                    error: 'Invalid Prices',
                    message: 'SHORT signal: SL must be > Entry'
                });
            }
        }
        
        // STEP 5: Insert to database
        let client;
        try {
            client = await ssePool.connect();
            
            const query = `
                INSERT INTO comprehensive_trades (
                    asset, direction, entry_price, target_price, stop_price,
                    confidence, signal_type, current_price, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, 'v3', $3, NOW())
                RETURNING id, created_at
            `;
            
            const values = [asset, direction, entry_price, target_price, stop_price, confidence];
            const result = await client.query(query, values);
            const trade = result.rows[0];
            
            log('info', `V3 signal inserted: ${asset} ${direction} @ ${entry_price}`, {
                trade_id: trade.id,
                target: target_price,
                stop: stop_price,
                confidence: confidence,
                source_ip: clientIP
            });
            
            // STEP 6: Success response
            res.status(200).json({ 
                success: true,
                trade_id: trade.id,
                created_at: trade.created_at,
                message: 'V3 signal processed and inserted to database'
            });
            
        } finally {
            if (client) client.release();
        }
        
    } catch (error) {
        log('error', 'V3 webhook processing error', {
            error: error.message,
            stack: error.stack?.split('\n')[0],
            ip: clientIP
        });
        
        res.status(500).json({ 
            error: 'Internal Server Error',
            message: process.env.NODE_ENV === 'development' ? error.message : 'Failed to process signal'
        });
    }
});

// Advanced request validation middleware
function validateRequestFingerprint(req, res, next) {
    // Skip validation for OPTIONS (CORS preflight) requests
    if (req.method === 'OPTIONS') {
        return next();
    }
    
    const userAgent = req.get('User-Agent') || '';
    const acceptHeader = req.get('Accept') || '';
    
    // Block obvious automated tools and scrapers (but allow Electron clients and browsers)
    const blockedPatterns = [
        /curl/i, /wget/i, /python/i, /postman/i, /insomnia/i,
        /bot/i, /crawler/i, /spider/i, /scraper/i
    ];
    
    // Allow legitimate clients: Electron or browsers (Mozilla/Chrome)
    const isElectronClient = userAgent.includes('electron-client') || userAgent.includes('Electron');
    const isBrowser = userAgent.includes('Mozilla') || userAgent.includes('Chrome') || userAgent.includes('Safari');
    
    if (blockedPatterns.some(pattern => pattern.test(userAgent)) && !isElectronClient && !isBrowser) {
        log('warn', 'Blocked automated tool request', { userAgent, ip: req.ip });
        return res.status(403).json({ error: 'Access denied' });
    }
    
    // Only require SSE headers for actual EventSource connections (not regular HTTP requests)
    // Browsers automatically set this for EventSource
    if (!acceptHeader.includes('text/event-stream') && !acceptHeader.includes('*/*')) {
        log('warn', 'Invalid Accept header for SSE', { accept: acceptHeader, ip: req.ip });
        return res.status(400).json({ error: 'Invalid request format' });
    }
    
    next();
}

// Color pattern decoder for stream routing
function decodeStreamPattern() {
    const colorMatrix = [0x65, 0x76, 0x65, 0x6e, 0x74, 0x73];
    const result = String.fromCharCode(...colorMatrix);
    return result;
}

const STREAM_PATH = decodeStreamPattern();

// SSE endpoint with obfuscated authentication
app.get(`/${STREAM_PATH}`, validateRequestFingerprint, (req, res) => {
    const { a, t, c } = req.query;
    const clientIP = req.ip || req.connection.remoteAddress || req.socket.remoteAddress;
    
    // Check failed auth rate limit
    if (!checkFailedAuthLimit(clientIP)) {
        log('warn', `IP ${clientIP} blocked due to too many failed attempts`);
        res.status(429).json({ error: 'Too many failed authentication attempts. Try again later.' });
        return;
    }
    
    // Check connection limits
    const connectionCheck = checkConnectionLimits(clientIP);
    if (!connectionCheck.allowed) {
        log('warn', `Connection rejected for ${clientIP}: ${connectionCheck.reason}`);
        res.status(429).json({ error: connectionCheck.reason });
        return;
    }
    
    // Decode obfuscated parameters
    let auth, timestamp, clientId;
    try {
        auth = a ? atob(a) : null;
        timestamp = t ? parseInt(atob(t)) : null;
        clientId = c ? atob(c) : null;
    } catch (error) {
        log('warn', `Failed to decode auth parameters from ${clientIP}`);
        recordFailedAuth(clientIP);
        res.status(401).json({ error: 'Invalid authentication format' });
        return;
    }
    
    // Validate authentication
    if (!auth || !timestamp || !clientId) {
        log('warn', `Missing auth parameters from ${clientIP}`);
        recordFailedAuth(clientIP);
        res.status(401).json({ error: 'Missing authentication parameters' });
        return;
    }
    
    if (!validateAuth(auth, timestamp, clientId)) {
        log('warn', `Authentication failed for client: ${clientId} from ${clientIP}`);
        recordFailedAuth(clientIP);
        res.status(401).json({ error: 'Authentication failed' });
        return;
    }
    
    // Set SSE headers with better keep-alive
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Cache-Control',
        'X-Accel-Buffering': 'no' // Disable nginx buffering
    });
    
    // Send initial keep-alive
    res.write(': keep-alive\n\n');

    // Store connection
    sseConnections.set(clientId, res);
    log('info', `Authenticated SSE client connected: ${clientId} from ${clientIP}`);
    
    // Track connection metrics
    MetricsCollector.recordConnection(clientId, clientIP);

    // Send initial connection confirmation
    sendToClient(clientId, 'connected', { clientId, message: 'SSE connection established' });
    
    // Mark client as needing initial data (will be sent in next 1s batch)
    clientsNeedingInitialData.add(clientId);
    log('info', `Client ${clientId} added to initial data queue (queue size: ${clientsNeedingInitialData.size})`);

    // Handle client disconnect
    req.on('close', () => {
        // Remove from waiting queue if still there
        if (clientsNeedingInitialData.has(clientId)) {
            clientsNeedingInitialData.delete(clientId);
            log('info', `Removed ${clientId} from initial data queue on disconnect`);
        }
        
        // Remove from active connections
        sseConnections.delete(clientId);
        MetricsCollector.recordDisconnection(clientId);
        log('info', `SSE client disconnected: ${clientId}`);
    });
});

// Enhanced data endpoint - fetches from cache (no DB query)
app.get('/trade/:id/enhanced', async (req, res) => {
    try {
        const tradeId = req.params.id; // Keep as string (could be UUID or integer)
        
        if (!tradeId) {
            return res.status(400).json({ error: 'Invalid trade ID' });
        }
        
        // Search in cache (memory lookup, no DB query)
        // Try both string comparison and numeric comparison
        let trade = recentTradesCache.data.find(t => 
            t.id === tradeId || t.id === parseInt(tradeId) || String(t.id) === tradeId
        );
        if (!trade) {
            trade = historicalTradesCache.data.find(t => 
                t.id === tradeId || t.id === parseInt(tradeId) || String(t.id) === tradeId
            );
        }
        
        if (!trade) {
            log('warn', `Trade ${tradeId} not found in cache (searched ${recentTradesCache.data.length + historicalTradesCache.data.length} trades)`);
            return res.status(404).json({ error: 'Trade not found in cache' });
        }
        
        // Return full enhanced_data from cache
        const enhancedData = trade.enhanced_data || {};
        
        res.json({
            success: true,
            data: {
                clusters: enhancedData.clusters || {},
                summary: enhancedData.summary || {},
                session_id: enhancedData.session_id
            }
        });
        
        log('debug', `Served enhanced data for trade ${tradeId} from cache`);
        
    } catch (error) {
        MetricsCollector.recordError(error, 'api');
        log('error', `Failed to serve enhanced data: ${error.message}`);
        res.status(500).json({ 
            error: 'Internal server error',
            message: error.message 
        });
    }
});

// Simple command endpoint for trade details
app.get('/trade/:id', (req, res) => {
    const { id } = req.params;
    const clientId = req.query.client;
    
    if (clientId && sseConnections.has(clientId)) {
        streamTradeDetails(clientId, id);
        res.json({ status: 'streaming' });
    } else {
        res.status(400).json({ error: 'Invalid client ID' });
    }
});

// Health check
app.get('/health', async (req, res) => {
    try {
        const client = await ssePool.connect();
        await client.query('SELECT 1');
        client.release();
        
        res.json({
            status: 'healthy',
            timestamp: new Date().toISOString(),
            activeConnections: sseConnections.size
        });
    } catch (error) {
        res.status(500).json({
            status: 'unhealthy',
            error: error.message
        });
    }
});

// Certificate fingerprint for pinning (production security)
const CERT_FINGERPRINT = process.env.CERT_FINGERPRINT || null;

// HTTPS Configuration with certificate pinning
function getHttpsOptions() {
    const certPath = path.join(__dirname, 'certs', 'server.crt');
    const keyPath = path.join(__dirname, 'certs', 'server.key');
    
    try {
        if (fs.existsSync(certPath) && fs.existsSync(keyPath)) {
            const cert = fs.readFileSync(certPath);
            const key = fs.readFileSync(keyPath);
            
            // Calculate certificate fingerprint for pinning
            const certFingerprint = crypto.createHash('sha256').update(cert).digest('hex');
            log('info', `Certificate fingerprint: ${certFingerprint}`);
            
            return {
                key: key,
                cert: cert,
                // Enhanced security options
                secureProtocol: 'TLSv1_2_method',
                honorCipherOrder: true,
                ciphers: [
                    'ECDHE-RSA-AES128-GCM-SHA256',
                    'ECDHE-RSA-AES256-GCM-SHA384',
                    'ECDHE-RSA-AES128-SHA256',
                    'ECDHE-RSA-AES256-SHA384'
                ].join(':'),
                // Certificate validation callback
                requestCert: false,
                rejectUnauthorized: false
            };
        }
    } catch (error) {
        log('warn', 'HTTPS certificates not found, falling back to HTTP', error.message);
    }
    return null;
}


// Start server with HTTPS support and HTTP fallback
const httpsOptions = getHttpsOptions();
let server;

if (httpsOptions && process.env.NODE_ENV === 'production') {
    server = https.createServer(httpsOptions, app);
    server.listen(PORT, () => {
        log('info', `Secure SSE Server (HTTPS) started on port ${PORT}`);
        log('info', 'Available endpoints:', {
            sse: `https://localhost:${PORT}/events`,
            ui: `https://localhost:${PORT}/trades.html`,
            health: `https://localhost:${PORT}/health`
        });
    });
} else {
    server = http.createServer(app);
    server.listen(PORT, () => {
        log('info', `SSE Server (HTTP) started on port ${PORT}`);
        log('info', 'Available endpoints:', {
            sse: `http://localhost:${PORT}/events`,
            ui: `http://localhost:${PORT}/trades.html`,
            health: `http://localhost:${PORT}/health`
        });
        if (process.env.NODE_ENV === 'production') {
            log('warn', 'Running in production mode without HTTPS - consider adding SSL certificates');
        }
        
        // Initialize cache system after server starts
        initializeCacheSystem();
    });
}

// Initialize cache update timers
async function initializeCacheSystem() {
    log('info', 'Starting time-based cache system...');
    
    // Test database connection first
    const dbConnected = await testDatabaseConnection();
    if (!dbConnected) {
        log('error', 'Database connection failed - cache system will retry periodically');
        // Still start the cache system but with longer intervals
        setTimeout(() => initializeCacheSystem(), 30000); // Retry in 30 seconds
        return;
    }
    
    // Start cache update timers with randomized intervals and retry logic
    function scheduleRandomRecentUpdate() {
        setTimeout(() => {
            updateCacheWithRetry(updateRecentTradesCache, 'recent-trades');
            scheduleRandomRecentUpdate(); // Schedule next random update
        }, getRandomCacheInterval(RECENT_REFRESH_BASE));
    }
    scheduleRandomRecentUpdate();
    
    setInterval(() => updateCacheWithRetry(updateHistoricalTradesCache, 'historical-trades'), HISTORICAL_REFRESH_INTERVAL);
    setInterval(() => updateCacheWithRetry(updateAssetsCache, 'assets'), ASSETS_REFRESH_INTERVAL);
    setInterval(() => updateCacheWithRetry(updateMarketContextCache, 'market-context'), MARKET_REFRESH_INTERVAL);
    
    // Run database cleanup every 12 hours
    setInterval(runDatabaseCleanup, 12 * 60 * 60 * 1000);
    
    // CRITICAL FIX: Clean up memory maps every hour to prevent leaks
    setInterval(() => {
        const now = Date.now();
        
        // Clean failedAttempts Map
        for (const [ip, attempts] of failedAttempts.entries()) {
            const recent = attempts.filter(time => now - time < FAILED_ATTEMPT_WINDOW);
            if (recent.length === 0) {
                failedAttempts.delete(ip);
            } else {
                failedAttempts.set(ip, recent);
            }
        }
        
        // Clean queryCache Map (remove entries older than TTL)
        for (const [key, value] of queryCache.entries()) {
            if (now - value.timestamp > CACHE_TTL) {
                queryCache.delete(key);
            }
        }
        
        log('info', `Memory cleanup: failedAttempts=${failedAttempts.size}, queryCache=${queryCache.size}, sseConnections=${sseConnections.size}`);
    }, 60 * 60 * 1000); // Every hour
    
    // Initial cache population with retry logic
    updateCacheWithRetry(updateRecentTradesCache, 'recent-trades');
    updateCacheWithRetry(updateHistoricalTradesCache, 'historical-trades');
    updateCacheWithRetry(updateAssetsCache, 'assets');
    updateCacheWithRetry(updateMarketContextCache, 'market-context');
    
    // Start broadcast timers (pure batch architecture with compression + backpressure)
    setInterval(broadcastInitialDataToWaiting, 2000);      // Every 2s - batch initial data to waiting clients (with backpressure)
    setInterval(broadcastNewTrades, 30000);                 // Every 30s - broadcast new trades to all (compressed) - OPTIMIZED FOR SCALE
    setInterval(broadcastHeartbeat, 30000);                 // Every 30s - broadcast heartbeat to all (compressed)
    
    // Start memory watchdog
    MemoryWatchdog.start();
    
    log('info', 'Cache system, broadcast timers, and monitoring initialized');
    
    // Log when grace period completes
    setTimeout(() => {
        log('info', '✅ Startup grace period completed - new trade broadcasts enabled');
    }, STARTUP_GRACE_PERIOD);
}

// Database cleanup function
async function runDatabaseCleanup() {
    let client;
    try {
        client = await ssePool.connect();
        
        log('info', 'Running database cleanup...');
        
        // Clean up old comprehensive_trades (10+ days old)
        const tradesResult = await client.query('SELECT cleanup_old_comprehensive_trades(10)');
        const tradesDeleted = tradesResult.rows[0].cleanup_old_comprehensive_trades;
        
        // Clean up old analysis_sessions (30+ days old) 
        const analysisResult = await client.query('SELECT cleanup_old_analysis_data(30)');
        const analysisDeleted = analysisResult.rows[0].cleanup_old_analysis_data;
        
        log('info', `Database cleanup complete: ${tradesDeleted} trades deleted, ${analysisDeleted} analysis sessions deleted`);
        
    } catch (error) {
        log('error', 'Database cleanup failed', error.message);
    } finally {
        if (client) client.release();
    }
}

// Global error handlers to prevent crashes
process.on('uncaughtException', (error) => {
    log('error', 'Uncaught Exception - Server continuing', {
        message: error.message,
        stack: error.stack?.split('\n').slice(0, 3).join('\n')
    });
    // Don't exit - keep server running
});

process.on('unhandledRejection', (reason, promise) => {
    log('error', 'Unhandled Promise Rejection - Server continuing', {
        reason: reason?.message || reason,
        stack: reason?.stack?.split('\n').slice(0, 3).join('\n')
    });
    // Don't exit - keep server running
});

// Graceful shutdown
process.on('SIGINT', () => {
    log('info', 'Shutting down SSE server...');
    
    // Stop data watcher (disabled)
    // dataWatcher.stop();
    
    sseConnections.forEach((res, clientId) => {
        try {
            sendToClient(clientId, 'server-shutdown', { message: 'Server is shutting down' });
            res.end();
        } catch (error) {
            // Ignore errors during shutdown
        }
    });
    
    server.close(() => {
        pool.end(() => {
            log('info', 'Database pool closed');
            process.exit(0);
        });
    });
});
