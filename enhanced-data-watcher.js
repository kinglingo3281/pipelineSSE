import chokidar from 'chokidar';
import fs from 'fs';
import path from 'path';
import { Pool } from 'pg';

class EnhancedDataWatcher {
    constructor(dbPool, watchDirectory) {
        this.pool = dbPool;
        this.watchDirectory = watchDirectory;
        this.filePattern = /^([A-Z]+)_enhanced_analysis_(\d{8}_\d{6})\.json$/;
        this.watcher = null;
        this.processing = new Set(); // Track files being processed
        this.stats = {
            filesProcessed: 0,
            errors: 0,
            totalProcessingTime: 0
        };
        
        // Add connection error handling
        this.pool.on('error', (err) => {
            console.error('[Enhanced Data Watcher] Pool error:', err);
        });
    }

    start() {
        console.log(`[Enhanced Data Watcher] Starting on: ${this.watchDirectory}`);
        
        this.watcher = chokidar.watch(this.watchDirectory, {
            ignored: /^\./, // ignore dotfiles
            persistent: true,
            ignoreInitial: true,  // Process existing files on startup
            awaitWriteFinish: {
                stabilityThreshold: 2000,
                pollInterval: 100
            }
        });

        this.watcher
            .on('add', (filePath) => this.handleFileAdd(filePath))
            .on('change', (filePath) => this.handleFileChange(filePath))
            .on('error', (error) => console.error('[Enhanced Data Watcher] Error:', error))
            .on('ready', () => {
                console.log('[Enhanced Data Watcher] Ready for changes');
                // Process existing files in batches after watcher is ready
                this.processExistingFilesBatched();
            });

        // Log stats every 5 minutes
        setInterval(() => this.logStats(), 5 * 60 * 1000);
    }

    async processExistingFilesBatched() {
        try {
            console.log('[Enhanced Data Watcher] Starting batched processing of existing files...');
            
            const files = fs.readdirSync(this.watchDirectory)
                .filter(file => this.filePattern.test(file))
                .map(file => path.join(this.watchDirectory, file));
            
            console.log(`[Enhanced Data Watcher] Found ${files.length} existing files to process`);
            
            const batchSize = 25; // Process 25 files at a time
            const delayBetweenBatches = 500; // 0.5 second delay between batches
            
            for (let i = 0; i < files.length; i += batchSize) {
                const batch = files.slice(i, i + batchSize);
                // console.log(`[Enhanced Data Watcher] Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(files.length/batchSize)} (${batch.length} files)`);
                
                // Process batch in parallel
                await Promise.all(batch.map(filePath => {
                    const filename = path.basename(filePath);
                    const fileInfo = this.parseFilename(filename);
                    if (fileInfo) {
                        return this.processFile(filePath, fileInfo);
                    } else {
                        // console.log(`[Enhanced Data Watcher] Skipping invalid filename: ${filename}`);
                        return Promise.resolve();
                    }
                }));
                
                // Delay between batches to prevent overwhelming the database
                if (i + batchSize < files.length) {
                    await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
                }
            }
            
            // console.log('[Enhanced Data Watcher] Completed batched processing of existing files');
        } catch (error) {
            console.error('[Enhanced Data Watcher] Error in batched processing:', error);
        }
    }

    stop() {
        if (this.watcher) {
            this.watcher.close();
            // console.log('[Enhanced Data Watcher] Stopped');
        }
    }

    logStats() {
        // console.log(`[Enhanced Data Watcher] Stats: ${this.stats.filesProcessed} files processed, ${this.stats.errors} errors, avg time: ${Math.round(this.stats.totalProcessingTime / Math.max(this.stats.filesProcessed, 1))}ms`);
    }

    async handleFileAdd(filePath) {
        const filename = path.basename(filePath);
        const fileInfo = this.parseFilename(filename);
        
        // Skip files older than 5 minutes (prevents processing old files on startup)
        const fileAge = Date.now() - fileInfo.datetime.getTime();
        if (fileAge > 300000) {  // 5 minutes = 300,000ms
            return;
        }
        
        if (fileInfo && !this.processing.has(filePath)) {
            console.log(`[Enhanced Data Watcher] New file detected: ${filename}`);
            await this.processFile(filePath, fileInfo);
        }
    }

    async handleFileChange(filePath) {
        const filename = path.basename(filePath);
        const fileInfo = this.parseFilename(filename);
        
        // Skip files older than 5 minutes (prevents reprocessing old files)
        const fileAge = Date.now() - fileInfo.datetime.getTime();
        if (fileAge > 300000) {  // 5 minutes = 300,000ms
            return;
        }
        
        if (fileInfo && !this.processing.has(filePath)) {
            // console.log(`[Enhanced Data Watcher] File updated: ${filename}`);
            await this.processFile(filePath, fileInfo);
        }
    }

    parseFilename(filename) {
        const match = filename.match(this.filePattern);
        if (match) {
            const timestampStr = match[2];
            const year = timestampStr.substring(0, 4);
            const month = timestampStr.substring(4, 6);
            const day = timestampStr.substring(6, 8);
            const hour = timestampStr.substring(9, 11);
            const minute = timestampStr.substring(11, 13);
            const second = timestampStr.substring(13, 15);
            
            return {
                asset: match[1],
                timestamp: timestampStr,
                datetime: new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`),
                filename: filename
            };
        }
        return null;
    }

    async processFile(filePath, fileInfo) {
        this.processing.add(filePath);
        const startTime = Date.now();
        
        try {
            // console.log(`[Enhanced Data Watcher] Processing ${fileInfo.asset} analysis from ${fileInfo.datetime.toISOString()}`);
            
            // Read and validate file
            const fileStats = fs.statSync(filePath);
            const jsonData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
            
            // Validate basic structure
            if (!this.validateJsonStructure(jsonData)) {
                throw new Error('Invalid JSON structure');
            }
            
            // Process data into database
            const sessionId = await this.insertEnhancedData(jsonData, fileInfo, filePath, fileStats.size);
            
            const processingTime = Date.now() - startTime;
            this.stats.totalProcessingTime += processingTime;
            
            console.log(`[Enhanced Data Watcher] Successfully processed ${fileInfo.asset} (session: ${sessionId.substring(0, 8)}...) in ${processingTime}ms`);
            
        } catch (error) {
            this.stats.errors++;
            console.error(`[Enhanced Data Watcher] Error processing ${filePath}:`, error.message);
            console.error('Stack trace:', error.stack);
        } finally {
            this.processing.delete(filePath);
        }
    }
    validateJsonStructure(data) {
        // Basic validation of required fields
        return data && 
               typeof data.current_price === 'number' &&
               data.clusters &&
               (data.clusters.long_clusters || data.clusters.short_clusters);
    }

    async insertEnhancedData(data, fileInfo, filePath, fileSize) {
        let client;
        const startTime = Date.now();
        
        try {
            client = await this.pool.connect();
            
            // Add connection timeout and retry logic
            client.query('SELECT 1').catch(() => {
                // console.log('[Enhanced Data Watcher] Connection test failed, retrying...');
                throw new Error('Connection failed');
            });
            
            await client.query('BEGIN');
            
            // NEW: Insert into comprehensive_trades table (contains EVERYTHING)
            await this.insertComprehensiveTrades(client, data, fileInfo, filePath);
            
            // LEGACY: Keep minimal legacy tables for backward compatibility only
            const sessionId = await this.insertAnalysisSession(client, data, fileInfo, filePath);
            
            // Count clusters for stats only
            let clusterStats = { clustersProcessed: 0, positionsProcessed: 0 };
            if (data.clusters) {
                const longClusters = data.clusters.long_clusters || [];
                const shortClusters = data.clusters.short_clusters || [];
                clusterStats.clustersProcessed = longClusters.length + shortClusters.length;
                longClusters.forEach(cluster => clusterStats.positionsProcessed += cluster.positions?.length || 0);
                shortClusters.forEach(cluster => clusterStats.positionsProcessed += cluster.positions?.length || 0);
                // console.log(`Processed ${clusterStats.clustersProcessed} clusters, ${clusterStats.positionsProcessed} positions`);
            }
            
            // Insert processing stats
            const stats = {
                clusters_processed: clusterStats.clustersProcessed,
                positions_processed: clusterStats.positionsProcessed,
                processing_time_ms: Date.now() - startTime,
                file_size_bytes: fs.statSync(filePath).size
            };
            await this.insertProcessingStats(client, sessionId, stats);
            
            await client.query('COMMIT');
            return sessionId;
            
        } catch (error) {
            if (client) {
                try {
                    await client.query('ROLLBACK');
                } catch (rollbackError) {
                    console.error('Rollback failed:', rollbackError.message);
                }
            }
            throw error;
        } finally {
            if (client) client.release();
        }
    }

    async insertAnalysisSession(client, data, fileInfo, filePath) {
        const query = `
            INSERT INTO analysis_sessions (
                asset, analysis_timestamp, file_timestamp, current_price, file_path,
                long_count, short_count, total_count, has_enhanced_risk, risk_analysis_version,
                processing_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (asset, analysis_timestamp) 
            DO UPDATE SET 
                current_price = EXCLUDED.current_price,
                file_path = EXCLUDED.file_path,
                processing_status = EXCLUDED.processing_status,
                long_count = EXCLUDED.long_count,
                short_count = EXCLUDED.short_count,
                total_count = EXCLUDED.total_count,
                updated_at = NOW()
            RETURNING id
        `;
        
        const metadata = data.clusters?.metadata || {};
        const values = [
            fileInfo.asset,
            fileInfo.datetime,
            new Date(),
            data.current_price,
            filePath,
            metadata.long_count || (data.clusters?.long_clusters?.length || 0),
            metadata.short_count || (data.clusters?.short_clusters?.length || 0),
            metadata.total_count || ((data.clusters?.long_clusters?.length || 0) + (data.clusters?.short_clusters?.length || 0)),
            metadata.has_enhanced_risk || true,
            metadata.risk_analysis_version || '1.0',
            'completed'
        ];
        
        const result = await client.query(query, values);
        return result.rows[0].id;
    }

    // Safe numeric sanitization to prevent PostgreSQL overflow
    sanitizeNumericValue(value, fieldName, maxValue = 99999999999999) {
        if (value === null || value === undefined) return null;
        
        const numValue = parseFloat(value);
        if (isNaN(numValue)) return null;
        
        // Only cap values that truly exceed PostgreSQL limits
        if (Math.abs(numValue) > maxValue) {
            console.warn(`[SANITIZE] ${fieldName}: ${numValue} exceeds limit, capped to ${Math.sign(numValue) * maxValue}`);
            return Math.sign(numValue) * maxValue;
        }
        
        return numValue;
    }

    async insertClusterData(client, sessionId, clustersData) {
        if (!clustersData) return { clustersProcessed: 0, positionsProcessed: 0 };
        
        const longClusters = clustersData.long_clusters || [];
        const shortClusters = clustersData.short_clusters || [];
        let clustersProcessed = 0;
        let positionsProcessed = 0;
        
        for (const cluster of [...longClusters, ...shortClusters]) {
            const clusterQuery = `
                INSERT INTO cluster_data_enhanced (
                    session_id, asset, direction, center_price, total_size,
                    price_range_min, price_range_max, price_distance_pct, relative_size,
                    tightness, trigger_probability, position_count, orderbook_risk,
                    liquidity_risk, composite_risk, dynamic_stop_price,
                    dynamic_stop_percentage, stop_risk_based, stop_explanation
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                RETURNING id
            `;
            
            const clusterValues = [
                sessionId,
                clustersData.asset || cluster.asset,
                cluster.direction,
                this.sanitizeNumericValue(cluster.center_price, 'center_price'),
                this.sanitizeNumericValue(cluster.total_size, 'total_size'),
                cluster.price_range ? this.sanitizeNumericValue(cluster.price_range[0], 'price_range_min') : null,
                cluster.price_range ? this.sanitizeNumericValue(cluster.price_range[1], 'price_range_max') : null,
                this.sanitizeNumericValue(cluster.price_distance_pct, 'price_distance_pct'), // No artificial cap for percentages
                this.sanitizeNumericValue(cluster.relative_size, 'relative_size'),
                this.sanitizeNumericValue(cluster.tightness, 'tightness'),
                this.sanitizeNumericValue(cluster.trigger_probability, 'trigger_probability'),
                cluster.position_count || (cluster.positions ? cluster.positions.length : 0),
                this.sanitizeNumericValue(cluster.orderbook_risk, 'orderbook_risk'),
                this.sanitizeNumericValue(cluster.liquidity_risk, 'liquidity_risk'),
                this.sanitizeNumericValue(cluster.composite_risk, 'composite_risk'),
                this.sanitizeNumericValue(cluster.dynamic_stop_loss?.price, 'dynamic_stop_price'),
                this.sanitizeNumericValue(cluster.dynamic_stop_loss?.percentage, 'dynamic_stop_percentage'),
                cluster.dynamic_stop_loss?.risk_based,
                cluster.dynamic_stop_loss?.explanation
            ];
            
            const clusterResult = await client.query(clusterQuery, clusterValues);
            const clusterId = clusterResult.rows[0].id;
            clustersProcessed++;
            
            // Insert positions for this cluster
            if (cluster.positions && Array.isArray(cluster.positions)) {
                positionsProcessed += await this.insertClusterPositions(client, clusterId, cluster.positions);
            } else if (cluster.positions) {
                console.warn(`Cluster positions is not an array: ${typeof cluster.positions}, value:`, JSON.stringify(cluster.positions).substring(0, 100));
            }
        }
        
        return { clustersProcessed, positionsProcessed };
    }

    async insertClusterPositions(client, clusterId, positions) {
        let insertedCount = 0;
        
        // Ensure positions is iterable
        if (!Array.isArray(positions)) {
            console.warn(`Positions is not an array (type: ${typeof positions}), skipping cluster positions insertion`);
            return 0;
        }
        
        // Filter out invalid positions
        const validPositions = positions.filter(pos => 
            pos && typeof pos === 'object' && 
            (pos.price !== null && pos.price !== undefined) &&
            (pos.size !== null && pos.size !== undefined)
        );
        
        for (const position of validPositions) {
            const positionQuery = `
                INSERT INTO cluster_positions_detailed (
                    cluster_id, price, size, usd_size, step, trigger_probability,
                    source, trader_address, entry_price
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            `;
            
            const positionValues = [
                clusterId,
                this.sanitizeNumericValue(position.price, 'position_price'),
                this.sanitizeNumericValue(position.size, 'position_size'),
                this.sanitizeNumericValue(position.usd_size, 'position_usd_size'),
                position.step,
                this.sanitizeNumericValue(position.trigger_probability, 'position_trigger_probability'),
                position.source || 'unknown',
                position.trader,
                this.sanitizeNumericValue(position.entry_price, 'position_entry_price')
            ];
            
            await client.query(positionQuery, positionValues);
            insertedCount++;
        }
        
        return insertedCount;
    }

    async insertCascadeProbabilities(client, sessionId, cascadeData) {
        if (!cascadeData) return;
        
        const query = `
            INSERT INTO cascade_probabilities (
                session_id, asset, long_probability, long_risk_level,
                long_critical_threshold, long_expected_impact, long_confidence,
                short_probability, short_risk_level, short_critical_threshold,
                short_expected_impact, short_confidence, dominant_direction,
                overall_risk_level
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        `;
        
        const values = [
            sessionId,
            cascadeData.asset,
            this.sanitizeNumericValue(cascadeData.long_cascade?.probability, 'long_probability'),
            cascadeData.long_cascade?.risk_level,
            this.sanitizeNumericValue(cascadeData.long_cascade?.critical_threshold, 'long_critical_threshold'),
            this.sanitizeNumericValue(cascadeData.long_cascade?.expected_price_impact, 'long_expected_impact'),
            this.sanitizeNumericValue(cascadeData.long_cascade?.confidence, 'long_confidence'),
            this.sanitizeNumericValue(cascadeData.short_cascade?.probability, 'short_probability'),
            cascadeData.short_cascade?.risk_level,
            this.sanitizeNumericValue(cascadeData.short_cascade?.critical_threshold, 'short_critical_threshold'),
            this.sanitizeNumericValue(cascadeData.short_cascade?.expected_price_impact, 'short_expected_impact'),
            this.sanitizeNumericValue(cascadeData.short_cascade?.confidence, 'short_confidence'),
            cascadeData.dominant_direction,
            cascadeData.risk_level
        ];
        
        await client.query(query, values);
    }

    async insertPriceTargets(client, sessionId, targetsData, targetType) {
        if (!targetsData) return;
        
        const longTargets = targetsData.long_targets || [];
        const shortTargets = targetsData.short_targets || [];
        const ranges = targetsData.ranges || [];
        
        // Process individual targets
        for (const target of longTargets) {
            target.direction = 'long';
            await this.insertSinglePriceTarget(client, sessionId, target, targetType, targetsData.asset);
        }
        for (const target of shortTargets) {
            target.direction = 'short';
            await this.insertSinglePriceTarget(client, sessionId, target, targetType, targetsData.asset);
        }
        
        // Process ranges (which may have additional data)
        for (const range of ranges) {
            await this.insertSinglePriceTarget(client, sessionId, range, targetType, targetsData.asset);
        }
    }

    async insertSinglePriceTarget(client, sessionId, target, targetType, asset) {
        const query = `
            INSERT INTO price_targets (
                session_id, asset, direction, target_type, entry_price,
                target_price, take_profit, stop_loss, size, trigger_probability,
                confidence, risk_reward, priority_score, tp_hours, sl_hours,
                tp_range, sl_range, duration_confidence, rationale,
                original_take_profit, trigger_weight, risk_reward_weight,
                confidence_weight, liquidity_weight, bonus
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
        `;
        
        const priorityFactors = target.priority_factors || {};
        
        const values = [
            sessionId,
            asset || target.asset,
            target.direction || 'long',
            targetType,
            this.sanitizeNumericValue(target.entry_price || target.entry, 'entry_price'),
            this.sanitizeNumericValue(target.target_price || target.take_profit || data.current_price, 'target_price'),
            this.sanitizeNumericValue(target.take_profit, 'take_profit'),
            this.sanitizeNumericValue(target.stop_loss, 'stop_loss'),
            this.sanitizeNumericValue(target.size, 'target_size'),
            this.sanitizeNumericValue(target.trigger_probability, 'target_trigger_probability'),
            this.sanitizeNumericValue(target.confidence, 'target_confidence'),
            this.sanitizeNumericValue(target.risk_reward, 'risk_reward'),
            this.sanitizeNumericValue(target.priority_score, 'priority_score'),
            this.sanitizeNumericValue(target.tp_hours, 'tp_hours'),
            this.sanitizeNumericValue(target.sl_hours, 'sl_hours'),
            this.sanitizeNumericValue(target.tp_range, 'tp_range'),
            this.sanitizeNumericValue(target.sl_range, 'sl_range'),
            this.sanitizeNumericValue(target.duration_confidence, 'duration_confidence'),
            target.rationale,
            this.sanitizeNumericValue(target.original_take_profit, 'original_take_profit'),
            this.sanitizeNumericValue(priorityFactors.trigger_weight, 'trigger_weight'),
            this.sanitizeNumericValue(priorityFactors.risk_reward_weight, 'risk_reward_weight'),
            this.sanitizeNumericValue(priorityFactors.confidence_weight, 'confidence_weight'),
            this.sanitizeNumericValue(priorityFactors.liquidity_weight, 'liquidity_weight'),
            this.sanitizeNumericValue(priorityFactors.bonus, 'priority_bonus')
        ];
        
        await client.query(query, values);
    }

    async insertMarketContext(client, sessionId, marketData) {
        if (!marketData) return;
        
        // Normalize enum values to lowercase to match database constraints
        const normalizeEnum = (value) => {
            if (!value) return null;
            return value.toString().toLowerCase();
        };
        
        const query = `
            INSERT INTO market_context (
                session_id, asset, volatility_level, volatility_percentile,
                atr_percent, recent_range_percent, trend_direction, trend_strength,
                in_trend, ema_alignment, support_levels, resistance_levels,
                closest_support, closest_resistance, market_bias, bias_strength,
                price_in_range, volatility_classification, volatility_ratio,
                baseline_atr, regime_changed, previous_regime
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        `;
        
        const values = [
            sessionId,
            marketData.asset,
            normalizeEnum(marketData.volatility?.level),
            this.sanitizeNumericValue(marketData.volatility?.percentile, 'volatility_percentile'),
            this.sanitizeNumericValue(marketData.volatility?.atr_percent, 'atr_percent'),
            this.sanitizeNumericValue(marketData.volatility?.recent_range_percent, 'recent_range_percent'),
            normalizeEnum(marketData.trend?.direction),
            this.sanitizeNumericValue(marketData.trend?.strength, 'trend_strength'),
            marketData.trend?.in_trend,
            normalizeEnum(marketData.trend?.ema_alignment),
            JSON.stringify(marketData.support_resistance?.support_levels || []),
            JSON.stringify(marketData.support_resistance?.resistance_levels || []),
            this.sanitizeNumericValue(marketData.support_resistance?.closest_support, 'closest_support'),
            this.sanitizeNumericValue(marketData.support_resistance?.closest_resistance, 'closest_resistance'),
            normalizeEnum(marketData.market_bias?.bias),
            this.sanitizeNumericValue(marketData.market_bias?.strength, 'bias_strength'),
            marketData.market_bias?.price_in_range,
            normalizeEnum(marketData.volatility_regime?.classification),
            this.sanitizeNumericValue(marketData.volatility_regime?.ratio, 'volatility_ratio'),
            this.sanitizeNumericValue(marketData.volatility_regime?.baseline_atr, 'baseline_atr'),
            marketData.volatility_regime?.regime_changed,
            normalizeEnum(marketData.volatility_regime?.previous_regime)
        ];
        
        await client.query(query, values);
    }

    async insertSummaryAnalytics(client, sessionId, summaryData) {
        if (!summaryData) return;
        
        const query = `
            INSERT INTO summary_analytics (
                session_id, asset, dominant_bias, dominant_cascade_direction,
                directional_strength, risk_assessment, range_consistency,
                long_imbalance, short_imbalance, long_cascade_probability,
                short_cascade_probability, overall_cascade_probability,
                long_quality_score, short_quality_score, primary_direction,
                primary_action, primary_confidence, primary_reasoning
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        `;
        
        const primaryRec = summaryData.primary_recommendation || {};
        
        const values = [
            sessionId,
            summaryData.market_context?.asset || summaryData.asset,
            summaryData.dominant_bias,
            summaryData.dominant_cascade_direction,
            this.sanitizeNumericValue(summaryData.directional_strength, 'directional_strength'),
            summaryData.risk_assessment,
            this.sanitizeNumericValue(summaryData.range_consistency, 'range_consistency'),
            this.sanitizeNumericValue(summaryData.long_imbalance, 'long_imbalance'),
            this.sanitizeNumericValue(summaryData.short_imbalance, 'short_imbalance'),
            this.sanitizeNumericValue(summaryData.long_cascade_probability, 'long_cascade_probability'),
            this.sanitizeNumericValue(summaryData.short_cascade_probability, 'short_cascade_probability'),
            this.sanitizeNumericValue(summaryData.overall_cascade_probability, 'overall_cascade_probability'),
            this.sanitizeNumericValue(summaryData.long_quality_score, 'long_quality_score'),
            this.sanitizeNumericValue(summaryData.short_quality_score, 'short_quality_score'),
            primaryRec.direction,
            primaryRec.action,
            this.sanitizeNumericValue(primaryRec.confidence, 'primary_confidence'),
            primaryRec.reasoning
        ];
        
        await client.query(query, values);
    }

    async insertTradeRanges(client, sessionId, rangesData, rangeType, asset) {
        if (!rangesData || !Array.isArray(rangesData)) return;
        
        for (const range of rangesData) {
            const query = `
                INSERT INTO trade_ranges (
                    session_id, asset, direction, range_type, entry_price,
                    take_profit, stop_loss, risk_reward, confidence,
                    trigger_probability, tp_hours, sl_hours, duration_confidence,
                    priority_score, rationale, cascade_probability, risk_level
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            `;
            
            const values = [
                sessionId,
                asset,
                range.direction,
                rangeType,
                this.sanitizeNumericValue(range.entry || range.entry_price, 'entry_price'),
                this.sanitizeNumericValue(range.take_profit, 'take_profit'),
                this.sanitizeNumericValue(range.stop_loss, 'stop_loss'),
                this.sanitizeNumericValue(range.risk_reward, 'risk_reward'),
                this.sanitizeNumericValue(range.confidence, 'confidence'),
                this.sanitizeNumericValue(range.trigger_probability, 'trigger_probability'),
                this.sanitizeNumericValue(range.tp_hours, 'tp_hours'),
                this.sanitizeNumericValue(range.sl_hours, 'sl_hours'),
                this.sanitizeNumericValue(range.duration_confidence, 'duration_confidence'),
                this.sanitizeNumericValue(range.priority_score, 'priority_score'),
                range.rationale,
                this.sanitizeNumericValue(range.cascade_probability, 'cascade_probability'),
                range.risk_level?.toLowerCase()
            ];
            
            await client.query(query, values);
        }
    }

    async insertOrderbookSnapshot(client, sessionId, orderbookData) {
        if (!orderbookData) return;
        
        const query = `
            INSERT INTO orderbook_snapshots (
                session_id, asset, current_price, bid_liquidity,
                ask_liquidity, long_risk_zones, short_risk_zones, high_risk_zones
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `;
        
        const values = [
            sessionId,
            orderbookData.asset,
            this.sanitizeNumericValue(orderbookData.current_price, 'orderbook_current_price'),
            JSON.stringify(orderbookData.bid_liquidity),
            JSON.stringify(orderbookData.ask_liquidity),
            JSON.stringify(orderbookData.long_risk),
            JSON.stringify(orderbookData.short_risk),
            JSON.stringify(orderbookData.high_risk_zones)
        ];
        
        await client.query(query, values);
    }

    // Create trade records from enhanced analysis data for SSE compatibility
    async createTradeRecordsFromAnalysis(client, sessionId, data, fileInfo) {
        if (!data.price_targets && !data.ta_price_targets) return;

        const { v4: uuidv4 } = require('uuid');
        
        // Process enhanced price targets
        if (data.price_targets) {
            await this.createTradesFromTargets(client, sessionId, data.price_targets, 'standard', data, fileInfo);
        }
        
        // Process TA-based targets - use ranges for TP/SL hours
        if (data.ta_price_targets) {
            // Process ranges separately by direction
            const ranges = data.ta_price_targets.ranges || [];
            const longRanges = ranges.filter(r => r.direction === 'long');
            const shortRanges = ranges.filter(r => r.direction === 'short');
            
            const taTargets = {
                ...data.ta_price_targets,
                short_targets: [...(data.ta_price_targets.short_targets || []), ...shortRanges],
                long_targets: [...(data.ta_price_targets.long_targets || []), ...longRanges]
            };
            await this.createTradesFromTargets(client, sessionId, taTargets, 'ta_based', data, fileInfo);
        }
    }

    async createTradesFromTargets(client, sessionId, targets, targetType, data, fileInfo) {
        const { v4: uuidv4 } = require('uuid');
        
        const longTargets = targets.long_targets || [];
        const shortTargets = targets.short_targets || [];
        
        // Create trades from long targets
        for (const target of longTargets) {
            await this.insertSingleTrade(client, sessionId, target, 'long', targetType, data, fileInfo);
        }
        
        // Create trades from short targets  
        for (const target of shortTargets) {
            await this.insertSingleTrade(client, sessionId, target, 'short', targetType, data, fileInfo);
        }
    }

    async insertSingleTrade(client, sessionId, target, direction, targetType, data, fileInfo) {
        const { v4: uuidv4 } = require('uuid');
        
        const tradeId = uuidv4();
        const query = `
            INSERT INTO trades (
                id, asset, direction, entry_price, target_price, stop_price,
                current_price, confidence, risk_reward, tp_hours, sl_hours,
                tp_range, sl_range, duration_confidence, signal_type, title,
                source_file, file_timestamp, raw_data, status, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
        `;
        
        const values = [
            tradeId,
            data.asset || fileInfo.asset,
            direction,
            this.sanitizeNumericValue(target.entry_price || target.entry, 'entry_price'),
            this.sanitizeNumericValue(target.target_price || target.take_profit || data.current_price, 'target_price'),
            this.sanitizeNumericValue(target.stop_loss, 'stop_price'),
            this.sanitizeNumericValue(data.current_price, 'current_price'),
            this.sanitizeNumericValue(target.confidence, 'confidence'),
            this.sanitizeNumericValue(target.risk_reward, 'risk_reward'),
            this.sanitizeNumericValue(target.tp_hours, 'tp_hours'),
            this.sanitizeNumericValue(target.sl_hours, 'sl_hours'),
            this.sanitizeNumericValue(target.tp_range, 'tp_range'),
            this.sanitizeNumericValue(target.sl_range, 'sl_range'),
            this.sanitizeNumericValue(target.duration_confidence, 'duration_confidence'),
            targetType,
            `${targetType} ${direction} trade for ${data.asset || fileInfo.asset}`,
            fileInfo.filename,
            fileInfo.datetime,
            JSON.stringify({ target, analysis_session_id: sessionId }),
            'active',
            fileInfo.datetime
        ];
        
        await client.query(query, values);
    }

    async insertProcessingStats(client, sessionId, stats) {
        const query = `
            INSERT INTO processing_stats (
                session_id, file_size_bytes, processing_duration_ms,
                clusters_processed, positions_processed, data_completeness_score,
                validation_errors, validation_warnings
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `;
        
        const values = [
            sessionId,
            stats.fileSize,
            stats.processingTime,
            stats.clustersProcessed,
            stats.positionsProcessed,
            95.0, // Default completeness score
            0, // No validation errors for now
            0  // No validation warnings for now
        ];
        
        await client.query(query, values);
    }

    // NEW: Insert into comprehensive_trades table - stores EVERYTHING in one table
    async insertComprehensiveTrades(client, data, fileInfo, filePath) {
        // Count ALL positions across ALL clusters
        let totalPositions = 0;
        let longClusterCount = 0;
        let shortClusterCount = 0;
        
        if (data.clusters?.long_clusters) {
            longClusterCount = data.clusters.long_clusters.length;
            data.clusters.long_clusters.forEach(cluster => {
                totalPositions += cluster.positions?.length || 0;
            });
        }
        if (data.clusters?.short_clusters) {
            shortClusterCount = data.clusters.short_clusters.length;
            data.clusters.short_clusters.forEach(cluster => {
                totalPositions += cluster.positions?.length || 0;
            });
        }
        
        // Extract targets and create trades
        const allTargets = [
            ...(data.price_targets?.long_targets || []).map(t => ({...t, direction: 'long', type: 'standard'})),
            ...(data.price_targets?.short_targets || []).map(t => ({...t, direction: 'short', type: 'standard'})),
            ...(data.ta_price_targets?.long_targets || []).map(t => ({...t, direction: 'long', type: 'ta_based'})),
            ...(data.ta_price_targets?.short_targets || []).map(t => ({...t, direction: 'short', type: 'ta_based'})),
            ...(data.ta_price_targets?.ranges || []).map(t => ({...t, type: 'ta_range'}))
        ];
        
        // If no targets, create one default entry to preserve the analysis
        if (allTargets.length === 0) {
            allTargets.push({
                direction: data.summary?.dominant_bias || 'long',
                type: 'analysis_only',
                entry_price: data.current_price,
                confidence: 0.5
            });
        }
        
        for (const target of allTargets) {
            const query = `
                INSERT INTO comprehensive_trades (
                    asset, direction, entry_price, target_price, stop_price, current_price,
                    confidence, risk_reward, signal_type, tp_hours, sl_hours, tp_range, sl_range, duration_confidence,
                    enhanced_data,
                    total_clusters, total_positions, long_cluster_count, short_cluster_count,
                    dominant_bias, dominant_cascade_direction, risk_assessment, range_consistency, directional_strength,
                    long_imbalance, short_imbalance, long_quality_score, short_quality_score,
                    long_cascade_probability, short_cascade_probability, overall_cascade_probability,
                    volatility_level, volatility_percentile, atr_percent, recent_range_percent,
                    trend_direction, trend_strength, trend_in_trend, ema_alignment,
                    market_bias, market_bias_strength, price_in_range,
                    support_levels, resistance_levels, closest_support, closest_resistance,
                    volatility_regime_classification, volatility_regime_ratio, regime_changed,
                    long_target_count, short_target_count, long_average_probability, short_average_probability,
                    long_average_risk_reward, short_average_risk_reward, long_average_confidence, short_average_confidence,
                    is_ta_based, priority_score, has_orderbook_data, bid_liquidity_count, ask_liquidity_count,
                    source_file, file_timestamp, analysis_timestamp
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65
                )
            `;
            
            const marketContext = data.summary?.market_context || data.market_context || {};
            const summary = data.summary || {};
            const priceTargetsSummary = data.price_targets?.summary || {};
            
            const values = [
                fileInfo.asset,                                                                    // $1
                target.direction,                                                                  // $2
                this.sanitizeNumericValue(target.entry_price || target.entry),                   // $3
                this.sanitizeNumericValue(target.target_price || target.take_profit),            // $4
                this.sanitizeNumericValue(target.stop_loss),                                      // $5
                this.sanitizeNumericValue(data.current_price),                                    // $6
                this.sanitizeNumericValue(target.confidence),                                     // $7
                this.sanitizeNumericValue(target.risk_reward),                                    // $8
                target.type,                                                                       // $9
                this.sanitizeNumericValue(target.tp_hours),                                       // $10
                this.sanitizeNumericValue(target.sl_hours),                                       // $11
                target.tp_range,                                                                   // $12
                target.sl_range,                                                                   // $13
                this.sanitizeNumericValue(target.duration_confidence),                           // $14
                JSON.stringify(data), // COMPLETE original JSON preserved                         // $15
                (data.clusters?.metadata?.total_count || (longClusterCount + shortClusterCount)), // $16
                totalPositions,                                                                    // $17
                longClusterCount,                                                                  // $18
                shortClusterCount,                                                                 // $19
                summary.dominant_bias,                                                            // $20
                summary.dominant_cascade_direction,                                               // $21
                summary.risk_assessment,                                                          // $22
                this.sanitizeNumericValue(summary.range_consistency),                            // $23
                this.sanitizeNumericValue(summary.directional_strength),                         // $24
                this.sanitizeNumericValue(summary.long_imbalance),                               // $25
                this.sanitizeNumericValue(summary.short_imbalance),                              // $26
                this.sanitizeNumericValue(summary.long_quality_score),                           // $27
                this.sanitizeNumericValue(summary.short_quality_score),                          // $28
                this.sanitizeNumericValue(data.cascade_probabilities?.long_cascade?.probability), // $29
                this.sanitizeNumericValue(data.cascade_probabilities?.short_cascade?.probability), // $30
                this.sanitizeNumericValue(summary.overall_cascade_probability),                  // $31
                marketContext.volatility?.level,                                                  // $32
                this.sanitizeNumericValue(marketContext.volatility?.percentile),                 // $33
                this.sanitizeNumericValue(marketContext.volatility?.atr_percent),                // $34 - ATR% field
                this.sanitizeNumericValue(marketContext.volatility?.recent_range_percent),       // $35
                marketContext.trend?.direction,                                                   // $36
                this.sanitizeNumericValue(marketContext.trend?.strength),                        // $37
                marketContext.trend?.in_trend,                                                    // $38
                marketContext.trend?.ema_alignment,                                               // $39
                marketContext.market_bias?.bias,                                                  // $40
                this.sanitizeNumericValue(marketContext.market_bias?.bias_strength),             // $41
                marketContext.market_bias?.price_in_range,                                        // $42
                JSON.stringify(marketContext.support_resistance?.support_levels || []),          // $43
                JSON.stringify(marketContext.support_resistance?.resistance_levels || []),       // $44
                this.sanitizeNumericValue(marketContext.support_resistance?.closest_support),    // $45
                this.sanitizeNumericValue(marketContext.support_resistance?.closest_resistance), // $46
                marketContext.volatility_regime?.classification,                                  // $47
                this.sanitizeNumericValue(marketContext.volatility_regime?.ratio),               // $48
                marketContext.volatility_regime?.regime_changed,                                  // $49
                priceTargetsSummary.long_count || 0,                                             // $50
                priceTargetsSummary.short_count || 0,                                            // $51
                this.sanitizeNumericValue(priceTargetsSummary.long_average_probability),         // $52
                this.sanitizeNumericValue(priceTargetsSummary.short_average_probability),        // $53
                this.sanitizeNumericValue(priceTargetsSummary.long_average_risk_reward),         // $54
                this.sanitizeNumericValue(priceTargetsSummary.short_average_risk_reward),        // $55
                this.sanitizeNumericValue(priceTargetsSummary.long_average_confidence),          // $56
                this.sanitizeNumericValue(priceTargetsSummary.short_average_confidence),         // $57
                target.type === 'ta_based' || target.type === 'ta_range',                        // $58
                this.sanitizeNumericValue(target.priority_score),                                // $59
                !!data.orderbook_analysis,                                                        // $60
                data.orderbook_analysis ? Object.keys(data.orderbook_analysis.bid_liquidity || {}).length : 0, // $61
                data.orderbook_analysis ? Object.keys(data.orderbook_analysis.ask_liquidity || {}).length : 0, // $62
                fileInfo.filename,                                                                // $63
                fileInfo.datetime,                                                                // $64
                fileInfo.datetime                                                                 // $65
            ];
            
            await client.query(query, values);
        }
        
        // console.log(`✅ Inserted ${allTargets.length} comprehensive trade records with complete enhanced data`);
    }
}

export default EnhancedDataWatcher;
