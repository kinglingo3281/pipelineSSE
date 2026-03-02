// Setup script to create SSE database schema using Node.js
import 'dotenv/config';
import { Pool } from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new Pool({
    connectionString: process.env.DATABASE_URL || "postgresql://postgres:password@localhost:5432/sse_dashboard"
});

async function setupDatabase() {
    const client = await pool.connect();
    
    try {
        // console.log('🚀 Setting up SSE Database Schema...\n');
        
        // Read and execute enhanced schema first
        const enhancedSchemaPath = path.join(__dirname, 'database', 'enhanced_schema.sql');
        if (fs.existsSync(enhancedSchemaPath)) {
            // console.log('📝 Executing Enhanced Trading Data schema...\n');
            const enhancedSchemaSql = fs.readFileSync(enhancedSchemaPath, 'utf8');
            
            try {
                await client.query(enhancedSchemaSql);
                // console.log('✅ Enhanced schema executed successfully');
            } catch (error) {
                if (error.message.includes('already exists')) {
                    // console.log('ℹ️  Some enhanced objects already exist, continuing...');
                } else {
                    throw error;
                }
            }
        }
        
        // Read and execute comprehensive_trades schema
        const comprehensiveTradesPath = path.join(__dirname, 'database', 'comprehensive_trades_schema_FULL.sql');
        if (fs.existsSync(comprehensiveTradesPath)) {
            // console.log('📝 Executing Comprehensive Trades schema...\n');
            const comprehensiveTradesSql = fs.readFileSync(comprehensiveTradesPath, 'utf8');
            
            try {
                await client.query(comprehensiveTradesSql);
                // console.log('✅ Comprehensive Trades schema executed successfully');
            } catch (error) {
                if (error.message.includes('already exists')) {
                    // console.log('ℹ️  Comprehensive Trades table already exists, continuing...');
                } else {
                    throw error;
                }
            }
        }
        
        // Read and execute original SSE schema
        const schemaPath = path.join(__dirname, 'database', 'sse_schema.sql');
        if (fs.existsSync(schemaPath)) {
            // console.log('📝 Executing SSE schema...\n');
            const schemaSql = fs.readFileSync(schemaPath, 'utf8');
            
            try {
                await client.query(schemaSql);
                // console.log('✅ SSE schema executed successfully');
            } catch (error) {
                if (error.message.includes('already exists')) {
                    // console.log('ℹ️  Some SSE objects already exist, continuing...');
                } else {
                    console.warn('⚠️  SSE schema warning:', error.message);
                }
            }
        }
        
        // console.log('\n🎉 SSE Database schema setup complete!\n');
        
        // Verify the setup
        // console.log('🔍 Verifying setup...');
        
        // Check enhanced tables
        const enhancedTables = await client.query(`
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('analysis_sessions', 'cluster_data_enhanced', 'cascade_probabilities', 'price_targets', 'market_context')
            ORDER BY table_name
        `);
        
        // console.log(`📋 Enhanced Tables created: ${enhancedTables.rows.map(r => r.table_name).join(', ')}`);
        
        // Check SSE tables
        const sseTables = await client.query(`
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'sse_%'
            ORDER BY table_name
        `);
        
        // console.log(`📋 SSE Tables created: ${sseTables.rows.map(r => r.table_name).join(', ')}`);
        
        // Test enhanced schema
        try {
            const sessionTest = await client.query('SELECT COUNT(*) as count FROM analysis_sessions');
            // console.log(`✅ Enhanced schema working. Analysis sessions table accessible.`);
        } catch (error) {
            console.error(`❌ Error accessing enhanced tables: ${error.message}`);
        }
        
        // Check if we can insert a test event (if SSE tables exist)
        try {
            const sseEventCheck = await client.query(`
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = 'sse_events'
            `);
            
            if (sseEventCheck.rows.length > 0) {
                await client.query(`
                    INSERT INTO sse_events (event_type, event_data, expires_at) 
                    VALUES ('heartbeat', '{"test": true}', NOW() + INTERVAL '1 hour')
                `);
                
                const eventCount = await client.query('SELECT COUNT(*) as count FROM sse_events');
                // console.log(`✅ Test event inserted successfully. Total events: ${eventCount.rows[0].count}`);
                
                // Clean up test event
                await client.query("DELETE FROM sse_events WHERE event_data = '{\"test\": true}'");
            }
        } catch (error) {
            console.error(`❌ Error testing event insertion: ${error.message}`);
        }
        
        // console.log('\n🚀 SSE Server is ready to start!');
        // console.log('Run: npm start');
        
    } catch (error) {
        console.error('❌ Database setup failed:', error);
        process.exit(1);
    } finally {
        client.release();
        await pool.end();
    }
}

// Run the setup
setupDatabase().catch(error => {
    console.error('❌ Setup script failed:', error);
    process.exit(1);
});
