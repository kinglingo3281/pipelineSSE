// PM2 Ecosystem Configuration for SSE Server
module.exports = {
  apps: [{
    name: 'sse-server',
    script: './sse-server-wrapper.js',  // Use wrapper for auto-restart & cleanup
    instances: 1,
    exec_mode: 'fork',
    
    // Memory management
    max_memory_restart: '900M',
    
    // Restart behavior
    restart_delay: 3000,
    max_restarts: 50,  // Increased from 10 for wrapper restarts
    min_uptime: '10s',
    
    // Logging (console only - use pm2 logs command to view, no files)
    error_file: '/dev/null',
    out_file: '/dev/null',
    combine_logs: true,
    
    // Environment
    env: {
      NODE_ENV: 'production',
      PORT: 8080,
      LOG_LEVEL: 'info',
      JSON_CLEANUP_DAYS: 1  // TESTING: Delete files older than 1 day (change back to 7 for production)
    },
    
    // Monitoring
    autorestart: true,
    watch: false,
    
    // Performance
    node_args: '--max-old-space-size=15360 --expose-gc',
    
    // Kill timeout
    kill_timeout: 5000,
    listen_timeout: 10000,
    
    // Windows specific
    interpreter: 'node',
    
    // Post-deploy actions
    post_update: 'npm install',
    
    // Graceful shutdown
    wait_ready: true
  }]
};
