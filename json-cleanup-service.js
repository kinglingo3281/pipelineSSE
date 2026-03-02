import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

class JsonCleanupService {
    constructor(config = {}) {
        this.watchDirectory = config.watchDirectory || 
            process.env.VISUALIZATIONS_DIR || './data/visualizations/';
        this.maxFileAgeDays = config.maxFileAgeDays || 
            parseInt(process.env.JSON_CLEANUP_DAYS) || 7;
        this.filePattern = config.filePattern || 
            /^([A-Z]+)_enhanced_analysis_(\d{8}_\d{6})\.json$/;
    }

    log(level, message, data = {}) {
        const timestamp = new Date().toISOString();
        console.log(`[${timestamp}] [JsonCleanup] [${level.toUpperCase()}] ${message}`, 
            Object.keys(data).length > 0 ? JSON.stringify(data) : '');
    }

    // Get file age in days
    getFileAgeDays(filePath) {
        const stats = fs.statSync(filePath);
        const ageMs = Date.now() - stats.mtimeMs;
        return ageMs / (1000 * 60 * 60 * 24);
    }

    // Find old JSON files
    findOldFiles() {
        try {
            const files = fs.readdirSync(this.watchDirectory);
            const oldFiles = [];

            for (const file of files) {
                if (!this.filePattern.test(file)) continue;

                const filePath = path.join(this.watchDirectory, file);
                const ageDays = this.getFileAgeDays(filePath);

                if (ageDays > this.maxFileAgeDays) {
                    oldFiles.push({
                        name: file,
                        path: filePath,
                        ageDays: ageDays.toFixed(1),
                        sizeBytes: fs.statSync(filePath).size
                    });
                }
            }

            return oldFiles;
        } catch (error) {
            this.log('error', 'Failed to scan directory', { error: error.message });
            return [];
        }
    }

    // Delete old files
    async deleteOldFiles(files) {
        let deletedCount = 0;
        let deletedBytes = 0;

        for (const file of files) {
            try {
                fs.unlinkSync(file.path);
                deletedCount++;
                deletedBytes += file.sizeBytes;
                this.log('debug', `Deleted: ${file.name}`, {
                    age: `${file.ageDays} days`
                });
            } catch (error) {
                this.log('error', `Failed to delete: ${file.name}`, {
                    error: error.message
                });
            }
        }

        return { deletedCount, deletedBytes };
    }

    // Recycle bin cleanup (no-op on Linux, files are already permanently deleted)
    async emptyRecycleBin() {
        // No recycle bin on Linux - files deleted by unlinkSync are gone
        this.log('debug', 'Skipping recycle bin (Linux)');
    }

    // Get directory stats
    getDirectoryStats() {
        try {
            const files = fs.readdirSync(this.watchDirectory);
            let totalSize = 0;
            let jsonCount = 0;

            for (const file of files) {
                if (this.filePattern.test(file)) {
                    const filePath = path.join(this.watchDirectory, file);
                    totalSize += fs.statSync(filePath).size;
                    jsonCount++;
                }
            }

            return {
                totalFiles: jsonCount,
                totalSizeMB: (totalSize / 1024 / 1024).toFixed(2)
            };
        } catch (error) {
            return { totalFiles: 0, totalSizeMB: 0 };
        }
    }

    // Main cleanup routine
    async run() {
        this.log('info', '=== JSON Cleanup Service Starting ===');
        
        // Get initial stats
        const beforeStats = this.getDirectoryStats();
        this.log('info', 'Directory stats before cleanup', beforeStats);

        // Find old files
        const oldFiles = this.findOldFiles();
        this.log('info', `Found ${oldFiles.length} files older than ${this.maxFileAgeDays} days`);

        if (oldFiles.length === 0) {
            this.log('info', 'No files to delete');
            return;
        }

        // Delete old files
        const { deletedCount, deletedBytes } = await this.deleteOldFiles(oldFiles);
        this.log('info', 'Deletion complete', {
            deleted: deletedCount,
            freedMB: (deletedBytes / 1024 / 1024).toFixed(2)
        });

        // Empty recycle bin
        await this.emptyRecycleBin();

        // Get final stats
        const afterStats = this.getDirectoryStats();
        this.log('info', 'Directory stats after cleanup', afterStats);

        this.log('info', '=== JSON Cleanup Service Complete ===');
    }
}

// Export for use in wrapper
export default {
    run: async () => {
        const service = new JsonCleanupService();
        await service.run();
    },
    
    // Export class for custom usage
    JsonCleanupService
};
