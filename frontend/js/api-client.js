// Theme Park Hall of Shame - API Client

class APIClient {
    constructor() {
        // Get API base URL from config, with port-based auto-detection for local dev
        // This handles cases where config.js hasn't loaded yet or is cached
        let configUrl = (window.APP_CONFIG && window.APP_CONFIG.API_BASE_URL);

        // Auto-detect local development: if on port 8080, use backend on 5001
        if (!configUrl || configUrl === '/api') {
            if (window.location.port === '8080') {
                this.baseUrl = 'http://localhost:5001/api';
            } else {
                this.baseUrl = '/api';
            }
        } else {
            this.baseUrl = configUrl;
        }

        // Response cache: { url: { data, timestamp } }
        this._cache = {};
        this._cacheTTL = 5 * 60 * 1000; // 5 minutes in milliseconds

        console.log(`API Client initialized with base URL: ${this.baseUrl}`);
    }

    /**
     * Generate cache key from URL
     * @param {URL} url - Full URL object
     * @returns {string} Cache key
     */
    _getCacheKey(url) {
        return url.href;
    }

    /**
     * Check if cached response is still valid
     * @param {string} key - Cache key
     * @returns {Object|null} Cached data or null if expired/missing
     */
    _getFromCache(key) {
        const cached = this._cache[key];
        if (!cached) return null;

        const age = Date.now() - cached.timestamp;
        if (age > this._cacheTTL) {
            delete this._cache[key];
            return null;
        }

        return cached.data;
    }

    /**
     * Store response in cache
     * @param {string} key - Cache key
     * @param {Object} data - Response data
     */
    _setCache(key, data) {
        this._cache[key] = {
            data: data,
            timestamp: Date.now()
        };
    }

    /**
     * Make a GET request to the API
     * @param {string} endpoint - API endpoint (e.g., '/parks/downtime')
     * @param {Object} params - Query parameters
     * @returns {Promise<Object>} API response
     */
    async get(endpoint, params = {}) {
        try {
            // Build full URL
            // If baseUrl is absolute (starts with http), use it directly
            // Otherwise, resolve relative to current origin
            let fullUrl;
            if (this.baseUrl.startsWith('http://') || this.baseUrl.startsWith('https://')) {
                fullUrl = `${this.baseUrl}${endpoint}`;
            } else {
                fullUrl = new URL(`${this.baseUrl}${endpoint}`, window.location.origin).href;
            }

            const url = new URL(fullUrl);
            Object.keys(params).forEach(key => {
                if (params[key] !== null && params[key] !== undefined) {
                    url.searchParams.append(key, params[key]);
                }
            });

            // Check cache first
            const cacheKey = this._getCacheKey(url);
            const cached = this._getFromCache(cacheKey);
            if (cached) {
                console.log(`API CACHE HIT: ${url.href}`);
                return cached;
            }

            console.log(`API GET: ${url.href}`);

            const response = await fetch(url, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                },
                // Include credentials if making cross-origin requests
                credentials: this.baseUrl.startsWith('http') ? 'omit' : 'same-origin'
            });

            if (!response.ok) {
                const errorText = await response.text();
                console.error(`API error response:`, errorText);
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();

            // Cache successful response
            this._setCache(cacheKey, data);

            return data;
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    }

    /**
     * Get the current API base URL
     * @returns {string} Base URL
     */
    getBaseUrl() {
        return this.baseUrl;
    }

    /**
     * Clear all cached data
     * Useful when period or filter changes
     */
    clearCache() {
        this._cache = {};
        console.log('API cache cleared');
    }

    /**
     * Prefetch common API endpoints in the background.
     * Loads data for all main tabs so tab switching is instant.
     *
     * @param {string} period - Time period (live, today, last_week, last_month)
     * @param {string} filter - Park filter (all-parks, disney-universal)
     */
    async prefetch(period = 'live', filter = 'all-parks') {
        console.log(`Prefetching data for period=${period}, filter=${filter}`);

        // Define all endpoints to prefetch
        const endpoints = [
            // Downtime tab
            { endpoint: '/parks/downtime', params: { period, filter, limit: 50 } },
            { endpoint: '/rides/downtime', params: { period, filter, limit: 100 } },
            { endpoint: '/live/status-summary', params: { filter } },

            // WaitTimes tab
            { endpoint: '/parks/waittimes', params: { period, filter, limit: 50 } },
            { endpoint: '/rides/waittimes', params: { period, filter, limit: 100 } }
        ];

        // Prefetch all endpoints in parallel (don't await - fire and forget)
        const prefetchPromises = endpoints.map(({ endpoint, params }) =>
            this.get(endpoint, params).catch(err => {
                console.warn(`Prefetch failed for ${endpoint}:`, err.message);
            })
        );

        // Wait for all prefetches to complete
        await Promise.all(prefetchPromises);
        console.log('Prefetch complete');
    }

    /**
     * Get cache statistics for debugging
     * @returns {Object} Cache stats
     */
    getCacheStats() {
        const now = Date.now();
        const entries = Object.entries(this._cache);
        const validEntries = entries.filter(([_, v]) => (now - v.timestamp) < this._cacheTTL);
        return {
            totalEntries: entries.length,
            validEntries: validEntries.length,
            ttlSeconds: this._cacheTTL / 1000
        };
    }
}

// Export singleton instance
const apiClient = new APIClient();
