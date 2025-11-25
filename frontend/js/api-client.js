// Theme Park Hall of Shame - API Client

class APIClient {
    constructor() {
        // Get API base URL from config, fallback to '/api' if not available
        this.baseUrl = (window.APP_CONFIG && window.APP_CONFIG.API_BASE_URL) || '/api';
        console.log(`API Client initialized with base URL: ${this.baseUrl}`);
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
}

// Export singleton instance
const apiClient = new APIClient();
