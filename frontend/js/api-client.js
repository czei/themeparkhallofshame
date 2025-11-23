// Theme Park Hall of Shame - API Client
// Placeholder - will be enhanced in Phase 12

const API_BASE_URL = '/api';  // Will be configurable for local vs production

class APIClient {
    async get(endpoint, params = {}) {
        try {
            const url = new URL(`${API_BASE_URL}${endpoint}`, window.location.origin);
            Object.keys(params).forEach(key => url.searchParams.append(key, params[key]));

            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    }
}

// Export singleton instance
const apiClient = new APIClient();
