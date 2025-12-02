/**
 * Theme Park Hall of Shame - Frontend Configuration
 *
 * DEPLOYMENT INSTRUCTIONS:
 *
 * For LOCAL DEVELOPMENT:
 *   - Leave API_BASE_URL as '/api'
 *   - Run backend locally on port 5001
 *   - Open frontend/index.html directly or serve with a local server
 *
 * For PRODUCTION DEPLOYMENT:
 *   - Update API_BASE_URL to point to your deployed backend API
 *   - Example: 'https://api.themepark-shame.com/api'
 *   - Or: 'https://your-backend-service.herokuapp.com/api'
 *   - Make sure CORS is properly configured on the backend
 */

const CONFIG = {
    // API Configuration
    // Always use production API path - no dev mode detection
    API_BASE_URL: '/api',

    // For production deployment, uncomment and set your backend URL:
    // API_BASE_URL: 'https://your-backend-api.com/api',

    // Feature Flags (for future use)
    FEATURES: {
        PARK_DETAILS: true,
        ABOUT_MODAL: true,
        TRENDS: false,  // Not yet implemented
        WEIGHTED_SCORING: true
    },

    // Update intervals (in milliseconds)
    DATA_REFRESH_INTERVAL: 600000,  // 10 minutes

    // App metadata
    APP_VERSION: '1.0.0',
    APP_NAME: 'Theme Park Hall of Shame'
};

// Make config available globally
window.APP_CONFIG = CONFIG;
