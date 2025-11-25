/**
 * Theme Park Hall of Shame - Trends Component
 * Displays performance trends showing parks/rides with ≥5% uptime changes
 */

class Trends {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: '7days',
            category: 'parks-improving',
            filter: initialFilter,
            limit: 50,
            loading: false,
            error: null,
            data: null
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await this.fetchTrends();
    }

    /**
     * Fetch trends from API
     */
    async fetchTrends() {
        this.setState({ loading: true, error: null });

        try {
            const params = {
                period: this.state.period,
                category: this.state.category,
                filter: this.state.filter,
                limit: this.state.limit
            };

            const response = await this.apiClient.get('/trends', params);

            if (response.success) {
                this.setState({
                    data: response,
                    loading: false
                });
                this.updateLastUpdateTime();
            } else {
                throw new Error(response.error || 'Failed to fetch trends');
            }
        } catch (error) {
            this.setState({
                error: error.message,
                loading: false
            });
        }
    }

    /**
     * Update component state and trigger re-render
     */
    setState(newState) {
        this.state = { ...this.state, ...newState };
        this.render();
    }

    /**
     * Render the component
     */
    render() {
        if (!this.container) return;

        this.container.innerHTML = `
            <div class="trends-view">
                <div class="view-header">
                    <h2>Performance Trends</h2>
                    <p class="view-description">
                        Parks and rides showing ≥5% uptime changes. Green = improving reliability, Red = declining reliability.
                    </p>
                </div>

                ${this.renderControls()}
                ${this.renderPeriodComparison()}
                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render period and category controls
     */
    renderControls() {
        return `
            <div class="trends-controls">
                <div class="control-group">
                    <label>Time Period:</label>
                    <div class="button-group">
                        <button
                            class="period-btn ${this.state.period === 'today' ? 'active' : ''}"
                            data-period="today"
                        >Today</button>
                        <button
                            class="period-btn ${this.state.period === '7days' ? 'active' : ''}"
                            data-period="7days"
                        >7 Days</button>
                        <button
                            class="period-btn ${this.state.period === '30days' ? 'active' : ''}"
                            data-period="30days"
                        >30 Days</button>
                    </div>
                </div>

                <div class="control-group">
                    <label>Category:</label>
                    <div class="button-group">
                        <button
                            class="category-btn ${this.state.category === 'parks-improving' ? 'active' : ''}"
                            data-category="parks-improving"
                        >Parks Improving</button>
                        <button
                            class="category-btn ${this.state.category === 'parks-declining' ? 'active' : ''}"
                            data-category="parks-declining"
                        >Parks Declining</button>
                        <button
                            class="category-btn ${this.state.category === 'rides-improving' ? 'active' : ''}"
                            data-category="rides-improving"
                        >Rides Improving</button>
                        <button
                            class="category-btn ${this.state.category === 'rides-declining' ? 'active' : ''}"
                            data-category="rides-declining"
                        >Rides Declining</button>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render period comparison info
     */
    renderPeriodComparison() {
        if (!this.state.data || !this.state.data.comparison) {
            return '<div class="period-comparison"></div>';
        }

        const comparison = this.state.data.comparison;
        return `
            <div class="period-comparison">
                <div class="comparison-info">
                    <span class="comparison-label">Comparing:</span>
                    <span class="period-dates">
                        <strong>Current:</strong> ${comparison.current_period}
                        &nbsp;&nbsp;|&nbsp;&nbsp;
                        <strong>Previous:</strong> ${comparison.previous_period}
                    </span>
                </div>
            </div>
        `;
    }

    /**
     * Render main content (loading, error, or trends table)
     */
    renderContent() {
        if (this.state.loading) {
            return `
                <div class="loading-state">
                    <div class="spinner"></div>
                    <p>Loading trends...</p>
                </div>
            `;
        }

        if (this.state.error) {
            return `
                <div class="error-state">
                    <p class="error-message">⚠️ ${this.state.error}</p>
                    <button class="retry-btn">Retry</button>
                </div>
            `;
        }

        if (this.state.data) {
            const isParksCategory = this.state.category.startsWith('parks-');
            const trendsData = isParksCategory ? this.state.data.parks : this.state.data.rides;

            if (trendsData && trendsData.length > 0) {
                return isParksCategory
                    ? this.renderParksTrendsTable(trendsData)
                    : this.renderRidesTrendsTable(trendsData);
            }

            return `
                <div class="empty-state">
                    <p>No significant trends found for the selected period and category</p>
                    <p class="empty-state-hint">Try selecting a different time period or category</p>
                </div>
            `;
        }

        return `
            <div class="empty-state">
                <p>No trends data available</p>
            </div>
        `;
    }

    /**
     * Render parks trends table
     */
    renderParksTrendsTable(parks) {
        const isImproving = this.state.category === 'parks-improving';

        return `
            <div class="trends-table-container">
                <div class="trends-count">
                    Showing ${parks.length} ${isImproving ? 'improving' : 'declining'} ${parks.length === 1 ? 'park' : 'parks'}
                </div>
                <table class="trends-table">
                    <thead>
                        <tr>
                            <th class="park-col">Park</th>
                            <th class="location-col">Location</th>
                            <th class="current-uptime-col">Current Uptime</th>
                            <th class="previous-uptime-col">Previous Uptime</th>
                            <th class="change-col">Change</th>
                            <th class="downtime-col">Current Downtime</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${parks.map(park => this.renderParkTrendRow(park, isImproving)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single park trend row
     */
    renderParkTrendRow(park, isImproving) {
        const changeClass = isImproving ? 'trend-better' : 'trend-worse';
        const changeIcon = isImproving ? '↑' : '↓';

        return `
            <tr class="trend-row ${changeClass}">
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(park.park_name || 'Unknown Park')}</span>
                </td>
                <td class="location-col">${this.escapeHtml(park.location || 'Unknown')}</td>
                <td class="current-uptime-col">
                    <span class="uptime-value">${(park.current_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="previous-uptime-col">
                    <span class="uptime-value">${(park.previous_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="change-col">
                    <span class="change-indicator ${changeClass}">
                        ${changeIcon} ${Math.abs(park.uptime_change || 0).toFixed(1)}%
                    </span>
                </td>
                <td class="downtime-col">
                    <span class="downtime-value">${this.formatHours(park.current_downtime_hours || 0)}</span>
                </td>
            </tr>
        `;
    }

    /**
     * Render rides trends table
     */
    renderRidesTrendsTable(rides) {
        const isImproving = this.state.category === 'rides-improving';

        return `
            <div class="trends-table-container">
                <div class="trends-count">
                    Showing ${rides.length} ${isImproving ? 'improving' : 'declining'} ${rides.length === 1 ? 'ride' : 'rides'}
                </div>
                <table class="trends-table">
                    <thead>
                        <tr>
                            <th class="ride-col">Ride</th>
                            <th class="park-col">Park</th>
                            <th class="tier-col">Tier</th>
                            <th class="current-uptime-col">Current Uptime</th>
                            <th class="previous-uptime-col">Previous Uptime</th>
                            <th class="change-col">Change</th>
                            <th class="downtime-col">Current Downtime</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rides.map(ride => this.renderRideTrendRow(ride, isImproving)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single ride trend row
     */
    renderRideTrendRow(ride, isImproving) {
        const changeClass = isImproving ? 'trend-better' : 'trend-worse';
        const changeIcon = isImproving ? '↑' : '↓';

        return `
            <tr class="trend-row ${changeClass}">
                <td class="ride-col">
                    <span class="ride-name">${this.escapeHtml(ride.ride_name || 'Unknown Ride')}</span>
                </td>
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(ride.park_name || 'Unknown Park')}</span>
                </td>
                <td class="tier-col">
                    <span class="tier-badge tier-${ride.tier || 2}">Tier ${ride.tier || 2}</span>
                </td>
                <td class="current-uptime-col">
                    <span class="uptime-value">${(ride.current_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="previous-uptime-col">
                    <span class="uptime-value">${(ride.previous_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="change-col">
                    <span class="change-indicator ${changeClass}">
                        ${changeIcon} ${Math.abs(ride.uptime_change || 0).toFixed(1)}%
                    </span>
                </td>
                <td class="downtime-col">
                    <span class="downtime-value">${this.formatMinutes(ride.current_downtime_minutes || 0)}</span>
                </td>
            </tr>
        `;
    }

    /**
     * Format hours into readable string
     */
    formatHours(hours) {
        if (hours === null || hours === undefined || hours === 0) return '0h 0m';

        const wholeHours = Math.floor(hours);
        const minutes = Math.round((hours - wholeHours) * 60);

        if (wholeHours === 0) return `${minutes}m`;
        if (minutes === 0) return `${wholeHours}h`;
        return `${wholeHours}h ${minutes}m`;
    }

    /**
     * Format minutes into readable string
     */
    formatMinutes(minutes) {
        if (minutes === null || minutes === undefined || minutes === 0) return '0m';

        const hours = Math.floor(minutes / 60);
        const mins = Math.round(minutes % 60);

        if (hours === 0) return `${mins}m`;
        if (mins === 0) return `${hours}h`;
        return `${hours}h ${mins}m`;
    }

    /**
     * Escape HTML to prevent XSS
     */
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /**
     * Update last update time in footer
     */
    updateLastUpdateTime() {
        const lastUpdateEl = document.getElementById('last-update-time');
        if (lastUpdateEl) {
            const now = new Date();
            lastUpdateEl.textContent = now.toLocaleTimeString();
        }
    }

    /**
     * Update filter from global filter (called by app.js)
     */
    updateFilter(newFilter) {
        if (newFilter !== this.state.filter) {
            this.state.filter = newFilter;
            this.fetchTrends();
        }
    }

    /**
     * Attach event listeners to controls
     */
    attachEventListeners() {
        // Period buttons
        const periodBtns = this.container.querySelectorAll('.period-btn');
        periodBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const period = btn.dataset.period;
                if (period !== this.state.period) {
                    this.state.period = period;
                    this.fetchTrends();
                }
            });
        });

        // Category buttons
        const categoryBtns = this.container.querySelectorAll('.category-btn');
        categoryBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const category = btn.dataset.category;
                if (category !== this.state.category) {
                    this.state.category = category;
                    this.fetchTrends();
                }
            });
        });

        // Retry button (if error state)
        const retryBtn = this.container.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.fetchTrends();
            });
        }
    }
}

// Initialize when view is loaded
window.Trends = Trends;
