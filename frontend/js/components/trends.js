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
            filter: initialFilter,
            entityType: 'parks',  // 'parks' or 'rides'
            limit: 20,
            loading: false,
            error: null,
            parksImproving: null,
            parksDeclining: null,
            ridesImproving: null,
            ridesDeclining: null,
            aggregateStats: null
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await Promise.all([
            this.fetchAllTrends(),
            this.fetchAggregateStats()
        ]);
    }

    /**
     * Fetch aggregate stats from parks/downtime endpoint
     */
    async fetchAggregateStats() {
        try {
            const response = await this.apiClient.get('/parks/downtime', {
                period: 'today',
                filter: this.state.filter,
                limit: 1
            });
            if (response.success && response.aggregate_stats) {
                this.setState({ aggregateStats: response.aggregate_stats });
            }
        } catch (error) {
            console.error('Failed to fetch aggregate stats:', error);
        }
    }

    /**
     * Fetch all trends categories from API
     */
    async fetchAllTrends() {
        this.setState({ loading: true, error: null });

        try {
            const params = {
                period: this.state.period,
                filter: this.state.filter,
                limit: this.state.limit
            };

            // Fetch all 4 categories in parallel
            const [parksImproving, parksDeclining, ridesImproving, ridesDeclining] = await Promise.all([
                this.apiClient.get('/trends', { ...params, category: 'parks-improving' }),
                this.apiClient.get('/trends', { ...params, category: 'parks-declining' }),
                this.apiClient.get('/trends', { ...params, category: 'rides-improving' }),
                this.apiClient.get('/trends', { ...params, category: 'rides-declining' })
            ]);

            this.setState({
                parksImproving: parksImproving.success ? parksImproving : null,
                parksDeclining: parksDeclining.success ? parksDeclining : null,
                ridesImproving: ridesImproving.success ? ridesImproving : null,
                ridesDeclining: ridesDeclining.success ? ridesDeclining : null,
                loading: false
            });
            this.updateLastUpdateTime();
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
     * Render aggregate statistics
     */
    renderAggregateStats() {
        if (!this.state.aggregateStats) {
            return '<div class="stats-grid"></div>';
        }

        const stats = this.state.aggregateStats;

        return `
            <div class="stats-grid">
                <div class="stat-block">
                    <div class="stat-label">Parks Tracked</div>
                    <div class="stat-value">${stats.total_parks_tracked || 0}</div>
                </div>
                <div class="stat-block">
                    <div class="stat-label">Peak Downtime</div>
                    <div class="stat-value">${this.formatHours(stats.peak_downtime_hours || 0)}</div>
                </div>
                <div class="stat-block">
                    <div class="stat-label">Currently Down</div>
                    <div class="stat-value">${stats.currently_down_rides || 0}</div>
                </div>
            </div>
        `;
    }

    /**
     * Render the component
     */
    render() {
        if (!this.container) return;

        this.container.innerHTML = `
            <div class="trends-view">
                ${this.renderAggregateStats()}
                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render main content (loading, error, or toggle + trends tables)
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
                    <p class="error-message">${this.state.error}</p>
                    <button class="retry-btn">Retry</button>
                </div>
            `;
        }

        // Render toggle + selected entity type trends
        return `
            <div class="section-header">
                <div class="entity-toggle">
                    <button class="entity-btn ${this.state.entityType === 'parks' ? 'active' : ''}"
                            data-entity="parks">Parks</button>
                    <button class="entity-btn ${this.state.entityType === 'rides' ? 'active' : ''}"
                            data-entity="rides">Rides</button>
                </div>
                <h2 class="section-title">${this.getPeriodTitle('Performance Trends')}</h2>
            </div>
            ${this.state.entityType === 'parks'
                ? this.renderParksTrends()
                : this.renderRidesTrends()}
        `;
    }

    /**
     * Render parks trends (improving + declining)
     */
    renderParksTrends() {
        return `
            ${this.renderTrendTable(this.state.parksImproving?.parks, 'parks', true, 'Most Improved')}
            ${this.renderTrendTable(this.state.parksDeclining?.parks, 'parks', false, 'Declining Performance')}
        `;
    }

    /**
     * Render rides trends (improving + declining)
     */
    renderRidesTrends() {
        return `
            ${this.renderTrendTable(this.state.ridesImproving?.rides, 'rides', true, 'Most Improved')}
            ${this.renderTrendTable(this.state.ridesDeclining?.rides, 'rides', false, 'Declining Performance')}
        `;
    }

    /**
     * Render a trend table (parks or rides)
     */
    renderTrendTable(data, type, isImproving, tableTitle) {
        if (!data || data.length === 0) {
            return `
                <div class="data-container">
                    <div class="table-header">${tableTitle}</div>
                    <div class="empty-state">
                        <p>No significant trends found for the selected period</p>
                    </div>
                </div>
            `;
        }

        if (type === 'parks') {
            return `
                <div class="data-container">
                    <div class="table-header">${tableTitle}</div>
                    <table class="rankings-table trends-table">
                        <thead>
                            <tr>
                                <th class="park-col">Park</th>
                                <th class="location-col">Location</th>
                                <th class="uptime-col">Current</th>
                                <th class="uptime-col">Previous</th>
                                <th class="change-col">Change</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.map(park => this.renderParkTrendRow(park, isImproving)).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        } else {
            return `
                <div class="data-container">
                    <div class="table-header">${tableTitle}</div>
                    <table class="rankings-table trends-table">
                        <thead>
                            <tr>
                                <th class="ride-col">Ride</th>
                                <th class="park-col">Park</th>
                                <th class="uptime-col">Current</th>
                                <th class="uptime-col">Previous</th>
                                <th class="change-col">Change</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.map(ride => this.renderRideTrendRow(ride, isImproving)).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        }
    }

    /**
     * Render a single park trend row
     */
    renderParkTrendRow(park, isImproving) {
        const changeClass = isImproving ? 'trend-better' : 'trend-worse';
        const changeIcon = isImproving ? '↑' : '↓';
        const change = Math.abs(park.improvement_percentage || park.decline_percentage || 0);

        return `
            <tr class="trend-row">
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(park.park_name || 'Unknown Park')}</span>
                </td>
                <td class="location-col">${this.escapeHtml(park.location || 'Unknown')}</td>
                <td class="uptime-col">
                    <span class="uptime-value">${Number(park.current_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="uptime-col">
                    <span class="uptime-value">${Number(park.previous_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="change-col">
                    <span class="change-indicator ${changeClass}">
                        ${changeIcon} ${change.toFixed(1)}%
                    </span>
                </td>
            </tr>
        `;
    }

    /**
     * Render a single ride trend row
     */
    renderRideTrendRow(ride, isImproving) {
        const changeClass = isImproving ? 'trend-better' : 'trend-worse';
        const changeIcon = isImproving ? '↑' : '↓';
        const change = Math.abs(ride.improvement_percentage || ride.decline_percentage || 0);

        return `
            <tr class="trend-row">
                <td class="ride-col">
                    <span class="ride-name">${this.escapeHtml(ride.ride_name || 'Unknown Ride')}</span>
                </td>
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(ride.park_name || 'Unknown Park')}</span>
                </td>
                <td class="uptime-col">
                    <span class="uptime-value">${Number(ride.current_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="uptime-col">
                    <span class="uptime-value">${Number(ride.previous_uptime || 0).toFixed(1)}%</span>
                </td>
                <td class="change-col">
                    <span class="change-indicator ${changeClass}">
                        ${changeIcon} ${change.toFixed(1)}%
                    </span>
                </td>
            </tr>
        `;
    }

    /**
     * Get table header title based on current period
     */
    getPeriodTitle(baseTitle) {
        const periodLabels = {
            'today': "Today's",
            '7days': '7 Day',
            '30days': '30 Day'
        };
        return `${periodLabels[this.state.period] || ''} ${baseTitle}`;
    }

    /**
     * Format hours into readable string
     */
    formatHours(hours) {
        const numHours = Number(hours);
        if (hours === null || hours === undefined || isNaN(numHours) || numHours === 0) return '0h 0m';

        const wholeHours = Math.floor(numHours);
        const minutes = Math.round((numHours - wholeHours) * 60);

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
            this.fetchAllTrends();
        }
    }

    /**
     * Update period (called by app.js global period selector)
     */
    updatePeriod(newPeriod) {
        if (newPeriod !== this.state.period) {
            this.state.period = newPeriod;
            this.fetchAllTrends();
        }
    }

    /**
     * Attach event listeners to controls
     */
    attachEventListeners() {
        // Entity toggle buttons
        const entityBtns = this.container.querySelectorAll('.entity-btn');
        entityBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const newEntityType = btn.dataset.entity;
                if (newEntityType !== this.state.entityType) {
                    this.setState({ entityType: newEntityType });
                }
            });
        });

        // Retry button (if error state)
        const retryBtn = this.container.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.fetchAllTrends();
            });
        }
    }
}

// Initialize when view is loaded
window.Trends = Trends;
