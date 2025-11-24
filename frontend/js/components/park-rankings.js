/**
 * Theme Park Hall of Shame - Park Rankings Component
 * Displays park downtime rankings with period and filter controls
 */

class ParkRankings {
    constructor(apiClient, containerId) {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: 'today',
            filter: 'all-parks',
            limit: 50,
            weighted: false,
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
        await this.fetchRankings();
    }

    /**
     * Fetch park rankings from API
     */
    async fetchRankings() {
        this.setState({ loading: true, error: null });

        try {
            const params = {
                period: this.state.period,
                filter: this.state.filter,
                limit: this.state.limit,
                weighted: this.state.weighted
            };

            const response = await this.apiClient.get('/parks/downtime', params);

            if (response.success) {
                this.setState({
                    data: response,
                    loading: false
                });
                this.updateLastUpdateTime();
            } else {
                throw new Error(response.error || 'Failed to fetch park rankings');
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
            <div class="park-rankings-view">
                <div class="view-header">
                    <h2>Park Downtime Rankings</h2>
                    <p class="view-description">
                        Compare theme park reliability based on ride downtime. Lower downtime = better reliability.
                    </p>
                </div>

                ${this.renderControls()}
                ${this.renderAggregateStats()}
                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render period and filter controls
     */
    renderControls() {
        return `
            <div class="rankings-controls">
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
                    <label>Filter:</label>
                    <div class="button-group">
                        <button
                            class="filter-btn ${this.state.filter === 'all-parks' ? 'active' : ''}"
                            data-filter="all-parks"
                        >All Parks</button>
                        <button
                            class="filter-btn ${this.state.filter === 'disney-universal' ? 'active' : ''}"
                            data-filter="disney-universal"
                        >Disney & Universal</button>
                    </div>
                </div>

                <div class="control-group">
                    <label>
                        <input
                            type="checkbox"
                            id="weighted-toggle"
                            ${this.state.weighted ? 'checked' : ''}
                        >
                        Weighted by ride tier
                    </label>
                </div>
            </div>
        `;
    }

    /**
     * Render aggregate statistics
     */
    renderAggregateStats() {
        if (!this.state.data || !this.state.data.aggregate_stats) {
            return '<div class="aggregate-stats"></div>';
        }

        const stats = this.state.data.aggregate_stats;

        return `
            <div class="aggregate-stats">
                <div class="stat-card">
                    <span class="stat-label">Total Parks Tracked</span>
                    <span class="stat-value">${stats.total_parks_tracked || 0}</span>
                </div>
                <div class="stat-card">
                    <span class="stat-label">Peak Downtime Today</span>
                    <span class="stat-value">${this.formatHours(stats.peak_downtime_hours || 0)}</span>
                </div>
                <div class="stat-card">
                    <span class="stat-label">Currently Down</span>
                    <span class="stat-value">${stats.currently_down_rides || 0} rides</span>
                </div>
                ${stats.avg_uptime_percentage !== undefined ? `
                <div class="stat-card">
                    <span class="stat-label">Average Uptime</span>
                    <span class="stat-value">${stats.avg_uptime_percentage.toFixed(1)}%</span>
                </div>
                ` : ''}
            </div>
        `;
    }

    /**
     * Render main content (loading, error, or rankings table)
     */
    renderContent() {
        if (this.state.loading) {
            return `
                <div class="loading-state">
                    <div class="spinner"></div>
                    <p>Loading park rankings...</p>
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

        if (this.state.data && this.state.data.data) {
            return this.renderRankingsTable(this.state.data.data);
        }

        return `
            <div class="empty-state">
                <p>No rankings data available</p>
            </div>
        `;
    }

    /**
     * Render rankings table
     */
    renderRankingsTable(rankings) {
        if (!rankings || rankings.length === 0) {
            return `
                <div class="empty-state">
                    <p>No parks found for the selected filters</p>
                </div>
            `;
        }

        return `
            <div class="rankings-table-container">
                <table class="rankings-table">
                    <thead>
                        <tr>
                            <th class="rank-col">Rank</th>
                            <th class="park-col">Park</th>
                            <th class="location-col">Location</th>
                            <th class="downtime-col">Downtime</th>
                            <th class="uptime-col">Uptime %</th>
                            <th class="affected-col">Affected Rides</th>
                            <th class="trend-col">Trend</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rankings.map(park => this.renderParkRow(park)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single park row
     */
    renderParkRow(park) {
        const trendClass = this.getTrendClass(park.trend_percentage);
        const trendIcon = this.getTrendIcon(park.trend_percentage);
        const trendText = park.trend_percentage !== null && park.trend_percentage !== undefined
            ? `${park.trend_percentage > 0 ? '+' : ''}${park.trend_percentage.toFixed(1)}%`
            : 'N/A';

        return `
            <tr class="park-row ${park.rank <= 3 ? 'top-three' : ''}">
                <td class="rank-col">
                    <span class="rank-badge ${park.rank === 1 ? 'rank-1' : park.rank === 2 ? 'rank-2' : park.rank === 3 ? 'rank-3' : ''}">
                        ${park.rank}
                    </span>
                </td>
                <td class="park-col">
                    <a
                        href="${park.queue_times_url}"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="park-link"
                    >
                        ${this.escapeHtml(park.name || 'Unknown Park')}
                        <span class="external-icon">↗</span>
                    </a>
                </td>
                <td class="location-col">${this.escapeHtml(park.location || 'Unknown')}</td>
                <td class="downtime-col">
                    <span class="downtime-value">
                        ${this.formatHours(park.total_downtime_hours || 0)}
                    </span>
                </td>
                <td class="uptime-col">
                    <div class="uptime-display">
                        <span class="uptime-percentage">${(park.uptime_percentage || 0).toFixed(1)}%</span>
                        <div class="uptime-bar">
                            <div
                                class="uptime-fill"
                                style="width: ${Math.min(park.uptime_percentage || 0, 100)}%"
                            ></div>
                        </div>
                    </div>
                </td>
                <td class="affected-col">${park.affected_rides_count || 0}</td>
                <td class="trend-col">
                    <span class="trend-indicator ${trendClass}">
                        ${trendIcon} ${trendText}
                    </span>
                </td>
            </tr>
        `;
    }

    /**
     * Get trend CSS class based on percentage
     */
    getTrendClass(trendPercentage) {
        if (trendPercentage === null || trendPercentage === undefined) return 'trend-neutral';
        if (trendPercentage > 0) return 'trend-worse';  // Downtime increased (bad)
        if (trendPercentage < 0) return 'trend-better';  // Downtime decreased (good)
        return 'trend-neutral';
    }

    /**
     * Get trend icon based on percentage
     */
    getTrendIcon(trendPercentage) {
        if (trendPercentage === null || trendPercentage === undefined) return '—';
        if (trendPercentage > 0) return '↑';  // Downtime increased (bad)
        if (trendPercentage < 0) return '↓';  // Downtime decreased (good)
        return '→';
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
                    this.fetchRankings();
                }
            });
        });

        // Filter buttons
        const filterBtns = this.container.querySelectorAll('.filter-btn');
        filterBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const filter = btn.dataset.filter;
                if (filter !== this.state.filter) {
                    this.state.filter = filter;
                    this.fetchRankings();
                }
            });
        });

        // Weighted toggle
        const weightedToggle = this.container.querySelector('#weighted-toggle');
        if (weightedToggle) {
            weightedToggle.addEventListener('change', (e) => {
                this.state.weighted = e.target.checked;
                this.fetchRankings();
            });
        }

        // Retry button (if error state)
        const retryBtn = this.container.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.fetchRankings();
            });
        }
    }
}

// Initialize when view is loaded
window.ParkRankings = ParkRankings;
