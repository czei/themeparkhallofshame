/**
 * Theme Park Hall of Shame - Ride Performance Component
 * Displays individual ride downtime rankings with status badges and tier indicators
 */

class RidePerformance {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: '7days',
            filter: initialFilter,
            limit: 100,
            loading: false,
            error: null,
            data: null,
            aggregateStats: null
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await Promise.all([
            this.fetchRidePerformance(),
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
     * Fetch ride performance rankings from API
     */
    async fetchRidePerformance() {
        this.setState({ loading: true, error: null });

        try {
            const params = {
                period: this.state.period,
                filter: this.state.filter,
                limit: this.state.limit
            };

            const response = await this.apiClient.get('/rides/downtime', params);

            if (response.success) {
                this.setState({
                    data: response,
                    loading: false
                });
                this.updateLastUpdateTime();
            } else {
                throw new Error(response.error || 'Failed to fetch ride performance');
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
            <div class="ride-performance-view">
                ${this.renderAggregateStats()}

                <div class="section-header">
                    <div class="section-marker" style="background: var(--turquoise);"></div>
                    <h2 class="section-title">Individual Ride Performance</h2>
                </div>

                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render period controls
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
                    <p>Loading ride performance...</p>
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
            return this.renderRideTable(this.state.data.data);
        }

        return `
            <div class="empty-state">
                <p>No ride performance data available</p>
            </div>
        `;
    }

    /**
     * Render ride performance table
     */
    renderRideTable(rides) {
        if (!rides || rides.length === 0) {
            return `
                <div class="empty-state">
                    <p>No rides found for the selected filters</p>
                </div>
            `;
        }

        return `
            <div class="data-container">
                <div class="table-header">Highest Downtime Attractions</div>
                <table class="rankings-table ride-table">
                    <thead>
                        <tr>
                            <th class="rank-col">Rank</th>
                            <th class="ride-col">Ride</th>
                            <th class="tier-col">Tier</th>
                            <th class="park-col">Park</th>
                            <th class="status-col">Status</th>
                            <th class="downtime-col">Downtime</th>
                            <th class="uptime-col">Uptime %</th>
                            <th class="trend-col">Trend</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rides.map(ride => this.renderRideRow(ride)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single ride row
     */
    renderRideRow(ride) {
        const trendPct = ride.trend_percentage !== null && ride.trend_percentage !== undefined
            ? Number(ride.trend_percentage) : null;
        const trendClass = this.getTrendClass(trendPct);
        const trendIcon = this.getTrendIcon(trendPct);
        const trendText = trendPct !== null
            ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
            : 'N/A';

        const statusBadge = this.getStatusBadge(ride.current_is_open);
        const tierBadge = this.getTierBadge(ride.tier);

        return `
            <tr class="ride-row ${ride.rank <= 5 ? 'top-five' : ''}">
                <td class="rank-col">
                    <span class="rank-number">${ride.rank}</span>
                </td>
                <td class="ride-col">
                    <a
                        href="${ride.queue_times_url}"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="ride-link"
                    >
                        ${this.escapeHtml(ride.ride_name || 'Unknown Ride')}
                        <span class="external-icon">↗</span>
                    </a>
                </td>
                <td class="tier-col">
                    ${tierBadge}
                </td>
                <td class="park-col">
                    <div class="park-info">
                        <div class="park-name">${this.escapeHtml(ride.park_name || 'Unknown Park')}</div>
                        <div class="park-location">${this.escapeHtml(ride.location || '')}</div>
                    </div>
                </td>
                <td class="status-col">
                    ${statusBadge}
                </td>
                <td class="downtime-col">
                    <span class="downtime-value">
                        ${this.formatHours(ride.downtime_hours || 0)}
                    </span>
                </td>
                <td class="uptime-col">
                    <div class="uptime-display">
                        <span class="uptime-percentage">${Number(ride.uptime_percentage || 0).toFixed(1)}%</span>
                        <div class="uptime-bar">
                            <div
                                class="uptime-fill"
                                style="width: ${Math.min(Number(ride.uptime_percentage) || 0, 100)}%"
                            ></div>
                        </div>
                    </div>
                </td>
                <td class="trend-col">
                    <span class="trend-indicator ${trendClass}">
                        ${trendIcon} ${trendText}
                    </span>
                </td>
            </tr>
        `;
    }

    /**
     * Get status badge HTML
     */
    getStatusBadge(isOpen) {
        if (isOpen === null || isOpen === undefined) {
            return '<span class="status-badge status-unknown">Unknown</span>';
        }

        if (isOpen) {
            return '<span class="status-badge status-running">Running</span>';
        } else {
            return '<span class="status-badge status-down">Down</span>';
        }
    }

    /**
     * Get tier badge HTML
     */
    getTierBadge(tier) {
        if (!tier) {
            return '<span class="tier-badge tier-unknown">?</span>';
        }

        const tierLabels = {
            1: 'Tier 1',
            2: 'Tier 2',
            3: 'Tier 3'
        };

        return `<span class="tier-badge tier-${tier}" title="${tierLabels[tier]} - ${this.getTierDescription(tier)}">${tier}</span>`;
    }

    /**
     * Get tier description
     */
    getTierDescription(tier) {
        const descriptions = {
            1: 'Major E-ticket attractions',
            2: 'Standard attractions',
            3: 'Minor attractions'
        };
        return descriptions[tier] || 'Unclassified';
    }

    /**
     * Get trend CSS class based on percentage
     */
    getTrendClass(trendPercentage) {
        if (trendPercentage === null || trendPercentage === undefined) return 'trend-neutral';
        if (trendPercentage > 0) return 'trend-worse';
        if (trendPercentage < 0) return 'trend-better';
        return 'trend-neutral';
    }

    /**
     * Get trend icon based on percentage
     */
    getTrendIcon(trendPercentage) {
        if (trendPercentage === null || trendPercentage === undefined) return '—';
        if (trendPercentage > 0) return '↑';
        if (trendPercentage < 0) return '↓';
        return '→';
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
            this.fetchRidePerformance();
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
                    this.fetchRidePerformance();
                }
            });
        });

        // Retry button (if error state)
        const retryBtn = this.container.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.fetchRidePerformance();
            });
        }
    }
}

// Initialize when view is loaded
window.RidePerformance = RidePerformance;
