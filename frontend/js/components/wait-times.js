/**
 * Theme Park Hall of Shame - Wait Times Component
 * Displays current wait times with multiple display modes (Live, 7-Day Average, Peak Times)
 */

class WaitTimes {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            mode: 'live',
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
            this.fetchWaitTimes(),
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
     * Fetch wait times from API
     */
    async fetchWaitTimes() {
        this.setState({ loading: true, error: null });

        try {
            const params = {
                mode: this.state.mode,
                filter: this.state.filter,
                limit: this.state.limit
            };

            const response = await this.apiClient.get('/rides/waittimes', params);

            if (response.success) {
                this.setState({
                    data: response,
                    loading: false
                });
                this.updateLastUpdateTime();
            } else {
                throw new Error(response.error || 'Failed to fetch wait times');
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
     * Render the component
     */
    render() {
        if (!this.container) return;

        this.container.innerHTML = `
            <div class="wait-times-view">
                ${this.renderAggregateStats()}

                <div class="section-header">
                    <div class="section-marker" style="background: var(--gold);"></div>
                    <h2 class="section-title">Longest Wait Times</h2>
                </div>

                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Get mode description text
     */
    getModeDescription() {
        const descriptions = {
            'live': 'Current live wait times sorted by longest waits. Updates every 10 minutes.',
            '7day-average': 'Average wait times over the past 7 days sorted by longest averages.',
            'peak-times': 'Peak wait times from the past 7 days sorted by highest recorded waits.'
        };
        return descriptions[this.state.mode] || '';
    }

    /**
     * Render mode controls
     */
    renderControls() {
        return `
            <div class="rankings-controls">
                <div class="control-group">
                    <label>Display Mode:</label>
                    <div class="button-group">
                        <button
                            class="mode-btn ${this.state.mode === 'live' ? 'active' : ''}"
                            data-mode="live"
                        >Live</button>
                        <button
                            class="mode-btn ${this.state.mode === '7day-average' ? 'active' : ''}"
                            data-mode="7day-average"
                        >7 Day Average</button>
                        <button
                            class="mode-btn ${this.state.mode === 'peak-times' ? 'active' : ''}"
                            data-mode="peak-times"
                        >Peak Times</button>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render main content (loading, error, or wait times table)
     */
    renderContent() {
        if (this.state.loading) {
            return `
                <div class="loading-state">
                    <div class="spinner"></div>
                    <p>Loading wait times...</p>
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
            return this.renderWaitTimesTable(this.state.data.data);
        }

        return `
            <div class="empty-state">
                <p>No wait time data available</p>
            </div>
        `;
    }

    /**
     * Render wait times table
     */
    renderWaitTimesTable(rides) {
        if (!rides || rides.length === 0) {
            return `
                <div class="empty-state">
                    <p>No rides found for the selected filters</p>
                </div>
            `;
        }

        return `
            <div class="rankings-table-container">
                <table class="rankings-table wait-times-table">
                    <thead>
                        <tr>
                            <th class="rank-col">Rank</th>
                            <th class="ride-col">Ride</th>
                            <th class="tier-col">Tier</th>
                            <th class="park-col">Park</th>
                            <th class="wait-col">${this.getWaitColumnHeader()}</th>
                            ${this.state.mode !== 'live' ? '<th class="avg-col">7-Day Avg</th>' : ''}
                            <th class="status-col">Status</th>
                            <th class="trend-col">Trend</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rides.map(ride => this.renderWaitTimeRow(ride)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Get wait column header based on mode
     */
    getWaitColumnHeader() {
        const headers = {
            'live': 'Current Wait',
            '7day-average': '7-Day Average',
            'peak-times': 'Peak Wait'
        };
        return headers[this.state.mode] || 'Wait Time';
    }

    /**
     * Render a single wait time row
     */
    renderWaitTimeRow(ride) {
        const trendPct = ride.trend_percentage !== null && ride.trend_percentage !== undefined
            ? Number(ride.trend_percentage) : null;
        const trendClass = this.getTrendClass(trendPct);
        const trendIcon = this.getTrendIcon(trendPct);
        const trendText = trendPct !== null
            ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
            : 'N/A';

        const statusBadge = this.getStatusBadge(ride.current_is_open);
        const tierBadge = this.getTierBadge(ride.tier);

        // Determine wait time value based on mode
        let waitTimeValue = 0;
        if (this.state.mode === 'live') {
            waitTimeValue = ride.current_wait_minutes || 0;
        } else if (this.state.mode === '7day-average') {
            waitTimeValue = ride.avg_wait_7days || 0;
        } else {  // peak-times
            waitTimeValue = ride.peak_wait_7days || 0;
        }

        return `
            <tr class="wait-time-row ${ride.rank <= 5 ? 'top-five' : ''}">
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
                <td class="wait-col">
                    <span class="wait-value">${this.formatWaitTime(waitTimeValue)}</span>
                </td>
                ${this.state.mode !== 'live' ? `
                    <td class="avg-col">
                        <span class="wait-value">${this.formatWaitTime(ride.avg_wait_7days || 0)}</span>
                    </td>
                ` : ''}
                <td class="status-col">
                    ${statusBadge}
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
     * Format wait time into readable string
     */
    formatWaitTime(minutes) {
        if (minutes === null || minutes === undefined || minutes === 0) return '0 min';

        const roundedMinutes = Math.round(minutes);
        return `${roundedMinutes} min`;
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
            this.fetchWaitTimes();
        }
    }

    /**
     * Attach event listeners to controls
     */
    attachEventListeners() {
        // Mode buttons
        const modeBtns = this.container.querySelectorAll('.mode-btn');
        modeBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const mode = btn.dataset.mode;
                if (mode !== this.state.mode) {
                    this.state.mode = mode;
                    this.fetchWaitTimes();
                }
            });
        });

        // Retry button (if error state)
        const retryBtn = this.container.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.fetchWaitTimes();
            });
        }
    }
}

// Initialize when view is loaded
window.WaitTimes = WaitTimes;
