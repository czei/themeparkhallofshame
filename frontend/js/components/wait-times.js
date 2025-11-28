/**
 * Theme Park Hall of Shame - Wait Times Component
 * Displays wait times sorted by longest average waits for the selected time period
 * Supports both park-level and ride-level views
 */

class WaitTimes {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: 'today',
            filter: initialFilter,
            entityType: 'rides',  // 'parks' or 'rides'
            parkLimit: 50,
            rideLimit: 100,
            loading: false,
            error: null,
            parkData: null,
            rideData: null,
            aggregateStats: null
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await this.fetchAllData();
    }

    /**
     * Fetch both park and ride wait time data in parallel
     */
    async fetchAllData() {
        this.setState({ loading: true, error: null });

        try {
            const parkParams = {
                period: this.state.period,
                filter: this.state.filter,
                limit: this.state.parkLimit
            };

            const rideParams = {
                period: this.state.period,
                filter: this.state.filter,
                limit: this.state.rideLimit
            };

            const [parkResponse, rideResponse, aggregateResponse] = await Promise.all([
                this.apiClient.get('/parks/waittimes', parkParams),
                this.apiClient.get('/rides/waittimes', rideParams),
                this.apiClient.get('/parks/downtime', {
                    period: 'today',
                    filter: this.state.filter,
                    limit: 1
                })
            ]);

            const newState = { loading: false };

            if (parkResponse.success) {
                newState.parkData = parkResponse;
            }

            if (rideResponse.success) {
                newState.rideData = rideResponse;
            }

            if (aggregateResponse.success && aggregateResponse.aggregate_stats) {
                newState.aggregateStats = aggregateResponse.aggregate_stats;
            }

            this.setState(newState);
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
                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
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
                    <p class="error-message">${this.state.error}</p>
                    <button class="retry-btn">Retry</button>
                </div>
            `;
        }

        return `
            <div class="section-header">
                <div class="entity-toggle">
                    <button class="entity-btn ${this.state.entityType === 'parks' ? 'active' : ''}"
                            data-entity="parks">Parks</button>
                    <button class="entity-btn ${this.state.entityType === 'rides' ? 'active' : ''}"
                            data-entity="rides">Rides</button>
                </div>
                <h2 class="section-title">${this.getPeriodTitle('Wait Time Rankings')}</h2>
            </div>
            ${this.state.entityType === 'parks'
                ? this.renderParkTable()
                : this.renderRideTable()}
        `;
    }

    /**
     * Render park wait times table
     */
    renderParkTable() {
        const parks = this.state.parkData?.data;

        if (!parks || parks.length === 0) {
            return `
                <div class="empty-state">
                    <p>No park wait time data available</p>
                    <p class="empty-state-hint">Parks with wait time data will appear here.</p>
                </div>
            `;
        }

        return `
            <div class="data-container">
                <table class="rankings-table wait-times-table">
                    <thead>
                        <tr>
                            <th class="rank-col">Rank</th>
                            <th class="park-col">Park</th>
                            <th class="location-col">Location</th>
                            <th class="wait-col">Avg Wait</th>
                            <th class="wait-col">Peak Wait</th>
                            <th class="rides-col">Rides</th>
                            <th class="trend-col">Trend</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${parks.map(park => this.renderParkRow(park)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single park wait time row
     */
    renderParkRow(park) {
        const trendPct = park.trend_percentage !== null && park.trend_percentage !== undefined
            ? Number(park.trend_percentage) : null;
        const trendClass = this.getTrendClass(trendPct);
        const trendIcon = this.getTrendIcon(trendPct);
        const trendText = trendPct !== null
            ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
            : 'N/A';

        return `
            <tr class="park-row ${park.rank <= 5 ? 'top-five' : ''}">
                <td class="rank-col">
                    <span class="rank-number">${park.rank}</span>
                </td>
                <td class="park-col">
                    <a
                        href="${park.queue_times_url}"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="park-link"
                    >
                        ${this.escapeHtml(park.park_name || 'Unknown Park')}
                        <span class="external-icon">↗</span>
                    </a>
                </td>
                <td class="location-col">${this.escapeHtml(park.location || '')}</td>
                <td class="wait-col">
                    <span class="wait-value">${this.formatWaitTime(park.avg_wait_minutes || 0)}</span>
                </td>
                <td class="wait-col">
                    <span class="wait-value">${this.formatWaitTime(park.peak_wait_minutes || 0)}</span>
                </td>
                <td class="rides-col">${park.rides_reporting || 0}</td>
                <td class="trend-col">
                    <span class="trend-indicator ${trendClass}">
                        ${trendIcon} ${trendText}
                    </span>
                </td>
            </tr>
        `;
    }

    /**
     * Render ride wait times table
     */
    renderRideTable() {
        const rides = this.state.rideData?.data;

        if (!rides || rides.length === 0) {
            return `
                <div class="empty-state">
                    <p>No ride wait time data available</p>
                    <p class="empty-state-hint">Rides with wait time data will appear here.</p>
                </div>
            `;
        }

        return `
            <div class="data-container">
                <table class="rankings-table wait-times-table">
                    <thead>
                        <tr>
                            <th class="rank-col">Rank</th>
                            <th class="ride-col">Ride</th>
                            <th class="tier-col">Tier</th>
                            <th class="park-col">Park</th>
                            <th class="wait-col">Avg Wait</th>
                            <th class="wait-col">Peak Wait</th>
                            <th class="status-col">Status</th>
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
     * Render a single ride wait time row
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
                    <span class="wait-value">${this.formatWaitTime(ride.avg_wait_minutes || 0)}</span>
                </td>
                <td class="wait-col">
                    <span class="wait-value">${this.formatWaitTime(ride.peak_wait_minutes || 0)}</span>
                </td>
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
            this.fetchAllData();
        }
    }

    /**
     * Update period from global period selector (called by app.js)
     */
    updatePeriod(newPeriod) {
        if (newPeriod !== this.state.period) {
            this.state.period = newPeriod;
            this.fetchAllData();
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
                this.fetchAllData();
            });
        }
    }
}

// Initialize when view is loaded
window.WaitTimes = WaitTimes;
