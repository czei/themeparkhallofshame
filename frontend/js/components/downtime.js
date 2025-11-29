/**
 * Theme Park Hall of Shame - Downtime Component
 * Combined view showing both park and ride downtime rankings
 */

class Downtime {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: '7days',  // Default to 7 days for more meaningful data (especially early in UTC day)
            filter: initialFilter,
            entityType: 'parks',  // 'parks' or 'rides'
            parkLimit: 50,
            rideLimit: 100,
            loading: false,
            error: null,
            parkData: null,
            rideData: null,
            aggregateStats: null,
            statusSummary: null
        };
        // Initialize park details modal
        this.parkDetailsModal = null;
        if (window.ParkDetailsModal) {
            this.parkDetailsModal = new window.ParkDetailsModal(apiClient);
        }
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await this.fetchAllData();
    }

    /**
     * Fetch both park and ride downtime data in parallel
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

            const statusParams = {
                filter: this.state.filter
            };

            const [parkResponse, rideResponse, statusResponse] = await Promise.all([
                this.apiClient.get('/parks/downtime', parkParams),
                this.apiClient.get('/rides/downtime', rideParams),
                this.apiClient.get('/live/status-summary', statusParams)
            ]);

            const newState = { loading: false };

            if (parkResponse.success) {
                newState.parkData = parkResponse;
                // Get aggregate stats from park response
                if (parkResponse.aggregate_stats) {
                    newState.aggregateStats = parkResponse.aggregate_stats;
                }
            }

            if (rideResponse.success) {
                newState.rideData = rideResponse;
            }

            if (statusResponse.success) {
                newState.statusSummary = statusResponse.status_summary;
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
     * Render aggregate statistics with 5 panels showing ride status breakdown
     */
    renderAggregateStats() {
        const stats = this.state.aggregateStats || {};
        const status = this.state.statusSummary || {};

        return `
            <div class="stats-grid stats-grid-5">
                <div class="stat-block stat-parks">
                    <div class="stat-label">Parks Tracked</div>
                    <div class="stat-value">${stats.total_parks_tracked || 0}</div>
                </div>
                <div class="stat-block stat-operating">
                    <div class="stat-label">Rides Operating</div>
                    <div class="stat-value">${status.OPERATING || 0}</div>
                </div>
                <div class="stat-block stat-down">
                    <div class="stat-label">Rides Down</div>
                    <div class="stat-value">${status.DOWN || 0}</div>
                </div>
                <div class="stat-block stat-closed">
                    <div class="stat-label">Rides Closed</div>
                    <div class="stat-value">${status.CLOSED || 0}</div>
                </div>
                <div class="stat-block stat-repairs">
                    <div class="stat-label">Rides Repairs</div>
                    <div class="stat-value">${status.REFURBISHMENT || 0}</div>
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
            <div class="downtime-view">
                ${this.renderAggregateStats()}
                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render main content (loading, error, or toggle + single table)
     */
    renderContent() {
        if (this.state.loading) {
            return `
                <div class="loading-state">
                    <div class="spinner"></div>
                    <p>Loading downtime data...</p>
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
                <h2 class="section-title">${this.getPeriodTitle('Downtime Rankings')}</h2>
            </div>
            ${this.state.entityType === 'parks'
                ? this.renderParkTable()
                : this.renderRideTable()}
        `;
    }

    /**
     * Render park rankings table
     */
    renderParkTable() {
        const parks = this.state.parkData?.data;

        if (!parks || parks.length === 0) {
            return `
                <div class="empty-state hall-of-shame-empty">
                    <h3>The Hall of Shame is Empty!</h3>
                    <p>All operational parks ran smoothly during this period.</p>
                    <p class="empty-state-hint">Check back later or try a different time period.</p>
                </div>
            `;
        }

        return `
            <div class="data-container">
                <table class="rankings-table">
                    <thead>
                        <tr>
                            <th class="rank-col">Rank</th>
                            <th class="park-col">Park</th>
                            <th class="shame-col" title="Weighted downtime per ride weight point. Higher = worse. Tier 1 rides count 3x, Tier 2 count 2x, Tier 3 count 1x.">Shame Score</th>
                            <th class="location-col">Location</th>
                            <th class="status-col">Status</th>
                            <th class="downtime-col">Downtime</th>
                            <th class="uptime-col">Uptime %</th>
                            <th class="affected-col">Affected Rides</th>
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
     * Render a single park row
     */
    renderParkRow(park) {
        const trendPct = park.trend_percentage !== null && park.trend_percentage !== undefined
            ? Number(park.trend_percentage) : null;
        const trendClass = this.getTrendClass(trendPct);
        const trendIcon = this.getTrendIcon(trendPct);
        const trendText = trendPct !== null
            ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
            : 'N/A';

        // Park status badge - show "Park Closed" when all rides have wait_time = 0
        const parkStatusBadge = this.getParkStatusBadge(park.park_is_open);

        return `
            <tr class="park-row ${park.rank <= 3 ? 'top-three' : ''}">
                <td class="rank-col">
                    <span class="rank-badge ${park.rank === 1 ? 'rank-1' : park.rank === 2 ? 'rank-2' : park.rank === 3 ? 'rank-3' : ''}">
                        ${park.rank}
                    </span>
                </td>
                <td class="park-col">
                    <div class="park-name-cell">
                        <span class="park-name">${this.escapeHtml(park.park_name || park.name || 'Unknown Park')}</span>
                        <div class="park-actions">
                            <button
                                class="park-details-btn"
                                data-park-id="${park.park_id}"
                                data-park-name="${this.escapeHtml(park.park_name || park.name || 'Unknown Park')}"
                                title="View park details"
                            >Details</button>
                            <a
                                href="${park.queue_times_url}"
                                target="_blank"
                                rel="noopener noreferrer"
                                class="park-external-link"
                                title="View on Queue-Times.com"
                            >
                                <span class="external-icon">↗</span>
                            </a>
                        </div>
                    </div>
                </td>
                <td class="shame-col">
                    <span class="shame-score ${this.getShameClass(park.shame_score)}">${park.shame_score !== null && park.shame_score !== undefined ? Number(park.shame_score).toFixed(2) : 'N/A'}</span>
                </td>
                <td class="location-col">${this.escapeHtml(park.location || 'Unknown')}</td>
                <td class="status-col">
                    ${parkStatusBadge}
                </td>
                <td class="downtime-col">
                    <span class="downtime-value">
                        ${this.formatHours(park.total_downtime_hours || 0)}
                    </span>
                </td>
                <td class="uptime-col">
                    <div class="uptime-display">
                        <span class="uptime-percentage">${Number(park.uptime_percentage || 0).toFixed(1)}%</span>
                        <div class="uptime-bar">
                            <div
                                class="uptime-fill"
                                style="width: ${Math.min(Number(park.uptime_percentage) || 0, 100)}%"
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
     * Render ride performance table
     */
    renderRideTable() {
        const rides = this.state.rideData?.data;

        if (!rides || rides.length === 0) {
            return `
                <div class="empty-state hall-of-shame-empty">
                    <h3>The Hall of Shame is Empty!</h3>
                    <p>All operational rides ran smoothly during this period.</p>
                    <p class="empty-state-hint">Check back later or try a different time period.</p>
                </div>
            `;
        }

        return `
            <div class="data-container">
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

        const statusBadge = this.getStatusBadge(ride.current_is_open, ride.park_is_open);
        const tierBadge = this.getTierBadge(ride.tier);

        return `
            <tr class="ride-row ${ride.rank <= 3 ? 'top-three' : ''}">
                <td class="rank-col">
                    <span class="rank-badge ${ride.rank === 1 ? 'rank-1' : ride.rank === 2 ? 'rank-2' : ride.rank === 3 ? 'rank-3' : ''}">
                        ${ride.rank}
                    </span>
                </td>
                <td class="ride-col">
                    <div class="ride-name-cell">
                        <span class="ride-name">${this.escapeHtml(ride.ride_name || 'Unknown Ride')}</span>
                        <div class="ride-actions">
                            <a
                                href="${ride.queue_times_url}"
                                target="_blank"
                                rel="noopener noreferrer"
                                class="ride-external-link"
                                title="View on Queue-Times.com"
                            >
                                <span class="external-icon">↗</span>
                            </a>
                        </div>
                    </div>
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
    getStatusBadge(isOpen, parkIsOpen) {
        // Park closed takes priority - show "Park Closed" instead of misleading "Down"
        if (parkIsOpen === false || parkIsOpen === 0) {
            return '<span class="status-badge status-closed">Park Closed</span>';
        }

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
     * Get park status badge HTML (for parks table)
     */
    getParkStatusBadge(parkIsOpen) {
        // Show "Closed" when all rides have wait_time = 0
        if (parkIsOpen === false || parkIsOpen === 0) {
            return '<span class="status-badge status-closed">Closed</span>';
        }
        // Show "Open" when park is operating
        return '<span class="status-badge status-running">Open</span>';
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
     * Get CSS class for shame score coloring
     * Higher scores = more shame = worse
     */
    getShameClass(shameScore) {
        if (shameScore === null || shameScore === undefined) return '';
        const score = Number(shameScore);
        if (isNaN(score)) return '';

        // Thresholds for shame levels (these are weighted downtime hours per weight point)
        if (score >= 1.0) return 'shame-high';      // 1+ hour weighted downtime per point = very bad
        if (score >= 0.5) return 'shame-medium';    // 0.5-1 hour = concerning
        if (score >= 0.1) return 'shame-low';       // 0.1-0.5 = minor issues
        return 'shame-none';                         // < 0.1 = negligible
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

        // Park details buttons
        const detailsBtns = this.container.querySelectorAll('.park-details-btn');
        detailsBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const parkId = parseInt(btn.dataset.parkId);
                const parkName = btn.dataset.parkName;
                if (this.parkDetailsModal && parkId) {
                    this.parkDetailsModal.open(parkId, parkName);
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
window.Downtime = Downtime;
