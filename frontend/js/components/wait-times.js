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
            sortBy: 'avg',        // 'avg' or 'max'
            parkLimit: 50,
            rideLimit: 100,
            loading: false,
            error: null,
            parkData: null,
            rideData: null,
            aggregateStats: null,
            statusSummary: null
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

            const statusParams = {
                filter: this.state.filter
            };

            const [parkResponse, rideResponse, aggregateResponse, statusResponse] = await Promise.all([
                this.apiClient.get('/parks/waittimes', parkParams),
                this.apiClient.get('/rides/waittimes', rideParams),
                this.apiClient.get('/parks/downtime', {
                    period: 'today',
                    filter: this.state.filter,
                    limit: 1
                }),
                this.apiClient.get('/live/status-summary', statusParams)
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
     * Render aggregate statistics with 5 panels showing ride status breakdown
     */
    renderAggregateStats() {
        const stats = this.state.aggregateStats || {};
        const status = this.state.statusSummary || {};

        return `
            <div class="stats-grid stats-grid-5">
                <div class="stat-block stat-parks" title="Number of theme parks being monitored in the current filter">
                    <div class="stat-label">Parks Tracked</div>
                    <div class="stat-value">${stats.total_parks_tracked || 0}</div>
                </div>
                <div class="stat-block stat-operating" title="Rides currently running and accepting guests at open parks">
                    <div class="stat-label">Rides Operating</div>
                    <div class="stat-value">${status.OPERATING || 0}</div>
                </div>
                <div class="stat-block stat-down" title="Rides experiencing unscheduled breakdowns or technical issues">
                    <div class="stat-label">Rides Down</div>
                    <div class="stat-value">${status.DOWN || 0}</div>
                </div>
                <div class="stat-block stat-closed" title="Rides on scheduled closure (weather, capacity, seasonal) at open parks">
                    <div class="stat-label">Rides Closed</div>
                    <div class="stat-value">${status.CLOSED || 0}</div>
                </div>
                <div class="stat-block stat-repairs" title="Rides undergoing extended refurbishment or major maintenance">
                    <div class="stat-label">Rides Repairs</div>
                    <div class="stat-value">${status.REFURBISHMENT || 0}</div>
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
            'live': 'Live',
            'today': "Today's",
            'yesterday': "Yesterday's",
            'last_week': 'Last Week',
            'last_month': 'Last Month'
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

        // Sort parks based on current sort column
        const sortedParks = [...parks].sort((a, b) => {
            const aVal = this.state.sortBy === 'avg'
                ? (a.avg_wait_minutes || 0)
                : (a.peak_wait_minutes || 0);
            const bVal = this.state.sortBy === 'avg'
                ? (b.avg_wait_minutes || 0)
                : (b.peak_wait_minutes || 0);
            return bVal - aVal; // Descending
        });

        // Re-assign ranks after sorting
        sortedParks.forEach((park, idx) => { park.rank = idx + 1; });

        return `
            <div class="data-container">
                <table class="rankings-table wait-times-table">
                    <thead>
                        <tr>
                            <th class="rank-col" title="Position based on average wait time (higher rank = longer waits)">Rank</th>
                            <th class="park-col" title="Theme park name">Park</th>
                            <th class="location-col" title="Geographic location of the park">Location</th>
                            <th class="wait-col sortable ${this.state.sortBy === 'avg' ? 'sorted' : ''}" data-sort="avg" title="Average wait time across all rides (click to sort)">Avg Wait ${this.state.sortBy === 'avg' ? '▼' : ''}</th>
                            <th class="wait-col sortable ${this.state.sortBy === 'max' ? 'sorted' : ''}" data-sort="max" title="Longest single ride wait time recorded (click to sort)">Max Wait ${this.state.sortBy === 'max' ? '▼' : ''}</th>
                            <th class="rides-col" title="Number of rides currently reporting wait times">Rides</th>
                            <th class="trend-col" title="Change in average wait time compared to previous period. Positive (+) = longer waits">Trend</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${sortedParks.map(park => this.renderParkRow(park)).join('')}
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

        // Park status badge - show "Park Closed" when all rides have wait_time = 0
        const parkStatusBadge = this.getParkStatusBadge(park.park_is_open);

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
                    ${parkStatusBadge}
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

        // Sort rides based on current sort column
        const sortedRides = [...rides].sort((a, b) => {
            const aVal = this.state.sortBy === 'avg'
                ? (a.avg_wait_minutes || 0)
                : (a.peak_wait_minutes || 0);
            const bVal = this.state.sortBy === 'avg'
                ? (b.avg_wait_minutes || 0)
                : (b.peak_wait_minutes || 0);
            return bVal - aVal; // Descending
        });

        // Re-assign ranks after sorting
        sortedRides.forEach((ride, idx) => { ride.rank = idx + 1; });

        // Only show status column for live/today (where it makes sense)
        const showStatusColumn = this.state.period === 'live' || this.state.period === 'today';

        // Dynamic column headers based on period
        const avgLabel = this.getColumnLabel('Avg');
        const maxLabel = this.getColumnLabel('Max');

        return `
            <div class="data-container">
                <table class="rankings-table wait-times-table">
                    <thead>
                        <tr>
                            <th class="rank-col" title="Position based on wait time (higher rank = longer waits)">Rank</th>
                            <th class="ride-col" title="Ride or attraction name">Ride</th>
                            <th class="tier-col" title="Importance tier: Tier 1 = flagship attractions, Tier 2 = major rides, Tier 3 = standard attractions">Tier</th>
                            <th class="park-col" title="Theme park where the ride is located">Park</th>
                            <th class="wait-col sortable ${this.state.sortBy === 'avg' ? 'sorted' : ''}" data-sort="avg" title="Average wait time for this ride (click to sort)">${avgLabel} ${this.state.sortBy === 'avg' ? '▼' : ''}</th>
                            <th class="wait-col sortable ${this.state.sortBy === 'max' ? 'sorted' : ''}" data-sort="max" title="Longest wait time recorded for this ride (click to sort)">${maxLabel} ${this.state.sortBy === 'max' ? '▼' : ''}</th>
                            ${showStatusColumn ? '<th class="status-col" title="Current operating status: Operating, Down (breakdown), Closed (scheduled), or Refurbishment">Status</th>' : ''}
                            <th class="trend-col" title="Change in average wait time compared to previous period. Positive (+) = longer waits">Trend</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${sortedRides.map(ride => this.renderRideRow(ride, showStatusColumn)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Get column label - simplified to avoid redundancy with table title
     * Table title already shows the period (e.g., "Yesterday's Wait Time Rankings")
     */
    getColumnLabel(baseLabel) {
        return `${baseLabel} Wait`;
    }

    /**
     * Render a single ride wait time row
     */
    renderRideRow(ride, showStatusColumn = true) {
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
                ${showStatusColumn ? `<td class="status-col">${statusBadge}</td>` : ''}
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
        // Show "Park Closed" when all rides have wait_time = 0
        if (parkIsOpen === false || parkIsOpen === 0) {
            return '<span class="status-badge status-closed">Park Closed</span>';
        }
        // Don't show any badge when park is operating
        return '';
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

        // Sortable column headers
        const sortHeaders = this.container.querySelectorAll('.sortable');
        sortHeaders.forEach(header => {
            header.addEventListener('click', () => {
                const sortBy = header.dataset.sort;
                if (sortBy !== this.state.sortBy) {
                    this.setState({ sortBy });
                }
            });
        });
    }
}

// Initialize when view is loaded
window.WaitTimes = WaitTimes;
