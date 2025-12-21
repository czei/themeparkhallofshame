/**
 * Theme Park Hall of Shame - Downtime Component
 * Combined view showing both park and ride downtime rankings
 */

class Downtime {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: 'last_week',  // Default to last week for calendar-based reporting
            filter: initialFilter,
            entityType: 'parks',  // 'parks' or 'rides'
            parkLimit: 50,
            rideLimit: 100,
            loading: false,
            error: null,
            parkData: null,
            rideData: null,
            aggregateStats: null,
            statusSummary: null,
            sortBy: 'shame_score',  // Default sort column for parks
            rideSortBy: 'downtime_hours'  // Default sort column for rides
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
                limit: this.state.parkLimit,
                sort_by: this.state.sortBy
            };

            const rideParams = {
                period: this.state.period,
                filter: this.state.filter,
                limit: this.state.rideLimit,
                sort_by: this.state.rideSortBy
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
            'live': 'Live',
            'today': 'Today',
            'last_week': 'Last Week',
            'last_month': 'Last Month'
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
                            <th class="rank-col" title="Position in the Hall of Shame based on current sort order">Rank</th>
                            <th class="park-col" title="Theme park name">Park</th>
                            <th class="shame-col sortable ${this.state.sortBy === 'shame_score' ? 'sorted' : ''}"
                                data-sort="shame_score"
                                title="Weighted downtime per ride weight point. Higher = worse. Click to sort.">
                                Shame Score ${this.getSortIndicator('shame_score')}
                            </th>
                            <th class="location-col" title="Geographic location of the park">Location</th>
                            ${this.state.period === 'live' ? '<th class="status-col" title="Whether the park is currently open or closed">Status</th>' : ''}
                            <th class="downtime-col sortable ${this.state.sortBy === 'total_downtime_hours' ? 'sorted' : ''}"
                                data-sort="total_downtime_hours"
                                title="Total accumulated ride downtime hours during the selected period. Click to sort.">
                                Total Ride-Hours Down ${this.getSortIndicator('total_downtime_hours')}
                            </th>
                            <th class="uptime-col sortable ${this.state.sortBy === 'uptime_percentage' ? 'sorted' : ''}"
                                data-sort="uptime_percentage"
                                title="Percentage of time rides were operational. Click to sort.">
                                Uptime % ${this.getSortIndicator('uptime_percentage')}
                            </th>
                            <th class="affected-col sortable ${this.state.sortBy === 'rides_down' ? 'sorted' : ''}"
                                data-sort="rides_down"
                                title="Number of rides currently down right now. Click to sort.">
                                Rides Down ${this.getSortIndicator('rides_down')}
                            </th>
                            <th class="trend-col" title="Change in downtime compared to previous period. Positive (+) = more downtime = worse performance">Trend</th>
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
                    <a href="park-detail.html?park_id=${park.park_id}&period=${this.state.period}" class="park-link">
                        ${this.escapeHtml(park.park_name || park.name || 'Unknown Park')}
                    </a>
                </td>
                <td class="shame-col">
                    <span class="shame-score ${this.getShameClass(park.shame_score)} clickable-shame"
                          data-park-id="${park.park_id}"
                          data-park-name="${this.escapeHtml(park.park_name || park.name || 'Unknown Park')}"
                          title="Explain Shame Score">
                        ${park.shame_score !== null && park.shame_score !== undefined ? Number(park.shame_score).toFixed(2) : 'N/A'}
                    </span>
                </td>
                <td class="location-col">${this.escapeHtml(park.location || 'Unknown')}</td>
                ${this.state.period === 'live' ? `<td class="status-col">${parkStatusBadge}</td>` : ''}
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
                <td class="affected-col">${park.rides_down || 0}</td>
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
                            <th class="rank-col" title="Position in the Hall of Shame based on downtime (lower rank = worse performance)">Rank</th>
                            <th class="ride-col" title="Ride or attraction name">Ride</th>
                            <th class="tier-col" title="Importance tier: Tier 1 = flagship attractions (3x weight), Tier 2 = major rides (2x weight), Tier 3 = standard attractions (1x weight)">Tier</th>
                            <th class="park-col" title="Theme park where the ride is located">Park</th>
                            ${this.state.period === 'live' ? `
                            <th class="status-col sortable ${this.state.rideSortBy === 'current_is_open' ? 'sorted' : ''}"
                                data-ride-sort="current_is_open"
                                title="Current operating status. Click to sort (Down first).">
                                Status ${this.getRideSortIndicator('current_is_open')}
                            </th>` : ''}
                            <th class="downtime-col sortable ${this.state.rideSortBy === 'downtime_hours' ? 'sorted' : ''}"
                                data-ride-sort="downtime_hours"
                                title="Total time the ride was non-operational. Click to sort (most downtime first).">
                                Total Ride-Hours Down ${this.getRideSortIndicator('downtime_hours')}
                            </th>
                            <th class="uptime-col sortable ${this.state.rideSortBy === 'uptime_percentage' ? 'sorted' : ''}"
                                data-ride-sort="uptime_percentage"
                                title="Percentage of time operational. Click to sort (lowest uptime first).">
                                Uptime % ${this.getRideSortIndicator('uptime_percentage')}
                            </th>
                            <th class="trend-col sortable ${this.state.rideSortBy === 'trend_percentage' ? 'sorted' : ''}"
                                data-ride-sort="trend_percentage"
                                title="Change in downtime vs previous period. Click to sort (most increased first).">
                                Trend ${this.getRideSortIndicator('trend_percentage')}
                            </th>
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

        // Handle both LIVE (is_down) and TODAY (current_is_open) field names
        const isOpen = ride.current_is_open !== undefined ? ride.current_is_open : (ride.is_down !== undefined ? !ride.is_down : null);
        const statusBadge = this.getStatusBadge(isOpen, ride.park_is_open);
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
                        <a
                            href="ride-detail.html?ride_id=${ride.ride_id}&period=${this.state.period === 'live' ? 'today' : this.state.period}"
                            class="ride-name-link"
                            title="View ride details"
                        >
                            <span class="ride-name">${this.escapeHtml(ride.ride_name || 'Unknown Ride')}</span>
                        </a>
                        <div class="ride-actions">
                            <a
                                href="ride-detail.html?ride_id=${ride.ride_id}&period=${this.state.period === 'live' ? 'today' : this.state.period}"
                                class="ride-external-link"
                                title="View ride details"
                            >
                                <span class="external-icon">→</span>
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
                ${this.state.period === 'live' ? `<td class="status-col">${statusBadge}</td>` : ''}
                <td class="downtime-col">
                    <span class="downtime-value">
                        ${this.formatHours(ride.downtime_hours || ride.total_downtime_hours || 0)}
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
     * Uses centralized ShameScoreConfig
     */
    getShameClass(shameScore) {
        return ShameScoreConfig.getCssClass(shameScore);
    }

    /**
     * Get sort indicator arrow for column header
     */
    getSortIndicator(column) {
        if (this.state.sortBy !== column) {
            return '<span class="sort-indicator"></span>';
        }
        // uptime_percentage sorts ascending (higher is better), others sort descending
        const isAscending = column === 'uptime_percentage';
        return `<span class="sort-indicator active">${isAscending ? '↑' : '↓'}</span>`;
    }

    /**
     * Handle sort column click for parks table
     */
    handleSortClick(sortColumn) {
        if (sortColumn !== this.state.sortBy) {
            this.state.sortBy = sortColumn;
            this.fetchAllData();
        }
    }

    /**
     * Get sort indicator arrow for ride column header
     */
    getRideSortIndicator(column) {
        if (this.state.rideSortBy !== column) {
            return '<span class="sort-indicator"></span>';
        }
        // uptime_percentage sorts ascending (lower is worse), current_is_open sorts ascending (down first)
        // downtime_hours and trend_percentage sort descending (higher is worse)
        const isAscending = column === 'uptime_percentage' || column === 'current_is_open';
        return `<span class="sort-indicator active">${isAscending ? '↑' : '↓'}</span>`;
    }

    /**
     * Handle sort column click for rides table
     */
    handleRideSortClick(sortColumn) {
        if (sortColumn !== this.state.rideSortBy) {
            this.state.rideSortBy = sortColumn;
            this.fetchAllData();
        }
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
            lastUpdateEl.textContent = now.toLocaleTimeString('en-US', { timeZone: 'America/Los_Angeles' }) + ' PST';
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
        // Sortable column headers for park table (data-sort)
        const sortableHeaders = this.container.querySelectorAll('th.sortable[data-sort]');
        sortableHeaders.forEach(th => {
            th.addEventListener('click', () => {
                const sortColumn = th.dataset.sort;
                if (sortColumn) {
                    this.handleSortClick(sortColumn);
                }
            });
        });

        // Sortable headers for ride table (data-ride-sort)
        const rideSortableHeaders = this.container.querySelectorAll('th.sortable[data-ride-sort]');
        rideSortableHeaders.forEach(th => {
            th.addEventListener('click', () => {
                const sortColumn = th.dataset.rideSort;
                if (sortColumn) {
                    this.handleRideSortClick(sortColumn);
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

        // Clickable shame scores - open park details modal
        const shameScores = this.container.querySelectorAll('.clickable-shame');
        shameScores.forEach(span => {
            span.addEventListener('click', (e) => {
                e.preventDefault();
                const parkId = span.dataset.parkId;
                const parkName = span.dataset.parkName;
                if (this.parkDetailsModal && parkId) {
                    this.parkDetailsModal.open(parseInt(parkId), parkName, this.state.period);
                }
            });
        });
    }
}

// Initialize when view is loaded
window.Downtime = Downtime;
