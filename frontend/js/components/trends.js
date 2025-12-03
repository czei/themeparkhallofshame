/**
 * Theme Park Hall of Shame - Trends Component
 * Displays Awards (Longest Wait Times, Least Reliable Rides) and
 * performance trends showing parks/rides with uptime changes
 */

class Trends {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: 'last_week',
            filter: initialFilter,
            entityType: 'parks',  // 'parks' or 'rides'
            limit: 20,
            loading: false,
            error: null,
            // Trends data
            parksImproving: null,
            parksDeclining: null,
            ridesImproving: null,
            ridesDeclining: null,
            // Awards data
            longestWaitTimes: null,
            leastReliable: null,
            // Stats
            aggregateStats: null,
            statusSummary: null
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await Promise.all([
            this.fetchAllTrends(),
            this.fetchAggregateStats(),
            this.fetchAwardsData()
        ]);
    }

    /**
     * Fetch Awards data (Longest Wait Times, Least Reliable)
     * Now supports both parks and rides based on entityType
     */
    async fetchAwardsData() {
        try {
            const params = {
                period: this.getEffectivePeriod(),
                filter: this.state.filter,
                entity: this.state.entityType,
                limit: 10  // Top 10 for awards
            };

            const [waitTimesResponse, reliableResponse] = await Promise.all([
                this.apiClient.get('/trends/longest-wait-times', params),
                this.apiClient.get('/trends/least-reliable', params)
            ]);

            const newState = {};
            if (waitTimesResponse.success) {
                newState.longestWaitTimes = waitTimesResponse.data;
            }
            if (reliableResponse.success) {
                newState.leastReliable = reliableResponse.data;
            }

            this.setState(newState);
        } catch (error) {
            console.error('Failed to fetch awards data:', error);
        }
    }

    /**
     * Fetch aggregate stats and status summary
     */
    async fetchAggregateStats() {
        try {
            const [aggregateResponse, statusResponse] = await Promise.all([
                this.apiClient.get('/parks/downtime', {
                    period: 'today',
                    filter: this.state.filter,
                    limit: 1
                }),
                this.apiClient.get('/live/status-summary', {
                    filter: this.state.filter
                })
            ]);

            const newState = {};
            if (aggregateResponse.success && aggregateResponse.aggregate_stats) {
                newState.aggregateStats = aggregateResponse.aggregate_stats;
            }
            if (statusResponse.success) {
                newState.statusSummary = statusResponse.status_summary;
            }
            this.setState(newState);
        } catch (error) {
            console.error('Failed to fetch aggregate stats:', error);
        }
    }

    /**
     * Get effective period for API calls.
     * Trends don't support 'live' period - use 'today' instead.
     */
    getEffectivePeriod() {
        return this.state.period === 'live' ? 'today' : this.state.period;
    }

    /**
     * Fetch all trends categories from API
     */
    async fetchAllTrends() {
        this.setState({ loading: true, error: null });

        try {
            const params = {
                period: this.getEffectivePeriod(),
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
            <div class="trends-view">
                ${this.renderAggregateStats()}
                ${this.renderEntityToggle()}
                ${this.renderAwardsSection()}
                ${this.renderUptimeTrends()}
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render the Parks/Rides toggle at the top
     */
    renderEntityToggle() {
        return `
            <div class="entity-toggle-section">
                <div class="entity-toggle">
                    <button class="entity-btn ${this.state.entityType === 'parks' ? 'active' : ''}"
                            data-entity="parks">Parks</button>
                    <button class="entity-btn ${this.state.entityType === 'rides' ? 'active' : ''}"
                            data-entity="rides">Rides</button>
                </div>
            </div>
        `;
    }

    /**
     * Render the Awards section (Longest Wait Times + Least Reliable)
     */
    renderAwardsSection() {
        const entityLabel = this.state.entityType === 'parks' ? 'Parks' : 'Rides';
        return `
            <div class="awards-section">
                <div class="section-header">
                    <h2 class="section-title">${entityLabel} Awards - ${this.getPeriodLabel()}</h2>
                </div>
                <div class="awards-grid">
                    ${this.renderLongestWaitTimesTable()}
                    ${this.renderLeastReliableTable()}
                </div>
            </div>
        `;
    }

    /**
     * Get period label for section titles
     */
    getPeriodLabel() {
        const labels = {
            'live': 'Today',
            'today': 'Today',
            'last_week': 'Last Week',
            'last_month': 'Last Month'
        };
        return labels[this.state.period] || 'Last Week';
    }

    /**
     * Render Longest Wait Times awards table
     */
    renderLongestWaitTimesTable() {
        const data = this.state.longestWaitTimes;
        const isParks = this.state.entityType === 'parks';

        if (!data || data.length === 0) {
            return `
                <div class="awards-table-container">
                    <div class="awards-table-header">
                        <span class="awards-icon">⏱️</span>
                        Longest Wait Times
                    </div>
                    <div class="empty-state">
                        <p>No wait time data available for this period</p>
                    </div>
                </div>
            `;
        }

        if (isParks) {
            return `
                <div class="awards-table-container">
                    <div class="awards-table-header">
                        <span class="awards-icon">⏱️</span>
                        Longest Wait Times
                    </div>
                    <table class="rankings-table awards-table">
                        <thead>
                            <tr>
                                <th class="rank-col">#</th>
                                <th class="park-col">Park</th>
                                <th class="location-col">Location</th>
                                <th class="metric-col">Wait Hours</th>
                                <th class="metric-col">Avg Wait</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.map((park, index) => this.renderWaitTimeParkRow(park, index + 1)).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        }

        return `
            <div class="awards-table-container">
                <div class="awards-table-header">
                    <span class="awards-icon">⏱️</span>
                    Longest Wait Times
                </div>
                <table class="rankings-table awards-table">
                    <thead>
                        <tr>
                            <th class="rank-col">#</th>
                            <th class="ride-col">Ride</th>
                            <th class="park-col">Park</th>
                            <th class="metric-col">Wait Hours</th>
                            <th class="metric-col">Avg Wait</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.map((ride, index) => this.renderWaitTimeRideRow(ride, index + 1)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single wait time row for PARKS
     */
    renderWaitTimeParkRow(park, rank) {
        const medalClass = rank <= 3 ? `medal-${rank}` : '';
        const waitHours = Number(park.cumulative_wait_hours || 0).toFixed(1);
        const avgWait = Math.round(park.avg_wait_time || 0);

        return `
            <tr class="awards-row ${medalClass}">
                <td class="rank-col">
                    <span class="rank-badge ${medalClass}">${rank}</span>
                </td>
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(park.park_name || 'Unknown')}</span>
                </td>
                <td class="location-col">
                    <span class="location">${this.escapeHtml(park.location || 'Unknown')}</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value">${waitHours}h</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value">${avgWait}m</span>
                </td>
            </tr>
        `;
    }

    /**
     * Render a single wait time row for RIDES
     */
    renderWaitTimeRideRow(ride, rank) {
        const medalClass = rank <= 3 ? `medal-${rank}` : '';
        const waitHours = Number(ride.cumulative_wait_hours || 0).toFixed(1);
        const avgWait = Math.round(ride.avg_wait_time || 0);

        return `
            <tr class="awards-row ${medalClass}">
                <td class="rank-col">
                    <span class="rank-badge ${medalClass}">${rank}</span>
                </td>
                <td class="ride-col">
                    <span class="ride-name">${this.escapeHtml(ride.ride_name || 'Unknown')}</span>
                </td>
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(ride.park_name || 'Unknown')}</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value">${waitHours}h</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value">${avgWait}m</span>
                </td>
            </tr>
        `;
    }

    /**
     * Render Least Reliable awards table (Parks or Rides)
     */
    renderLeastReliableTable() {
        const data = this.state.leastReliable;
        const isParks = this.state.entityType === 'parks';
        const title = isParks ? 'Least Reliable Parks' : 'Least Reliable Rides';

        if (!data || data.length === 0) {
            return `
                <div class="awards-table-container">
                    <div class="awards-table-header">
                        <span class="awards-icon">🔧</span>
                        ${title}
                    </div>
                    <div class="empty-state">
                        <p>No reliability data available for this period</p>
                    </div>
                </div>
            `;
        }

        if (isParks) {
            return `
                <div class="awards-table-container">
                    <div class="awards-table-header">
                        <span class="awards-icon">🔧</span>
                        ${title}
                    </div>
                    <table class="rankings-table awards-table">
                        <thead>
                            <tr>
                                <th class="rank-col">#</th>
                                <th class="park-col">Park</th>
                                <th class="location-col">Location</th>
                                <th class="metric-col">Down Time</th>
                                <th class="metric-col">Uptime</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.map((park, index) => this.renderReliabilityParkRow(park, index + 1)).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        }

        return `
            <div class="awards-table-container">
                <div class="awards-table-header">
                    <span class="awards-icon">🔧</span>
                    ${title}
                </div>
                <table class="rankings-table awards-table">
                    <thead>
                        <tr>
                            <th class="rank-col">#</th>
                            <th class="ride-col">Ride</th>
                            <th class="park-col">Park</th>
                            <th class="metric-col">Down Time</th>
                            <th class="metric-col">Uptime</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.map((ride, index) => this.renderReliabilityRideRow(ride, index + 1)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }

    /**
     * Render a single reliability row for PARKS
     */
    renderReliabilityParkRow(park, rank) {
        const medalClass = rank <= 3 ? `medal-${rank}` : '';
        const downtime = Number(park.downtime_hours || 0).toFixed(1);
        const uptime = Number(park.uptime_percentage || 0).toFixed(1);

        return `
            <tr class="awards-row ${medalClass}">
                <td class="rank-col">
                    <span class="rank-badge ${medalClass}">${rank}</span>
                </td>
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(park.park_name || 'Unknown')}</span>
                </td>
                <td class="location-col">
                    <span class="location">${this.escapeHtml(park.location || 'Unknown')}</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value metric-bad">${downtime}h</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value">${uptime}%</span>
                </td>
            </tr>
        `;
    }

    /**
     * Render a single reliability row for RIDES
     */
    renderReliabilityRideRow(ride, rank) {
        const medalClass = rank <= 3 ? `medal-${rank}` : '';
        const downtime = Number(ride.downtime_hours || 0).toFixed(1);
        const uptime = Number(ride.uptime_percentage || 0).toFixed(1);

        return `
            <tr class="awards-row ${medalClass}">
                <td class="rank-col">
                    <span class="rank-badge ${medalClass}">${rank}</span>
                </td>
                <td class="ride-col">
                    <span class="ride-name">${this.escapeHtml(ride.ride_name || 'Unknown')}</span>
                </td>
                <td class="park-col">
                    <span class="park-name">${this.escapeHtml(ride.park_name || 'Unknown')}</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value metric-bad">${downtime}h</span>
                </td>
                <td class="metric-col">
                    <span class="metric-value">${uptime}%</span>
                </td>
            </tr>
        `;
    }

    /**
     * Render Uptime Trends section (loading, error, or trends tables)
     * Uses the entity toggle from renderEntityToggle() at the top level
     */
    renderUptimeTrends() {
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

        // Render section title + selected entity type trends (toggle is at top level)
        return `
            <div class="section-header">
                <h2 class="section-title">${this.getPeriodTitle('Uptime Trends')}</h2>
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
            // Show time-aware message for "today" period
            let emptyMessage = 'No significant trends found for the selected period';
            if (this.state.period === 'today' || this.state.period === 'live') {
                const hour = new Date().getHours();
                if (hour < 12) {
                    emptyMessage = "Today's data is still being collected. Check back later or try 7 Days view.";
                } else {
                    emptyMessage = 'No significant changes detected today (requires 2%+ uptime change)';
                }
            }
            return `
                <div class="data-container">
                    <div class="table-header">${tableTitle}</div>
                    <div class="empty-state">
                        <p>${emptyMessage}</p>
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
                                <th class="uptime-col">Uptime</th>
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
                                <th class="uptime-col">Uptime</th>
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
            'live': "Today's",
            'today': "Today's",
            'last_week': 'Last Week',
            'last_month': 'Last Month'
        };
        return `${periodLabels[this.state.period] || ''} ${baseTitle}`;
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
            this.fetchAwardsData();
        }
    }

    /**
     * Update period (called by app.js global period selector)
     */
    updatePeriod(newPeriod) {
        if (newPeriod !== this.state.period) {
            this.state.period = newPeriod;
            this.fetchAllTrends();
            this.fetchAwardsData();
        }
    }

    /**
     * Attach event listeners to controls
     */
    attachEventListeners() {
        // Entity toggle buttons - controls both Awards and Uptime Trends
        const entityBtns = this.container.querySelectorAll('.entity-btn');
        entityBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const newEntityType = btn.dataset.entity;
                if (newEntityType !== this.state.entityType) {
                    this.state.entityType = newEntityType;
                    // Refetch Awards data with new entity type, then re-render
                    this.fetchAwardsData();
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
