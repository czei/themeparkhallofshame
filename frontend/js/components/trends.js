/**
 * Theme Park Hall of Shame - Trends Component
 * Displays performance trends showing parks/rides with ≥5% uptime changes
 * Includes line charts for shame score and downtime visualization
 */

// Mary Blair inspired color palette for charts
const MARY_BLAIR_COLORS = [
    '#FF6B5A',  // Coral
    '#00B8C5',  // Turquoise
    '#FFB627',  // Gold
    '#FF9BAA',  // Soft Pink
    '#9B59B6',  // Purple
    '#A8E6CF',  // Lime/Mint
    '#FF8C42',  // Orange
    '#87CEEB',  // Sky Blue
    '#E91E63',  // Magenta
    '#00CED1',  // Dark Cyan
];

class Trends {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.parksChart = null;  // Chart.js instance for parks shame score
        this.waitTimesChart = null;  // Chart.js instance for park wait times
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
            aggregateStats: null,
            statusSummary: null,
            parksChartData: null,
            waitTimesChartData: null,
            chartsMock: false,
            chartsGranularity: 'daily'  // 'hourly' for today, 'daily' for 7/30 days
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
            this.fetchChartData()
        ]);
    }

    /**
     * Fetch chart data for parks shame score and park wait times
     */
    async fetchChartData() {
        try {
            // Pass the actual period - API now supports 'today' with hourly data
            const params = {
                period: this.state.period,
                filter: this.state.filter,
                limit: 4  // Top 4 performers for cleaner charts
            };

            // Fetch parks shame scores and park wait times in parallel
            const [parksResponse, waitTimesResponse] = await Promise.all([
                this.apiClient.get('/trends/chart-data', { ...params, type: 'parks' }),
                this.apiClient.get('/trends/chart-data', { ...params, type: 'waittimes' })
            ]);

            const newState = {};
            if (parksResponse.success) {
                newState.parksChartData = parksResponse.chart_data;
                newState.chartsMock = parksResponse.mock;
                newState.chartsGranularity = parksResponse.granularity || 'daily';
            }
            if (waitTimesResponse.success) {
                newState.waitTimesChartData = waitTimesResponse.chart_data;
            }

            this.setState(newState);
            this.renderCharts();
        } catch (error) {
            console.error('Failed to fetch chart data:', error);
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
                ${this.renderChartsSection()}
                ${this.renderContent()}
            </div>
        `;

        this.attachEventListeners();
        // Re-render charts after DOM is updated
        setTimeout(() => this.renderCharts(), 0);
    }

    /**
     * Render the charts section with canvas elements
     */
    renderChartsSection() {
        const mockIndicator = this.state.chartsMock
            ? '<span class="mock-data-indicator">Sample data shown - real data accumulating</span>'
            : '';

        return `
            <div class="trends-charts">
                <div class="chart-container">
                    <h3>Park Shame Scores (${this.getPeriodLabel()})</h3>
                    ${mockIndicator}
                    <div class="chart-wrapper">
                        <canvas id="parks-shame-chart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <h3>Park Avg Wait Times (${this.getPeriodLabel()})</h3>
                    <div class="chart-wrapper">
                        <canvas id="parks-waittimes-chart"></canvas>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Get period label for chart titles
     * Today shows hourly breakdown, other periods show daily trends
     */
    getPeriodLabel() {
        const labels = {
            'today': 'Today (Hourly)',
            '7days': 'Last 7 Days',
            '30days': 'Last 30 Days'
        };
        return labels[this.state.period] || 'Last 7 Days';
    }

    /**
     * Render Chart.js charts
     */
    renderCharts() {
        if (typeof Chart === 'undefined') {
            console.warn('Chart.js not loaded');
            return;
        }

        this.renderParksChart();
        this.renderWaitTimesChart();
    }

    /**
     * Render parks shame score chart
     */
    renderParksChart() {
        const canvas = document.getElementById('parks-shame-chart');
        if (!canvas || !this.state.parksChartData) return;

        // Destroy existing chart if any
        if (this.parksChart) {
            this.parksChart.destroy();
        }

        const ctx = canvas.getContext('2d');
        const chartData = this.state.parksChartData;

        // Add colors to datasets
        const datasets = chartData.datasets.map((dataset, index) => ({
            ...dataset,
            borderColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length],
            backgroundColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length] + '20',
            tension: 0.3,
            borderWidth: 3,
            pointRadius: 4,
            pointHoverRadius: 6,
            fill: false
        }));

        this.parksChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: datasets
            },
            options: this.getChartOptions('Shame Score')
        });
    }

    /**
     * Render park wait times chart
     */
    renderWaitTimesChart() {
        const canvas = document.getElementById('parks-waittimes-chart');
        if (!canvas || !this.state.waitTimesChartData) return;

        // Destroy existing chart if any
        if (this.waitTimesChart) {
            this.waitTimesChart.destroy();
        }

        const ctx = canvas.getContext('2d');
        const chartData = this.state.waitTimesChartData;

        // Add colors to datasets
        const datasets = chartData.datasets.map((dataset, index) => ({
            ...dataset,
            borderColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length],
            backgroundColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length] + '20',
            tension: 0.3,
            borderWidth: 3,
            pointRadius: 4,
            pointHoverRadius: 6,
            fill: false
        }));

        this.waitTimesChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: datasets
            },
            options: this.getChartOptions('Avg Wait (min)')
        });
    }

    /**
     * Get common chart options with Mary Blair styling
     */
    getChartOptions(yAxisLabel) {
        return {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        font: {
                            family: 'Inter',
                            size: 11
                        },
                        usePointStyle: true,
                        padding: 16,
                        boxWidth: 8
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(42, 42, 42, 0.95)',
                    titleFont: {
                        family: 'Space Grotesk',
                        size: 13
                    },
                    bodyFont: {
                        family: 'Inter',
                        size: 12
                    },
                    padding: 12,
                    cornerRadius: 8,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            const label = context.dataset.label || '';
                            const value = context.parsed.y;
                            if (yAxisLabel === 'Avg Wait (min)') {
                                return `${label}: ${value} min`;
                            }
                            return `${label}: ${value}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        font: {
                            family: 'Inter',
                            size: 11
                        },
                        callback: function(value) {
                            if (yAxisLabel === 'Avg Wait (min)') {
                                return value + ' min';
                            }
                            return value;
                        }
                    },
                    title: {
                        display: true,
                        text: yAxisLabel,
                        font: {
                            family: 'Space Grotesk',
                            size: 12,
                            weight: 600
                        },
                        color: '#007B8A'
                    }
                },
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        font: {
                            family: 'Inter',
                            size: 11
                        }
                    }
                }
            }
        };
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
            if (this.state.period === 'today') {
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
            this.fetchChartData();  // Also refetch chart data
        }
    }

    /**
     * Update period (called by app.js global period selector)
     */
    updatePeriod(newPeriod) {
        if (newPeriod !== this.state.period) {
            this.state.period = newPeriod;
            this.fetchAllTrends();
            this.fetchChartData();  // Also refetch chart data
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
