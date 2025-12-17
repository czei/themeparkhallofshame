/**
 * Theme Park Hall of Shame - Charts Component
 * Displays line charts for park shame scores and wait times over time
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

class Charts {
    constructor(apiClient, containerId, initialFilter = 'all-parks', parkDetailsModal = null) {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.parkDetailsModal = parkDetailsModal;  // For click handlers
        this.parksChart = null;  // Chart.js instance for parks shame score
        this.waitTimesChart = null;  // Chart.js instance for park wait times
        this.ridesDowntimeChart = null;  // Chart.js instance for ride downtime
        this.ridesWaitTimesChart = null;  // Chart.js instance for ride wait times
        this.state = {
            period: 'last_week',
            filter: initialFilter,
            chartMode: 'line',  // 'line' or 'heatmap'
            loading: false,
            error: null,
            parksChartData: null,
            waitTimesChartData: null,
            ridesDowntimeChartData: null,
            ridesWaitTimesChartData: null,
            // Heatmap data (lazy loaded when toggle clicked)
            parksShameHeatmap: null,
            parksWaitTimesHeatmap: null,
            ridesDowntimeHeatmap: null,
            ridesWaitTimesHeatmap: null,
            // Accordion state: track which heatmap sections are expanded
            expandedHeatmaps: new Set(),  // e.g., Set(['parks-shame', 'parks-waittimes'])
            chartsMock: false,
            chartsGranularity: 'daily'  // 'hourly' for today, 'daily' for 7/30 days
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await this.fetchChartData();
    }

    /**
     * Fetch chart data for all four charts
     */
    async fetchChartData() {
        this.setState({ loading: true, error: null });

        try {
            // Pass the effective period - charts don't support 'live', use 'today' instead
            const parksParams = {
                period: this.getEffectivePeriod(),
                filter: this.state.filter,
                limit: 4  // Top 4 performers for cleaner charts
            };

            const ridesParams = {
                period: this.getEffectivePeriod(),
                filter: this.state.filter,
                limit: 5  // Worst 5 rides
            };

            // Fetch all four chart data types in parallel
            const [parksResponse, waitTimesResponse, ridesDowntimeResponse, ridesWaitTimesResponse] = await Promise.all([
                this.apiClient.get('/trends/chart-data', { ...parksParams, type: 'parks' }),
                this.apiClient.get('/trends/chart-data', { ...parksParams, type: 'waittimes' }),
                this.apiClient.get('/trends/chart-data', { ...ridesParams, type: 'rides' }),
                this.apiClient.get('/trends/chart-data', { ...ridesParams, type: 'ridewaittimes' })
            ]);

            const newState = { loading: false };
            if (parksResponse.success) {
                newState.parksChartData = parksResponse.chart_data;
                newState.chartsMock = parksResponse.mock;
                newState.chartsGranularity = parksResponse.granularity || 'daily';
            }
            if (waitTimesResponse.success) {
                newState.waitTimesChartData = waitTimesResponse.chart_data;
            }
            if (ridesDowntimeResponse.success) {
                newState.ridesDowntimeChartData = ridesDowntimeResponse.chart_data;
            }
            if (ridesWaitTimesResponse.success) {
                newState.ridesWaitTimesChartData = ridesWaitTimesResponse.chart_data;
            }

            this.setState(newState);
            this.renderCharts();
        } catch (error) {
            console.error('Failed to fetch chart data:', error);
            this.setState({ loading: false, error: error.message });
        }
    }

    /**
     * Get effective period for API calls.
     * Now supports all periods including 'live' with 5-minute granularity.
     */
    getEffectivePeriod() {
        return this.state.period;
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
            <div class="charts-view">
                ${this.renderContent()}
            </div>
        `;

        // Re-render charts after DOM is updated
        setTimeout(() => this.renderCharts(), 0);
    }

    /**
     * Render main content
     */
    renderContent() {
        if (this.state.loading) {
            return `
                <div class="loading-state">
                    <div class="spinner"></div>
                    <p>Loading charts...</p>
                </div>
            `;
        }

        if (this.state.error) {
            return `
                <div class="error-state">
                    <p class="error-message">${this.state.error}</p>
                    <button class="retry-btn" onclick="window.chartsComponent?.fetchChartData()">Retry</button>
                </div>
            `;
        }

        return this.renderChartsSection();
    }

    /**
     * Render the charts section with toggle and canvas/heatmap elements
     */
    renderChartsSection() {
        const mockIndicator = this.state.chartsMock
            ? '<span class="mock-data-indicator">Sample data shown - real data accumulating</span>'
            : '';

        const isLivePeriod = this.state.period === 'live';

        // Toggle button (hidden for LIVE period - heatmaps don't support LIVE)
        const toggleHtml = isLivePeriod
            ? ''
            : `<div class="chart-mode-toggle">
                   <button class="toggle-btn ${this.state.chartMode === 'line' ? 'active' : ''}"
                           onclick="window.chartsComponent?.toggleChartMode('line')">
                       LINE CHARTS
                   </button>
                   <button class="toggle-btn ${this.state.chartMode === 'heatmap' ? 'active' : ''}"
                           onclick="window.chartsComponent?.toggleChartMode('heatmap')">
                       HEATMAPS
                   </button>
               </div>`;

        return `
            <div class="section-header">
                <h2 class="section-title">Performance Charts</h2>
                ${toggleHtml}
            </div>
            ${this.state.chartMode === 'line' ? this.renderLineChartsContent() : this.renderHeatmapsContent()}
        `;
    }

    /**
     * Render line charts content
     */
    renderLineChartsContent() {
        const mockIndicator = this.state.chartsMock
            ? '<span class="mock-data-indicator">Sample data shown - real data accumulating</span>'
            : '';

        const isLivePeriod = this.state.period === 'live';
        const downtimeChartHtml = isLivePeriod
            ? `<div class="chart-container">
                   <h3>Worst 5 Rides - Downtime (${this.getPeriodLabel()})</h3>
                   <div class="chart-wrapper chart-not-applicable">
                       <p class="na-message">Downtime data not applicable for LIVE period.<br>
                       Switch to TODAY, YESTERDAY, or other periods to view cumulative downtime trends.</p>
                   </div>
               </div>`
            : `<div class="chart-container">
                   <h3>Worst 5 Rides - Downtime (${this.getPeriodLabel()})</h3>
                   <div class="chart-wrapper">
                       <canvas id="rides-downtime-chart"></canvas>
                   </div>
               </div>`;

        return `
            <div class="charts-grid">
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
                ${downtimeChartHtml}
                <div class="chart-container">
                    <h3>Worst 5 Rides - Wait Times (${this.getPeriodLabel()})</h3>
                    <div class="chart-wrapper">
                        <canvas id="rides-waittimes-chart"></canvas>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render heatmaps content with accordion sections
     */
    renderHeatmapsContent() {
        const heatmaps = [
            {
                id: 'parks-shame',
                title: `Top 10 Parks by Shame Score (${this.getPeriodLabel()})`,
                dataKey: 'parksShameHeatmap'
            },
            {
                id: 'parks-waittimes',
                title: `Top 10 Parks by Wait Times (${this.getPeriodLabel()})`,
                dataKey: 'parksWaitTimesHeatmap'
            },
            {
                id: 'rides-downtime',
                title: `Top 10 Rides by Downtime (${this.getPeriodLabel()})`,
                dataKey: 'ridesDowntimeHeatmap'
            },
            {
                id: 'rides-waittimes',
                title: `Top 10 Rides by Wait Times (${this.getPeriodLabel()})`,
                dataKey: 'ridesWaitTimesHeatmap'
            }
        ];

        const sectionsHtml = heatmaps.map(heatmap => {
            const isExpanded = this.state.expandedHeatmaps.has(heatmap.id);
            const hasData = this.state[heatmap.dataKey];

            return `
                <div class="heatmap-accordion-section">
                    <button class="heatmap-accordion-header ${isExpanded ? 'expanded' : ''}"
                            onclick="window.chartsComponent?.toggleHeatmapSection('${heatmap.id}')"
                            aria-expanded="${isExpanded}">
                        <h3>${heatmap.title}</h3>
                        <span class="chevron">${isExpanded ? '▲' : '▼'}</span>
                    </button>
                    <div class="heatmap-accordion-content ${isExpanded ? 'expanded' : ''}"
                         data-section="${heatmap.id}">
                        <div class="heatmap-wrapper">
                            <div id="${heatmap.id}-heatmap" class="heatmap-container">
                                ${!hasData && isExpanded ? '<p style="text-align: center; padding: 2rem; color: #6c757d;">Loading heatmap data...</p>' : ''}
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        return `
            <div class="heatmap-accordion-controls">
                <button class="accordion-control-btn" onclick="window.chartsComponent?.expandAllHeatmaps()">
                    Expand All
                </button>
                <button class="accordion-control-btn" onclick="window.chartsComponent?.collapseAllHeatmaps()">
                    Collapse All
                </button>
            </div>
            <div class="heatmaps-accordion">
                ${sectionsHtml}
            </div>
        `;
    }

    /**
     * Get period label for chart titles
     * Live shows 5-minute breakdown, today shows hourly breakdown, other periods show daily trends
     */
    getPeriodLabel() {
        const labels = {
            'live': 'Live',
            'today': 'Today',
            'yesterday': 'Yesterday',
            'last_week': 'Last Week',
            'last_month': 'Last Month'
        };
        return labels[this.state.period] || 'Last Week';
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
        this.renderRidesDowntimeChart();
        this.renderRidesWaitTimesChart();
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
        // CRITICAL: spanGaps=true for LIVE period to connect sparse data points
        const datasets = chartData.datasets.map((dataset, index) => ({
            ...dataset,
            borderColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length],
            backgroundColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length] + '20',
            tension: 0.3,
            borderWidth: 3,
            pointRadius: 4,
            pointHoverRadius: 6,
            fill: false,
            spanGaps: true  // Connect points even when data is missing (important for LIVE)
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
     * Render rides downtime chart (worst 5 rides)
     */
    renderRidesDowntimeChart() {
        const canvas = document.getElementById('rides-downtime-chart');
        if (!canvas || !this.state.ridesDowntimeChartData) return;

        // Destroy existing chart if any
        if (this.ridesDowntimeChart) {
            this.ridesDowntimeChart.destroy();
        }

        const ctx = canvas.getContext('2d');
        const chartData = this.state.ridesDowntimeChartData;

        // Add colors to datasets - include park name in tooltip
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

        this.ridesDowntimeChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: datasets
            },
            options: this.getRideChartOptions('Downtime (hrs)')
        });
    }

    /**
     * Render rides wait times chart (worst 5 rides)
     */
    renderRidesWaitTimesChart() {
        const canvas = document.getElementById('rides-waittimes-chart');
        if (!canvas || !this.state.ridesWaitTimesChartData) return;

        // Destroy existing chart if any
        if (this.ridesWaitTimesChart) {
            this.ridesWaitTimesChart.destroy();
        }

        const ctx = canvas.getContext('2d');
        const chartData = this.state.ridesWaitTimesChartData;

        // Add colors to datasets
        // CRITICAL: spanGaps=true for LIVE period to connect sparse data points
        const datasets = chartData.datasets.map((dataset, index) => ({
            ...dataset,
            borderColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length],
            backgroundColor: MARY_BLAIR_COLORS[index % MARY_BLAIR_COLORS.length] + '20',
            tension: 0.3,
            borderWidth: 3,
            pointRadius: 4,
            pointHoverRadius: 6,
            fill: false,
            spanGaps: true  // Connect points even when data is missing (important for LIVE)
        }));

        this.ridesWaitTimesChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: datasets
            },
            options: this.getRideChartOptions('Avg Wait (min)')
        });
    }

    /**
     * Get chart options for ride charts (includes park name in tooltip)
     */
    getRideChartOptions(yAxisLabel) {
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
                            const park = context.dataset.park || '';
                            const value = context.parsed.y;
                            const parkSuffix = park ? ` (${park})` : '';
                            if (yAxisLabel === 'Avg Wait (min)') {
                                return `${label}${parkSuffix}: ${value} min`;
                            }
                            if (yAxisLabel === 'Downtime (hrs)') {
                                return `${label}${parkSuffix}: ${value} hrs`;
                            }
                            return `${label}${parkSuffix}: ${value}`;
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
                            if (yAxisLabel === 'Downtime (hrs)') {
                                return value + ' hrs';
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
     * Update filter from global filter (called by app.js)
     */
    updateFilter(newFilter) {
        if (newFilter !== this.state.filter) {
            this.state.filter = newFilter;
            this.fetchChartData();
        }
    }

    /**
     * Update period (called by app.js global period selector)
     */
    updatePeriod(newPeriod) {
        if (newPeriod !== this.state.period) {
            this.state.period = newPeriod;
            this.fetchChartData();
            // Reset heatmap data when period changes
            this.setState({
                parksShameHeatmap: null,
                parksWaitTimesHeatmap: null,
                ridesDowntimeHeatmap: null,
                ridesWaitTimesHeatmap: null
            });
        }
    }

    /**
     * Toggle between line charts and heatmaps
     */
    toggleChartMode(newMode) {
        if (newMode === this.state.chartMode) return;

        this.setState({ chartMode: newMode });

        // Lazy load heatmap data if switching to heatmap mode
        if (newMode === 'heatmap' && !this.state.parksShameHeatmap) {
            this.fetchHeatmapData();
        }
    }

    /**
     * Fetch all heatmap data
     */
    async fetchHeatmapData() {
        try {
            const baseParams = {
                period: this.getEffectivePeriod(),
                filter: this.state.filter,
                limit: 10
            };

            // Fetch all four heatmap types in parallel
            const [parksShame, parksWaitTimes, ridesDowntime, ridesWaitTimes] = await Promise.all([
                this.apiClient.get('/trends/heatmap-data', { ...baseParams, type: 'parks-shame' }),
                this.apiClient.get('/trends/heatmap-data', { ...baseParams, type: 'parks' }),
                this.apiClient.get('/trends/heatmap-data', { ...baseParams, type: 'rides-downtime' }),
                this.apiClient.get('/trends/heatmap-data', { ...baseParams, type: 'rides-waittimes' })
            ]);

            this.setState({
                parksShameHeatmap: parksShame.success ? parksShame : null,
                parksWaitTimesHeatmap: parksWaitTimes.success ? parksWaitTimes : null,
                ridesDowntimeHeatmap: ridesDowntime.success ? ridesDowntime : null,
                ridesWaitTimesHeatmap: ridesWaitTimes.success ? ridesWaitTimes : null
            });

            // Render heatmaps after data is loaded
            this.renderHeatmaps();
        } catch (error) {
            console.error('Failed to fetch heatmap data:', error);
            this.setState({ error: error.message });
        }
    }

    /**
     * Render all heatmaps using HeatmapRenderer (lazy rendering - only expanded sections)
     */
    renderHeatmaps() {
        if (typeof HeatmapRenderer === 'undefined') {
            console.error('HeatmapRenderer not loaded');
            return;
        }

        // Only render heatmaps that are currently expanded
        if (this.state.expandedHeatmaps.has('parks-shame')) {
            this.renderParksShameHeatmap();
        }
        if (this.state.expandedHeatmaps.has('parks-waittimes')) {
            this.renderParksWaitTimesHeatmap();
        }
        if (this.state.expandedHeatmaps.has('rides-downtime')) {
            this.renderRidesDowntimeHeatmap();
        }
        if (this.state.expandedHeatmaps.has('rides-waittimes')) {
            this.renderRidesWaitTimesHeatmap();
        }
    }

    /**
     * Toggle accordion section (expand/collapse)
     */
    toggleHeatmapSection(sectionId) {
        const expanded = new Set(this.state.expandedHeatmaps);

        if (expanded.has(sectionId)) {
            expanded.delete(sectionId);
        } else {
            expanded.add(sectionId);
        }

        this.setState({ expandedHeatmaps: expanded });

        // Re-render to update UI
        this.render();

        // Render the heatmap if it was just expanded
        if (expanded.has(sectionId)) {
            setTimeout(() => {
                this.renderHeatmaps();
            }, 100);  // Small delay to ensure DOM is updated
        }
    }

    /**
     * Expand all heatmap sections
     */
    expandAllHeatmaps() {
        const expanded = new Set(['parks-shame', 'parks-waittimes', 'rides-downtime', 'rides-waittimes']);
        this.setState({ expandedHeatmaps: expanded });
        this.render();

        // Render all heatmaps after DOM updates
        setTimeout(() => {
            this.renderHeatmaps();
        }, 100);
    }

    /**
     * Collapse all heatmap sections
     */
    collapseAllHeatmaps() {
        this.setState({ expandedHeatmaps: new Set() });
        this.render();
    }

    /**
     * Render parks shame score heatmap
     */
    renderParksShameHeatmap() {
        const container = document.getElementById('parks-shame-heatmap');
        if (!container || !this.state.parksShameHeatmap) return;

        const data = this.state.parksShameHeatmap;

        const renderer = new HeatmapRenderer({
            entities: data.entities,
            timeLabels: data.time_labels,
            matrix: data.matrix,
            metric: data.metric,
            metricUnit: data.metric_unit,
            granularity: data.granularity,
            getEntityLabel: (entity) => entity.entity_name,
            getTierBadge: null,  // Parks don't have tiers
            onCellClick: (entityId, entityType, timeLabel) => {
                this.handleHeatmapCellClick(entityId, entityType, timeLabel);
            }
        });

        renderer.render(container);
    }

    /**
     * Render parks wait times heatmap
     */
    renderParksWaitTimesHeatmap() {
        const container = document.getElementById('parks-waittimes-heatmap');
        if (!container || !this.state.parksWaitTimesHeatmap) return;

        const data = this.state.parksWaitTimesHeatmap;

        const renderer = new HeatmapRenderer({
            entities: data.entities,
            timeLabels: data.time_labels,
            matrix: data.matrix,
            metric: data.metric,
            metricUnit: data.metric_unit,
            granularity: data.granularity,
            getEntityLabel: (entity) => entity.entity_name,
            getTierBadge: null,  // Parks don't have tiers
            onCellClick: (entityId, entityType, timeLabel) => {
                this.handleHeatmapCellClick(entityId, entityType, timeLabel);
            }
        });

        renderer.render(container);
    }

    /**
     * Render rides downtime heatmap
     */
    renderRidesDowntimeHeatmap() {
        const container = document.getElementById('rides-downtime-heatmap');
        if (!container || !this.state.ridesDowntimeHeatmap) return;

        const data = this.state.ridesDowntimeHeatmap;

        const renderer = new HeatmapRenderer({
            entities: data.entities,
            timeLabels: data.time_labels,
            matrix: data.matrix,
            metric: data.metric,
            metricUnit: data.metric_unit,
            granularity: data.granularity,
            getEntityLabel: (entity) => entity.entity_name,
            getTierBadge: (entity) => {
                if (!entity.tier) return '';
                return `<span class="tier-badge tier-${entity.tier}">T${entity.tier}</span>`;
            },
            onCellClick: (entityId, entityType, timeLabel) => {
                this.handleHeatmapCellClick(entityId, entityType, timeLabel);
            }
        });

        renderer.render(container);
    }

    /**
     * Render rides wait times heatmap
     */
    renderRidesWaitTimesHeatmap() {
        const container = document.getElementById('rides-waittimes-heatmap');
        if (!container || !this.state.ridesWaitTimesHeatmap) return;

        const data = this.state.ridesWaitTimesHeatmap;

        const renderer = new HeatmapRenderer({
            entities: data.entities,
            timeLabels: data.time_labels,
            matrix: data.matrix,
            metric: data.metric,
            metricUnit: data.metric_unit,
            granularity: data.granularity,
            getEntityLabel: (entity) => entity.entity_name,
            getTierBadge: (entity) => {
                if (!entity.tier) return '';
                return `<span class="tier-badge tier-${entity.tier}">T${entity.tier}</span>`;
            },
            onCellClick: (entityId, entityType, timeLabel) => {
                this.handleHeatmapCellClick(entityId, entityType, timeLabel);
            }
        });

        renderer.render(container);
    }

    /**
     * Handle heatmap cell click - navigates to detail pages
     */
    handleHeatmapCellClick(entityId, entityType, timeLabel) {
        if (entityType === 'park') {
            // Navigate to park detail page
            window.location.href = `park-detail.html?park_id=${entityId}&period=${this.state.period}`;
        } else if (entityType === 'ride') {
            // Navigate to ride detail page
            window.location.href = `ride-detail.html?ride_id=${entityId}&period=${this.state.period}`;
        }
    }

    /**
     * Cleanup chart instances to prevent memory leaks
     */
    destroy() {
        if (this.parksChart) {
            this.parksChart.destroy();
            this.parksChart = null;
        }
        if (this.waitTimesChart) {
            this.waitTimesChart.destroy();
            this.waitTimesChart = null;
        }
        if (this.ridesDowntimeChart) {
            this.ridesDowntimeChart.destroy();
            this.ridesDowntimeChart = null;
        }
        if (this.ridesWaitTimesChart) {
            this.ridesWaitTimesChart.destroy();
            this.ridesWaitTimesChart = null;
        }
    }
}

// Initialize when view is loaded
window.Charts = Charts;
