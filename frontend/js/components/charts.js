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
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.parksChart = null;  // Chart.js instance for parks shame score
        this.waitTimesChart = null;  // Chart.js instance for park wait times
        this.state = {
            period: 'last_week',
            filter: initialFilter,
            loading: false,
            error: null,
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
        await this.fetchChartData();
    }

    /**
     * Fetch chart data for parks shame score and park wait times
     */
    async fetchChartData() {
        this.setState({ loading: true, error: null });

        try {
            // Pass the effective period - charts don't support 'live', use 'today' instead
            const params = {
                period: this.getEffectivePeriod(),
                filter: this.state.filter,
                limit: 4  // Top 4 performers for cleaner charts
            };

            // Fetch parks shame scores and park wait times in parallel
            const [parksResponse, waitTimesResponse] = await Promise.all([
                this.apiClient.get('/trends/chart-data', { ...params, type: 'parks' }),
                this.apiClient.get('/trends/chart-data', { ...params, type: 'waittimes' })
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

            this.setState(newState);
            this.renderCharts();
        } catch (error) {
            console.error('Failed to fetch chart data:', error);
            this.setState({ loading: false, error: error.message });
        }
    }

    /**
     * Get effective period for API calls.
     * Charts don't support 'live' period - use 'today' instead.
     */
    getEffectivePeriod() {
        return this.state.period === 'live' ? 'today' : this.state.period;
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
     * Render the charts section with canvas elements
     */
    renderChartsSection() {
        const mockIndicator = this.state.chartsMock
            ? '<span class="mock-data-indicator">Sample data shown - real data accumulating</span>'
            : '';

        return `
            <div class="section-header">
                <h2 class="section-title">Performance Charts</h2>
            </div>
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
            </div>
        `;
    }

    /**
     * Get period label for chart titles
     * Today shows hourly breakdown, other periods show daily trends
     */
    getPeriodLabel() {
        const labels = {
            'live': 'Today (Hourly)',
            'today': 'Today (Hourly)',
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
    }
}

// Initialize when view is loaded
window.Charts = Charts;
