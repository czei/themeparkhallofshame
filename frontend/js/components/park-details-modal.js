/**
 * Theme Park Hall of Shame - Park Details Modal Component
 * Displays detailed park information including tier distribution and operating hours
 */

class ParkDetailsModal {
    constructor(apiClient) {
        this.apiClient = apiClient;
        this.state = {
            isOpen: false,
            loading: false,
            error: null,
            parkDetails: null
        };
        this.chartInstance = null;  // Store Chart.js instance for cleanup
    }

    /**
     * Open modal and fetch park details
     * @param {number} parkId - The park ID
     * @param {string} parkName - The park name for display
     * @param {string} period - The time period ('live', 'today', 'last_week', 'last_month')
     */
    async open(parkId, parkName, period = 'live') {
        // All periods are now supported by the API
        const validPeriods = ['live', 'today', 'yesterday', 'last_week', 'last_month'];
        const apiPeriod = validPeriods.includes(period) ? period : 'live';

        this.state = {
            isOpen: true,
            loading: true,
            error: null,
            parkDetails: null,
            parkId,
            parkName,
            period: apiPeriod
        };

        this.render();

        try {
            const response = await this.apiClient.get(`/parks/${parkId}/details?period=${apiPeriod}`);

            if (response.success) {
                this.state.loading = false;
                this.state.parkDetails = response;
                this.render();
            } else {
                throw new Error(response.error || 'Failed to load park details');
            }
        } catch (error) {
            this.state.loading = false;
            this.state.error = error.message;
            this.render();
        }
    }

    /**
     * Close modal
     */
    close() {
        // Cleanup chart instance
        if (this.chartInstance) {
            this.chartInstance.destroy();
            this.chartInstance = null;
        }
        this.state.isOpen = false;
        this.render();
    }

    /**
     * Render the modal
     */
    render() {
        // Remove existing modal if present
        const existingModal = document.getElementById('park-details-modal');
        if (existingModal) {
            existingModal.remove();
        }

        // Don't render if modal is closed
        if (!this.state.isOpen) return;

        // Create modal element
        const modalHTML = `
            <div id="park-details-modal" class="modal-overlay active">
                <div class="modal-content park-details-modal">
                    <div class="modal-header">
                        <h2>${this.escapeHtml(this.state.parkName || 'Park Details')}</h2>
                        <button class="modal-close-btn" aria-label="Close modal">&times;</button>
                    </div>

                    <div class="modal-body">
                        ${this.renderModalBody()}
                    </div>
                </div>
            </div>
        `;

        // Append to body
        document.body.insertAdjacentHTML('beforeend', modalHTML);

        // Attach event listeners
        this.attachEventListeners();
    }

    /**
     * Render modal body content
     */
    renderModalBody() {
        if (this.state.loading) {
            return `
                <div class="loading-state">
                    <div class="spinner"></div>
                    <p>Loading park details...</p>
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

        if (!this.state.parkDetails) {
            return '<div class="empty-state"><p>No park details available</p></div>';
        }

        const { park, tier_distribution, operating_sessions, current_status, shame_breakdown, chart_data } = this.state.parkDetails;

        // Only show current status for LIVE period
        const showCurrentStatus = this.state.period === 'live';

        return `
            <div class="park-details-content">
                ${this.renderShameBreakdown(shame_breakdown, chart_data)}
                ${showCurrentStatus ? this.renderCurrentStatus(current_status) : ''}
                ${this.renderTierDistribution(tier_distribution)}
                ${this.renderParkInfo(park)}
            </div>
        `;
    }

    /**
     * Render shame score breakdown - dispatches to appropriate renderer based on breakdown_type
     */
    renderShameBreakdown(breakdown, chartData = null) {
        if (!breakdown) return '';

        // Dispatch based on breakdown_type from API
        switch (breakdown.breakdown_type) {
            case 'today':
            case 'yesterday':
                return this.renderTodayShameBreakdown(breakdown, chartData);
            case 'last_week':
            case 'last_month':
                return this.renderHistoricalShameBreakdown(breakdown);
            default:
                return this.renderLiveShameBreakdown(breakdown, chartData);
        }
    }

    /**
     * Render LIVE shame score breakdown - shows rides currently down RIGHT NOW
     * When chartData is provided with granularity='minutes', shows recent snapshot trend
     */
    renderLiveShameBreakdown(breakdown, chartData = null) {
        const { rides_down, total_park_weight, total_weighted_down, shame_score, park_is_open } = breakdown;

        // If park is closed, show that instead
        if (!park_is_open) {
            return `
                <div class="shame-breakdown-section">
                    <div class="shame-header">
                        <h3>Shame Score Breakdown</h3>
                        <span class="breakdown-period-badge live">LIVE</span>
                    </div>
                    <div class="shame-closed-message">
                        <div class="closed-badge">Park Closed</div>
                        <p>This park is currently closed or has fewer than 50% of rides operating.
                           The shame score is not calculated when parks are closed.</p>
                    </div>
                </div>
            `;
        }

        return `
            <div class="shame-breakdown-section">
                <div class="shame-header">
                    <h3>Shame Score Breakdown</h3>
                    <span class="breakdown-period-badge live">LIVE</span>
                </div>

                <div class="shame-score-display">
                    <div class="shame-score-value ${shame_score > 5 ? 'high' : shame_score > 2 ? 'medium' : 'low'}">
                        ${shame_score.toFixed(1)}
                    </div>
                    <div class="shame-score-label">Current Shame Score (0-10 scale)</div>
                </div>

                <div class="shame-formula-box">
                    <div class="formula-title">How It's Calculated</div>
                    <div class="formula">
                        <span class="formula-part">Shame Score</span> =
                        <span class="formula-fraction">
                            <span class="numerator">Sum of Down Ride Weights</span>
                            <span class="denominator">Total Park Weight</span>
                        </span>
                        <span class="formula-multiplier">× 10</span>
                    </div>
                    <div class="formula-calculation">
                        <span class="calc-fraction">
                            <span class="calc-numerator">${total_weighted_down.toFixed(1)}</span>
                            <span class="calc-denominator">${total_park_weight.toFixed(1)}</span>
                        </span>
                        <span class="calc-multiply">× 10</span>
                        <span class="calc-equals">=</span>
                        <span class="calc-result">${shame_score.toFixed(2)}</span>
                    </div>
                    <div class="formula-explanation">
                        The shame score measures how much of a park's ride capacity is currently unavailable,
                        weighted by ride importance. Flagship attractions (Tier 1) count 3x more than minor rides (Tier 3).
                        Scores typically range from 0 (perfect) to 10+ (severe problems).
                    </div>
                </div>

                ${chartData && chartData.granularity === 'minutes' ? this.renderLiveChart(chartData) : ''}

                ${rides_down.length > 0 ? `
                    <div class="rides-down-section">
                        <h4>Rides Currently Down (${rides_down.length})</h4>
                        <div class="rides-down-list">
                            ${this.renderRidesByWeight(rides_down)}
                        </div>
                    </div>
                ` : `
                    <div class="no-rides-down">
                        <span class="success-icon">✓</span>
                        <p>All rides are currently operating!</p>
                    </div>
                `}

                <div class="tier-weights-info">
                    <div class="tier-weights-title">Tier Weight Reference</div>
                    <div class="tier-weights-grid">
                        <div class="tier-weight-item tier-1">
                            <span class="tier-badge">Tier 1</span>
                            <span class="weight-value">3x</span>
                            <span class="weight-desc">Flagship E-tickets</span>
                        </div>
                        <div class="tier-weight-item tier-2">
                            <span class="tier-badge">Tier 2</span>
                            <span class="weight-value">2x</span>
                            <span class="weight-desc">Standard rides</span>
                        </div>
                        <div class="tier-weight-item tier-3">
                            <span class="tier-badge">Tier 3</span>
                            <span class="weight-value">1x</span>
                            <span class="weight-desc">Minor attractions</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render TODAY/YESTERDAY shame score breakdown - shows AVERAGE shame score
     * This is completely different from live - it shows ALL rides that had ANY downtime
     */
    renderTodayShameBreakdown(breakdown, chartData = null) {
        const {
            rides_with_downtime,
            rides_affected_count,
            total_park_weight,
            total_downtime_hours,
            weighted_downtime_hours,
            shame_score,
            park_is_open,
            breakdown_type
        } = breakdown;

        // Determine display text based on breakdown_type
        const isYesterday = breakdown_type === 'yesterday';
        const periodTitle = isYesterday ? "Yesterday's" : "Today's";
        const periodBadge = isYesterday ? 'YESTERDAY' : 'TODAY';
        const periodClass = isYesterday ? 'yesterday' : 'today';
        const periodText = isYesterday ? 'yesterday' : 'today';

        // If no data available
        if (!rides_with_downtime) {
            return `
                <div class="shame-breakdown-section">
                    <div class="shame-header">
                        <h3>${periodTitle} Shame Score Breakdown</h3>
                        <span class="breakdown-period-badge ${periodClass}">${periodBadge}</span>
                    </div>
                    <div class="no-rides-down">
                        <span class="success-icon">&#10003;</span>
                        <p>No downtime data available for ${periodText} yet.</p>
                    </div>
                </div>
            `;
        }

        // Group rides by tier
        const tier1Rides = rides_with_downtime.filter(r => r.tier === 1);
        const tier2Rides = rides_with_downtime.filter(r => r.tier === 2);
        const tier3Rides = rides_with_downtime.filter(r => r.tier === 3);

        return `
            <div class="shame-breakdown-section today-breakdown">
                <div class="shame-header">
                    <h3>${periodTitle} Shame Score Breakdown</h3>
                    <span class="breakdown-period-badge ${periodClass}">${periodBadge}</span>
                </div>

                <div class="shame-score-display">
                    <div class="shame-score-value ${shame_score > 5 ? 'high' : shame_score > 2 ? 'medium' : 'low'}">
                        ${shame_score.toFixed(1)}
                    </div>
                    <div class="shame-score-label">Average Shame Score ${isYesterday ? 'Yesterday' : 'Today'} (0-10 scale)</div>
                </div>

                <div class="shame-formula-box today-formula">
                    <div class="formula-title">How ${periodTitle} Score Is Calculated</div>
                    <div class="formula">
                        <span class="formula-part">Shame Score</span> =
                        <span class="formula-text">Average of Snapshot Shame Scores</span>
                    </div>
                    <div class="formula-explanation today-explanation">
                        <strong>${periodTitle} score is an average</strong> of instantaneous shame scores throughout the day.
                        Every 5 minutes while the park is open, we calculate what % of capacity was down at that moment.
                        The final score is the average across all these snapshots during operating hours ${periodText}.
                        This makes it comparable to the Live score (same 0-10 scale).
                    </div>
                    <div class="cumulative-stats">
                        <div class="stat-item">
                            <span class="stat-value">${total_downtime_hours.toFixed(1)}h</span>
                            <span class="stat-label">Total Downtime</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value">${weighted_downtime_hours.toFixed(1)}h</span>
                            <span class="stat-label">Weighted Downtime</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value">${rides_affected_count}</span>
                            <span class="stat-label">Rides Affected</span>
                        </div>
                    </div>
                </div>

                ${chartData ? this.renderShameChart(chartData, isYesterday) : ''}

                ${rides_with_downtime.length > 0 ? `
                    <div class="rides-down-section">
                        <h4>Rides With Downtime ${isYesterday ? 'Yesterday' : 'Today'} (${rides_affected_count})</h4>
                        <p class="rides-section-note">All rides that experienced downtime during operating hours ${periodText}, sorted by total downtime.</p>
                        <div class="rides-down-list today-list">
                            ${tier1Rides.length > 0 ? this.renderTodayRidesByTier(tier1Rides, 1, 'Flagship Attractions', '3x weight') : ''}
                            ${tier2Rides.length > 0 ? this.renderTodayRidesByTier(tier2Rides, 2, 'Standard Attractions', '2x weight') : ''}
                            ${tier3Rides.length > 0 ? this.renderTodayRidesByTier(tier3Rides, 3, 'Minor Attractions', '1x weight') : ''}
                        </div>
                    </div>
                ` : `
                    <div class="no-rides-down">
                        <span class="success-icon">&#10003;</span>
                        <p>No rides have experienced downtime ${periodText}!</p>
                    </div>
                `}

                <div class="tier-weights-info">
                    <div class="tier-weights-title">Tier Weight Reference</div>
                    <div class="tier-weights-grid">
                        <div class="tier-weight-item tier-1">
                            <span class="tier-badge">Tier 1</span>
                            <span class="weight-value">3x</span>
                            <span class="weight-desc">Flagship E-tickets</span>
                        </div>
                        <div class="tier-weight-item tier-2">
                            <span class="tier-badge">Tier 2</span>
                            <span class="weight-value">2x</span>
                            <span class="weight-desc">Standard rides</span>
                        </div>
                        <div class="tier-weight-item tier-3">
                            <span class="tier-badge">Tier 3</span>
                            <span class="weight-value">1x</span>
                            <span class="weight-desc">Minor attractions</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render HISTORICAL shame score breakdown - shows average daily shame score for last_week or last_month
     * This shows ALL rides that had ANY downtime during the period
     */
    renderHistoricalShameBreakdown(breakdown) {
        const {
            rides_with_downtime,
            rides_affected_count,
            total_park_weight,
            total_downtime_hours,
            weighted_downtime_hours,
            shame_score,
            period_label,
            days_in_period,
            breakdown_type
        } = breakdown;

        const isWeekly = breakdown_type === 'last_week';
        const periodBadgeText = isWeekly ? 'LAST WEEK' : 'LAST MONTH';
        const periodBadgeClass = isWeekly ? 'last-week' : 'last-month';

        // If no data available
        if (!rides_with_downtime || rides_with_downtime.length === 0) {
            return `
                <div class="shame-breakdown-section">
                    <div class="shame-header">
                        <h3>Shame Score Breakdown</h3>
                        <span class="breakdown-period-badge ${periodBadgeClass}">${periodBadgeText}</span>
                    </div>
                    <div class="period-label">${period_label || ''}</div>
                    <div class="no-rides-down">
                        <span class="success-icon">&#10003;</span>
                        <p>No downtime recorded during this period.</p>
                    </div>
                </div>
            `;
        }

        // Group rides by tier
        const tier1Rides = rides_with_downtime.filter(r => r.tier === 1);
        const tier2Rides = rides_with_downtime.filter(r => r.tier === 2);
        const tier3Rides = rides_with_downtime.filter(r => r.tier === 3);

        return `
            <div class="shame-breakdown-section historical-breakdown">
                <div class="shame-header">
                    <h3>Shame Score Breakdown</h3>
                    <span class="breakdown-period-badge ${periodBadgeClass}">${periodBadgeText}</span>
                </div>
                <div class="period-label">${period_label || ''}</div>

                <div class="shame-score-display">
                    <div class="shame-score-value ${shame_score > 5 ? 'high' : shame_score > 2 ? 'medium' : 'low'}">
                        ${shame_score.toFixed(1)}
                    </div>
                    <div class="shame-score-label">Average Daily Shame Score (0-10 scale)</div>
                </div>

                <div class="shame-formula-box historical-formula">
                    <div class="formula-title">How This Score Is Calculated</div>
                    <div class="formula">
                        <span class="formula-part">Shame Score</span> =
                        <span class="formula-text">Average of Daily Shame Scores</span>
                    </div>
                    <div class="formula-explanation historical-explanation">
                        <strong>This is an average</strong> of the daily shame scores during ${period_label || 'this period'}.
                        Each day's score = (weighted downtime / total park weight) &times; 10.
                        Days with no downtime contribute 0 to the average.
                        This makes the score comparable to Live and Today periods.
                    </div>
                    <div class="cumulative-stats">
                        <div class="stat-item">
                            <span class="stat-value">${total_downtime_hours.toFixed(1)}h</span>
                            <span class="stat-label">Total Downtime</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value">${weighted_downtime_hours.toFixed(1)}h</span>
                            <span class="stat-label">Weighted Downtime</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value">${rides_affected_count}</span>
                            <span class="stat-label">Rides Affected</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value">${days_in_period || '?'}</span>
                            <span class="stat-label">Days in Period</span>
                        </div>
                    </div>
                </div>

                ${rides_with_downtime.length > 0 ? `
                    <div class="rides-down-section">
                        <h4>Rides With Downtime (${rides_affected_count})</h4>
                        <p class="rides-section-note">All rides that experienced downtime during ${period_label || 'this period'}, sorted by total downtime.</p>
                        <div class="rides-down-list historical-list">
                            ${tier1Rides.length > 0 ? this.renderHistoricalRidesByTier(tier1Rides, 1, 'Flagship Attractions', '3x weight') : ''}
                            ${tier2Rides.length > 0 ? this.renderHistoricalRidesByTier(tier2Rides, 2, 'Standard Attractions', '2x weight') : ''}
                            ${tier3Rides.length > 0 ? this.renderHistoricalRidesByTier(tier3Rides, 3, 'Minor Attractions', '1x weight') : ''}
                        </div>
                    </div>
                ` : ''}

                <div class="tier-weights-info">
                    <div class="tier-weights-title">Tier Weight Reference</div>
                    <div class="tier-weights-grid">
                        <div class="tier-weight-item tier-1">
                            <span class="tier-badge">Tier 1</span>
                            <span class="weight-value">3x</span>
                            <span class="weight-desc">Flagship E-tickets</span>
                        </div>
                        <div class="tier-weight-item tier-2">
                            <span class="tier-badge">Tier 2</span>
                            <span class="weight-value">2x</span>
                            <span class="weight-desc">Standard rides</span>
                        </div>
                        <div class="tier-weight-item tier-3">
                            <span class="tier-badge">Tier 3</span>
                            <span class="weight-value">1x</span>
                            <span class="weight-desc">Minor attractions</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render rides grouped by tier for HISTORICAL breakdown (shows downtime hours and days affected)
     */
    renderHistoricalRidesByTier(rides, tier, tierName, weightLabel) {
        return `
            <div class="tier-group tier-${tier}">
                <div class="tier-group-header">
                    <span class="tier-badge">Tier ${tier}</span>
                    <span class="tier-name">${tierName}</span>
                    <span class="tier-weight-label">${weightLabel}</span>
                </div>
                <ul class="rides-list historical-rides-list">
                    ${rides.map(ride => `
                        <li class="ride-item historical-ride-item">
                            <span class="ride-name">${this.escapeHtml(ride.ride_name)}</span>
                            <span class="ride-downtime-info">
                                <span class="ride-downtime">${this.formatDowntimeHours(ride.downtime_hours)} down</span>
                                <span class="ride-days">${ride.days_with_downtime || '?'} day${(ride.days_with_downtime || 0) !== 1 ? 's' : ''}</span>
                            </span>
                        </li>
                    `).join('')}
                </ul>
            </div>
        `;
    }

    /**
     * Render rides grouped by tier for TODAY breakdown (shows downtime hours per ride)
     */
    renderTodayRidesByTier(rides, tier, tierName, weightLabel) {
        return `
            <div class="tier-group tier-${tier}">
                <div class="tier-group-header">
                    <span class="tier-badge">Tier ${tier}</span>
                    <span class="tier-name">${tierName}</span>
                    <span class="tier-weight-label">${weightLabel}</span>
                </div>
                <ul class="rides-list today-rides-list">
                    ${rides.map(ride => `
                        <li class="ride-item today-ride-item">
                            <span class="ride-name">${this.escapeHtml(ride.ride_name)}</span>
                            <span class="ride-downtime-info">
                                <span class="ride-downtime">${this.formatDowntimeHours(ride.downtime_hours)} down</span>
                                <span class="ride-weighted">+${ride.weighted_contribution.toFixed(1)} weighted</span>
                            </span>
                        </li>
                    `).join('')}
                </ul>
            </div>
        `;
    }

    /**
     * Render shame score chart HTML container
     * Chart will be initialized after DOM render in attachEventListeners
     */
    renderShameChart(chartData, isYesterday = false) {
        if (!chartData || !chartData.data) return '';

        const periodLabel = isYesterday ? 'Yesterday' : 'Today';
        // API returns average as string (MariaDB ROUND() returns Decimal) - convert to number
        const avgScore = parseFloat(chartData.average) || 0;

        return `
            <div class="shame-chart-section">
                <div class="shame-chart-header">
                    <h4>Shame Score Throughout the Day</h4>
                    <div class="chart-average-badge ${avgScore > 5 ? 'high' : avgScore > 2 ? 'medium' : 'low'}">
                        Avg: ${avgScore.toFixed(1)}
                    </div>
                </div>
                <div class="shame-chart-container">
                    <canvas id="shame-score-chart"></canvas>
                </div>
                <p class="chart-description">
                    Hourly shame score snapshots ${isYesterday ? 'from yesterday' : 'throughout today'}.
                    The dashed line shows the average score (${avgScore.toFixed(1)}).
                </p>
            </div>
        `;
    }

    /**
     * Render LIVE shame score chart HTML container (minute granularity)
     * Chart will be initialized after DOM render in attachEventListeners
     */
    renderLiveChart(chartData) {
        if (!chartData || !chartData.data || chartData.data.length === 0) return '';

        // API returns 'current' (the most recent value from chart data) for LIVE period
        const currentScore = parseFloat(chartData.current) || 0;
        const dataPoints = chartData.data.filter(v => v !== null).length;

        return `
            <div class="shame-chart-section live-chart">
                <div class="shame-chart-header">
                    <h4>Shame Score Trend (Last 60 Minutes)</h4>
                    <div class="chart-average-badge ${currentScore > 5 ? 'high' : currentScore > 2 ? 'medium' : 'low'}">
                        Current: ${currentScore.toFixed(1)}
                    </div>
                </div>
                <div class="shame-chart-container">
                    <canvas id="shame-score-chart"></canvas>
                </div>
                <p class="chart-description">
                    Real-time shame scores at 5-minute intervals (${dataPoints} data points).
                    Shows instantaneous values, not averages.
                </p>
            </div>
        `;
    }

    /**
     * Initialize Chart.js instance for shame score chart
     * Called from attachEventListeners after DOM is rendered
     * Handles both hourly (TODAY/YESTERDAY) and minute (LIVE) granularity
     */
    initializeShameChart(chartData) {
        if (!chartData || !chartData.data) return;

        const canvas = document.getElementById('shame-score-chart');
        if (!canvas) return;

        // Destroy existing chart if any
        if (this.chartInstance) {
            this.chartInstance.destroy();
        }

        const ctx = canvas.getContext('2d');
        const isLive = chartData.granularity === 'minutes';
        // For LIVE charts, use 'current' (last value); for others use 'average'
        const displayValue = isLive
            ? (parseFloat(chartData.current) || 0)
            : (parseFloat(chartData.average) || 0);

        // Filter out null values and convert strings to numbers for gradient calculation
        const validValues = chartData.data
            .filter(v => v !== null)
            .map(v => parseFloat(v) || 0);
        const maxValue = Math.max(...validValues, displayValue, 5); // At least 5 for scale

        // Create gradient for the line
        const gradient = ctx.createLinearGradient(0, 0, 0, 200);
        gradient.addColorStop(0, 'rgba(220, 53, 69, 0.3)');   // Red at top
        gradient.addColorStop(0.5, 'rgba(255, 193, 7, 0.2)'); // Yellow middle
        gradient.addColorStop(1, 'rgba(40, 167, 69, 0.1)');   // Green at bottom

        // Convert chart data to numbers (API returns strings from MariaDB ROUND)
        const numericData = chartData.data.map(v => v === null ? null : parseFloat(v));

        // Build datasets - LIVE charts don't show average line (instantaneous values)
        const datasets = [
            {
                label: isLive ? 'Instantaneous Score' : 'Shame Score',
                data: numericData,
                borderColor: 'rgb(220, 53, 69)',
                backgroundColor: gradient,
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: isLive ? 2 : 3,  // Smaller points for more frequent data
                pointHoverRadius: 5,
                pointBackgroundColor: 'rgb(220, 53, 69)',
                spanGaps: true // Connect points across null values
            }
        ];

        // Only add average line for hourly charts (TODAY/YESTERDAY)
        if (!isLive) {
            datasets.push({
                label: 'Average',
                data: chartData.labels.map(() => displayValue),
                borderColor: 'rgba(108, 117, 125, 0.7)',
                borderWidth: 2,
                borderDash: [5, 5],
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 0
            });
        }

        this.chartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            label: function(context) {
                                if (context.dataset.label === 'Average') {
                                    return `Average: ${context.raw.toFixed(1)}`;
                                }
                                if (context.raw === null) {
                                    return 'No data';
                                }
                                const label = isLive ? 'Score' : 'Shame Score';
                                return `${label}: ${context.raw.toFixed(1)}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        grid: {
                            display: false
                        },
                        ticks: {
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: isLive ? 6 : 9  // Fewer labels for minute granularity
                        }
                    },
                    y: {
                        beginAtZero: true,
                        max: Math.ceil(maxValue * 1.2), // 20% headroom
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        },
                        ticks: {
                            callback: function(value) {
                                return value.toFixed(1);
                            }
                        }
                    }
                },
                interaction: {
                    mode: 'nearest',
                    axis: 'x',
                    intersect: false
                }
            }
        });
    }

    /**
     * Format downtime hours to readable string
     */
    formatDowntimeHours(hours) {
        if (!hours || hours === 0) return '0m';
        const numHours = Number(hours);
        const wholeHours = Math.floor(numHours);
        const minutes = Math.round((numHours - wholeHours) * 60);

        if (wholeHours === 0) return `${minutes}m`;
        if (minutes === 0) return `${wholeHours}h`;
        return `${wholeHours}h ${minutes}m`;
    }

    /**
     * Render rides grouped by their actual tier_weight value (for LIVE breakdown)
     * Dynamically creates groups based on actual weight values in the data
     */
    renderRidesByWeight(rides) {
        // Group rides by their actual tier_weight
        const groupedByWeight = {};
        rides.forEach(ride => {
            const weight = ride.tier_weight || 2; // Default to 2 if missing
            if (!groupedByWeight[weight]) {
                groupedByWeight[weight] = [];
            }
            groupedByWeight[weight].push(ride);
        });

        // Sort weights descending (highest weight first)
        const sortedWeights = Object.keys(groupedByWeight)
            .map(Number)
            .sort((a, b) => b - a);

        // Get tier name based on weight (3x/2x/1x system)
        const getTierInfo = (weight) => {
            if (weight >= 3) return { tier: 1, name: 'Flagship Attractions' };
            if (weight >= 2) return { tier: 2, name: 'Standard Attractions' };
            return { tier: 3, name: 'Minor Attractions' };
        };

        return sortedWeights.map(weight => {
            const ridesInGroup = groupedByWeight[weight];
            const tierInfo = getTierInfo(weight);
            return `
                <div class="tier-group tier-${tierInfo.tier}">
                    <div class="tier-group-header">
                        <span class="tier-badge">Tier ${tierInfo.tier}</span>
                        <span class="tier-name">${tierInfo.name}</span>
                        <span class="tier-weight-label">${weight}x weight</span>
                    </div>
                    <ul class="rides-list">
                        ${ridesInGroup.map(ride => `
                            <li class="ride-item">
                                <span class="ride-name">${this.escapeHtml(ride.ride_name)}</span>
                                <span class="ride-weight">+${ride.tier_weight}</span>
                            </li>
                        `).join('')}
                    </ul>
                </div>
            `;
        }).join('');
    }

    /**
     * Render rides grouped by tier (legacy - used by historical breakdowns)
     */
    renderRidesByTier(rides, tier, tierName, weightLabel) {
        return `
            <div class="tier-group tier-${tier}">
                <div class="tier-group-header">
                    <span class="tier-badge">Tier ${tier}</span>
                    <span class="tier-name">${tierName}</span>
                    <span class="tier-weight-label">${weightLabel}</span>
                </div>
                <ul class="rides-list">
                    ${rides.map(ride => `
                        <li class="ride-item">
                            <span class="ride-name">${this.escapeHtml(ride.ride_name)}</span>
                            <span class="ride-weight">+${ride.tier_weight}</span>
                        </li>
                    `).join('')}
                </ul>
            </div>
        `;
    }

    /**
     * Render park basic information
     */
    renderParkInfo(park) {
        if (!park) return '';

        return `
            <div class="park-info-section">
                <h3>Park Information</h3>
                <div class="info-grid">
                    <div class="info-item">
                        <span class="info-label">Location:</span>
                        <span class="info-value">${this.escapeHtml(park.location || 'Unknown')}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Operator:</span>
                        <span class="info-value">${this.escapeHtml(park.operator || 'Unknown')}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Timezone:</span>
                        <span class="info-value">${this.escapeHtml(park.timezone || 'Unknown')}</span>
                    </div>
                    ${park.queue_times_url ? `
                    <div class="info-item">
                        <span class="info-label">More Info:</span>
                        <span class="info-value">
                            <a href="${park.queue_times_url}" target="_blank" rel="noopener noreferrer" class="external-link">
                                Queue-Times.com ↗
                            </a>
                        </span>
                    </div>
                    ` : ''}
                </div>
            </div>
        `;
    }

    /**
     * Render current status
     */
    renderCurrentStatus(status) {
        if (!status) return '';

        return `
            <div class="current-status-section">
                <h3>Current Status</h3>
                <div class="status-grid">
                    <div class="status-card">
                        <span class="status-label">Total Rides</span>
                        <span class="status-value">${status.total_rides || 0}</span>
                    </div>
                    <div class="status-card status-running">
                        <span class="status-label">Running</span>
                        <span class="status-value">${status.rides_open || 0}</span>
                    </div>
                    <div class="status-card status-down">
                        <span class="status-label">Down</span>
                        <span class="status-value">${status.rides_closed || 0}</span>
                    </div>
                    ${status.uptime_percentage !== undefined ? `
                    <div class="status-card">
                        <span class="status-label">Uptime</span>
                        <span class="status-value">${status.uptime_percentage.toFixed(1)}%</span>
                    </div>
                    ` : ''}
                </div>
            </div>
        `;
    }

    /**
     * Render tier distribution
     */
    renderTierDistribution(tierDist) {
        if (!tierDist) return '';

        const tier1 = tierDist.tier_1_count || 0;
        const tier2 = tierDist.tier_2_count || 0;
        const tier3 = tierDist.tier_3_count || 0;
        const total = tierDist.total_rides || (tier1 + tier2 + tier3);

        return `
            <div class="tier-distribution-section">
                <h3>Ride Tier Distribution</h3>
                <p class="section-description">Classification of rides by importance and popularity</p>

                <div class="tier-breakdown">
                    <div class="tier-item tier-1">
                        <div class="tier-header">
                            <span class="tier-badge">Tier 1</span>
                            <span class="tier-count">${tier1} rides</span>
                        </div>
                        <div class="tier-description">Major E-ticket attractions</div>
                        ${total > 0 ? `
                        <div class="tier-bar">
                            <div class="tier-fill" style="width: ${(tier1 / total * 100).toFixed(1)}%"></div>
                        </div>
                        ` : ''}
                    </div>

                    <div class="tier-item tier-2">
                        <div class="tier-header">
                            <span class="tier-badge">Tier 2</span>
                            <span class="tier-count">${tier2} rides</span>
                        </div>
                        <div class="tier-description">Standard attractions</div>
                        ${total > 0 ? `
                        <div class="tier-bar">
                            <div class="tier-fill" style="width: ${(tier2 / total * 100).toFixed(1)}%"></div>
                        </div>
                        ` : ''}
                    </div>

                    <div class="tier-item tier-3">
                        <div class="tier-header">
                            <span class="tier-badge">Tier 3</span>
                            <span class="tier-count">${tier3} rides</span>
                        </div>
                        <div class="tier-description">Minor attractions</div>
                        ${total > 0 ? `
                        <div class="tier-bar">
                            <div class="tier-fill" style="width: ${(tier3 / total * 100).toFixed(1)}%"></div>
                        </div>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Render operating sessions
     */
    renderOperatingSessions(sessions) {
        if (!sessions || sessions.length === 0) {
            return `
                <div class="operating-sessions-section">
                    <h3>Recent Operating Hours</h3>
                    <p class="empty-message">No recent operating hours data available</p>
                </div>
            `;
        }

        return `
            <div class="operating-sessions-section">
                <h3>Recent Operating Hours</h3>
                <p class="section-description">Last 7 days of park operating sessions</p>

                <div class="sessions-list">
                    ${sessions.map(session => this.renderOperatingSession(session)).join('')}
                </div>
            </div>
        `;
    }

    /**
     * Render a single operating session
     */
    renderOperatingSession(session) {
        const date = session.operating_date || session.date;
        const openTime = session.open_time || session.opens_at;
        const closeTime = session.close_time || session.closes_at;
        const duration = session.operating_hours || session.duration_hours;

        return `
            <div class="session-item">
                <div class="session-date">${this.formatDate(date)}</div>
                <div class="session-times">
                    ${openTime && closeTime ? `
                        <span class="session-time">${this.formatTime(openTime)} - ${this.formatTime(closeTime)}</span>
                    ` : '<span class="session-time">Hours unknown</span>'}
                </div>
                ${duration ? `
                <div class="session-duration">${duration.toFixed(1)} hours</div>
                ` : ''}
            </div>
        `;
    }

    /**
     * Format date for display
     */
    formatDate(dateStr) {
        if (!dateStr) return 'Unknown Date';

        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', {
            weekday: 'short',
            month: 'short',
            day: 'numeric'
        });
    }

    /**
     * Format time for display
     */
    formatTime(timeStr) {
        if (!timeStr) return '';

        // Handle various time formats
        if (typeof timeStr === 'string') {
            // If it's already formatted like "9:00 AM", return as is
            if (timeStr.match(/\d{1,2}:\d{2}\s?[AP]M/i)) {
                return timeStr;
            }

            // If it's HH:MM:SS format, parse it
            const parts = timeStr.split(':');
            if (parts.length >= 2) {
                let hours = parseInt(parts[0]);
                const minutes = parts[1];
                const ampm = hours >= 12 ? 'PM' : 'AM';
                hours = hours % 12 || 12;
                return `${hours}:${minutes} ${ampm}`;
            }
        }

        return timeStr;
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
     * Attach event listeners
     */
    attachEventListeners() {
        const modal = document.getElementById('park-details-modal');
        if (!modal) return;

        // Close button
        const closeBtn = modal.querySelector('.modal-close-btn');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.close());
        }

        // Close on overlay click
        modal.addEventListener('click', (e) => {
            if (e.target.classList.contains('modal-overlay')) {
                this.close();
            }
        });

        // Retry button (if error state)
        const retryBtn = modal.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.open(this.state.parkId, this.state.parkName, this.state.period);
            });
        }

        // Close on Escape key
        const handleEscape = (e) => {
            if (e.key === 'Escape' && this.state.isOpen) {
                this.close();
                document.removeEventListener('keydown', handleEscape);
            }
        };
        document.addEventListener('keydown', handleEscape);

        // Initialize chart if chart_data is present (for TODAY/YESTERDAY periods)
        if (this.state.parkDetails && this.state.parkDetails.chart_data) {
            // Use setTimeout to ensure DOM is fully rendered
            setTimeout(() => {
                this.initializeShameChart(this.state.parkDetails.chart_data);
            }, 50);
        }
    }
}

// Initialize when script is loaded
window.ParkDetailsModal = ParkDetailsModal;
