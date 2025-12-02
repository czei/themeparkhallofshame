/**
 * Theme Park Hall of Shame - Awards Component
 * Displays 4 award cards for social media validation:
 * - Longest Wait Times (Park & Ride)
 * - Least Reliable (Park & Ride)
 */

class Awards {
    constructor(apiClient, containerId, initialFilter = 'all-parks') {
        this.apiClient = apiClient;
        this.container = document.getElementById(containerId);
        this.state = {
            period: 'today',  // Awards default to 'today' (no 'live' option)
            filter: 'all-parks',  // Awards always show all parks
            loading: false,
            error: null,
            // Award winners (single winner per category)
            longestWaitPark: null,
            longestWaitRide: null,
            leastReliablePark: null,
            leastReliableRide: null
        };
    }

    /**
     * Initialize and render the component
     */
    async init() {
        this.render();
        await this.fetchAwardsData();
    }

    /**
     * Fetch all 4 award winners
     *
     * PERFORMANCE: Sequential calls to avoid overwhelming the server.
     * With server-side caching (5 min TTL), subsequent requests are instant.
     */
    async fetchAwardsData() {
        this.setState({ loading: true, error: null });

        try {
            const period = this.state.period;

            // PERFORMANCE: Sequential calls to reduce server load on cold cache
            // Each endpoint is cached for 5 minutes, so subsequent calls are instant
            const waitPark = await this.apiClient.get('/trends/longest-wait-times', { period, entity: 'parks', filter: 'all-parks', limit: 1 });
            const waitRide = await this.apiClient.get('/trends/longest-wait-times', { period, entity: 'rides', filter: 'all-parks', limit: 1 });
            const reliablePark = await this.apiClient.get('/trends/least-reliable', { period, entity: 'parks', filter: 'all-parks', limit: 1 });
            const reliableRide = await this.apiClient.get('/trends/least-reliable', { period, entity: 'rides', filter: 'all-parks', limit: 1 });

            this.setState({
                longestWaitPark: waitPark.success && waitPark.data?.length > 0 ? waitPark.data[0] : null,
                longestWaitRide: waitRide.success && waitRide.data?.length > 0 ? waitRide.data[0] : null,
                leastReliablePark: reliablePark.success && reliablePark.data?.length > 0 ? reliablePark.data[0] : null,
                leastReliableRide: reliableRide.success && reliableRide.data?.length > 0 ? reliableRide.data[0] : null,
                loading: false
            });

            this.updateLastUpdateTime();
        } catch (error) {
            console.error('Failed to fetch awards data:', error);
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
     * Get period label for display - shows actual dates for social media
     */
    getPeriodLabel() {
        const now = new Date();
        const options = { month: 'short', day: 'numeric', year: 'numeric' };

        if (this.state.period === 'today') {
            // Show today's date: "Dec 2, 2025"
            return now.toLocaleDateString('en-US', options);
        } else if (this.state.period === 'last_week') {
            // Show date range: "Nov 25 - Dec 1, 2025"
            const endDate = new Date(now);
            endDate.setDate(endDate.getDate() - 1); // Yesterday
            const startDate = new Date(endDate);
            startDate.setDate(startDate.getDate() - 6); // 7 days ago

            const startMonth = startDate.toLocaleDateString('en-US', { month: 'short' });
            const startDay = startDate.getDate();
            const endMonth = endDate.toLocaleDateString('en-US', { month: 'short' });
            const endDay = endDate.getDate();
            const year = endDate.getFullYear();

            // Same month: "Nov 25 - Dec 1, 2025" or different: "Nov 25 - Dec 1, 2025"
            if (startMonth === endMonth) {
                return `${startMonth} ${startDay} - ${endDay}, ${year}`;
            } else {
                return `${startMonth} ${startDay} - ${endMonth} ${endDay}, ${year}`;
            }
        } else if (this.state.period === 'last_month') {
            // Show date range: "Nov 2 - Dec 1, 2025"
            const endDate = new Date(now);
            endDate.setDate(endDate.getDate() - 1); // Yesterday
            const startDate = new Date(endDate);
            startDate.setDate(startDate.getDate() - 29); // 30 days ago

            const startMonth = startDate.toLocaleDateString('en-US', { month: 'short' });
            const startDay = startDate.getDate();
            const endMonth = endDate.toLocaleDateString('en-US', { month: 'short' });
            const endDay = endDate.getDate();
            const year = endDate.getFullYear();

            if (startMonth === endMonth) {
                return `${startMonth} ${startDay} - ${endDay}, ${year}`;
            } else {
                return `${startMonth} ${startDay} - ${endMonth} ${endDay}, ${year}`;
            }
        }

        return now.toLocaleDateString('en-US', options);
    }

    /**
     * Render the component
     */
    render() {
        if (!this.container) return;

        if (this.state.loading) {
            this.container.innerHTML = `
                <div class="awards-view">
                    <div class="loading-state">
                        <div class="spinner"></div>
                        <p>Loading awards...</p>
                    </div>
                </div>
            `;
            return;
        }

        if (this.state.error) {
            this.container.innerHTML = `
                <div class="awards-view">
                    <div class="error-state">
                        <p class="error-message">${this.state.error}</p>
                        <button class="retry-btn">Retry</button>
                    </div>
                </div>
            `;
            this.attachEventListeners();
            return;
        }

        this.container.innerHTML = `
            <div class="awards-view">
                <div class="awards-grid-2x2">
                    ${this.renderAwardCard({
                        type: 'reliable-park',
                        headerTitle: 'Least Reliable \u00b7 Park',
                        headerSubtitle: 'Park with highest cumulative downtime / shame score',
                        badgeText: 'RELIABILITY AWARD',
                        badgeIcon: '\ud83d\udd27',
                        winner: this.state.leastReliablePark,
                        winnerName: this.state.leastReliablePark?.park_name,
                        statText: this.state.leastReliablePark
                            ? `${Number(this.state.leastReliablePark.avg_shame_score || 0).toFixed(2)} shame score \u00b7 ${Number(this.state.leastReliablePark.uptime_percentage || 0).toFixed(1)}% uptime`
                            : null
                    })}

                    ${this.renderAwardCard({
                        type: 'reliable-ride',
                        headerTitle: 'Least Reliable \u00b7 Ride',
                        headerSubtitle: 'Individual ride with most downtime / outages',
                        badgeText: 'RELIABILITY AWARD',
                        badgeIcon: '\ud83d\udcc9',
                        winner: this.state.leastReliableRide,
                        winnerName: this.state.leastReliableRide?.ride_name,
                        parkName: this.state.leastReliableRide?.park_name,
                        statText: this.state.leastReliableRide
                            ? `${Number(this.state.leastReliableRide.downtime_hours || 0).toFixed(1)}h downtime \u00b7 ${this.state.leastReliableRide.downtime_incidents || 0} outages`
                            : null
                    })}

                    ${this.renderAwardCard({
                        type: 'wait-park',
                        headerTitle: 'Longest Wait Time \u00b7 Park',
                        headerSubtitle: 'Park with highest average wait across all rides',
                        badgeText: 'WAIT TIME AWARD',
                        badgeIcon: '\u23f1',
                        winner: this.state.longestWaitPark,
                        winnerName: this.state.longestWaitPark?.park_name,
                        statText: this.state.longestWaitPark
                            ? `${Math.round(this.state.longestWaitPark.avg_wait_time || 0)} min average wait`
                            : null
                    })}

                    ${this.renderAwardCard({
                        type: 'wait-ride',
                        headerTitle: 'Longest Wait Time \u00b7 Ride',
                        headerSubtitle: 'Individual ride with highest average queue',
                        badgeText: 'WAIT TIME AWARD',
                        badgeIcon: '\ud83c\udfa2',
                        winner: this.state.longestWaitRide,
                        winnerName: this.state.longestWaitRide?.ride_name,
                        parkName: this.state.longestWaitRide?.park_name,
                        statText: this.state.longestWaitRide
                            ? `${Math.round(this.state.longestWaitRide.avg_wait_time || 0)} min average wait`
                            : null
                    })}
                </div>
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render a single award card matching the mockup design
     * @param {string} parkName - For ride awards, the park the ride belongs to
     */
    renderAwardCard({ type, headerTitle, headerSubtitle, badgeText, badgeIcon, winner, winnerName, parkName, statText }) {
        const periodLabel = this.getPeriodLabel();

        if (!winner) {
            return `
                <div class="award-card-v2 award-card-${type}">
                    <div class="award-header-v2">
                        <div class="award-header-icon">${badgeIcon}</div>
                        <div class="award-header-text">
                            <div class="award-header-title">${headerTitle}</div>
                            <div class="award-header-subtitle">${headerSubtitle}</div>
                        </div>
                    </div>
                    <div class="award-body-v2">
                        <div class="award-empty-v2">
                            <p>No data available for this period</p>
                        </div>
                    </div>
                </div>
            `;
        }

        // For ride awards, show park name below ride name
        const parkNameHtml = parkName ? `<div class="award-park-v2">${this.escapeHtml(parkName)}</div>` : '';

        return `
            <div class="award-card-v2 award-card-${type}">
                <div class="award-header-v2">
                    <div class="award-header-icon">${badgeIcon}</div>
                    <div class="award-header-text">
                        <div class="award-header-title">${headerTitle}</div>
                        <div class="award-header-subtitle">${headerSubtitle}</div>
                    </div>
                </div>
                <div class="award-body-v2">
                    <div class="award-badge-v2">${badgeIcon} ${badgeText}</div>
                    <div class="award-period-v2">${periodLabel}</div>
                    <div class="award-winner-v2">${this.escapeHtml(winnerName)}</div>
                    ${parkNameHtml}
                    <div class="award-stat-v2">${statText}</div>
                </div>
            </div>
        `;
    }

    /**
     * Escape HTML to prevent XSS
     */
    escapeHtml(text) {
        if (!text) return '';
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
     * Update filter - Awards always uses 'all-parks' but we accept the call
     */
    updateFilter(newFilter) {
        // Awards ignore filter changes - always show all parks
    }

    /**
     * Update period (called by app.js global period selector)
     */
    updatePeriod(newPeriod) {
        // Awards don't support 'live' - use 'today' instead
        const effectivePeriod = newPeriod === 'live' ? 'today' : newPeriod;

        if (effectivePeriod !== this.state.period) {
            this.state.period = effectivePeriod;
            this.fetchAwardsData();
        }
    }

    /**
     * Attach event listeners
     */
    attachEventListeners() {
        // Retry button (if error state)
        const retryBtn = this.container.querySelector('.retry-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                this.fetchAwardsData();
            });
        }
    }
}

// Export for use in app.js
window.Awards = Awards;
