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
            period: 'yesterday',  // Awards default to 'yesterday' (first complete period)
            filter: 'all-parks',  // Awards always show all parks
            loading: false,
            error: null,
            // Award winners (single winner per category)
            longestWaitPark: null,
            longestWaitRide: null,
            leastReliablePark: null,
            leastReliableRide: null
        };

        // Valid periods for awards (only completed time periods - no live/today)
        this.validPeriods = ['yesterday', 'last_week', 'last_month'];
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
        } else if (this.state.period === 'yesterday') {
            // Show yesterday's date: "Dec 1, 2025"
            const yesterday = new Date(now);
            yesterday.setDate(yesterday.getDate() - 1);
            return yesterday.toLocaleDateString('en-US', options);
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
            // Show previous calendar month name: "November 2025"
            const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
            return lastMonth.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
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
                ${this.renderPeriodToggle()}
                <div class="awards-grid-2x2">
                    ${this.renderAwardCard({
                        type: 'reliable-park',
                        category: 'LEAST RELIABLE PARK',
                        winner: this.state.leastReliablePark,
                        winnerName: this.state.leastReliablePark?.park_name,
                        location: this.state.leastReliablePark?.location,
                        stats: this.state.leastReliablePark ? [
                            { value: Number(this.state.leastReliablePark.avg_shame_score || 0).toFixed(2), label: 'SHAME' },
                            { value: `${Number(this.state.leastReliablePark.uptime_percentage || 0).toFixed(1)}%`, label: 'UPTIME' }
                        ] : []
                    })}

                    ${this.renderAwardCard({
                        type: 'reliable-ride',
                        category: 'LEAST RELIABLE RIDE',
                        winner: this.state.leastReliableRide,
                        winnerName: this.state.leastReliableRide?.ride_name,
                        location: this.state.leastReliableRide?.park_name,
                        stats: this.state.leastReliableRide ? [
                            { value: `${Number(this.state.leastReliableRide.downtime_hours || 0).toFixed(1)}h`, label: 'DOWNTIME' }
                        ] : []
                    })}

                    ${this.renderAwardCard({
                        type: 'wait-park',
                        category: 'LONGEST AVG WAIT - PARK',
                        winner: this.state.longestWaitPark,
                        winnerName: this.state.longestWaitPark?.park_name,
                        location: this.state.longestWaitPark?.location,
                        stats: this.state.longestWaitPark ? [
                            { value: `${Math.round(this.state.longestWaitPark.avg_wait_time || 0)}`, label: 'AVG MINS' }
                        ] : []
                    })}

                    ${this.renderAwardCard({
                        type: 'wait-ride',
                        category: 'LONGEST AVG WAIT - RIDE',
                        winner: this.state.longestWaitRide,
                        winnerName: this.state.longestWaitRide?.ride_name,
                        location: this.state.longestWaitRide?.park_name,
                        stats: this.state.longestWaitRide ? [
                            { value: `${Math.round(this.state.longestWaitRide.avg_wait_time || 0)}`, label: 'AVG MINS' }
                        ] : []
                    })}
                </div>
            </div>
        `;

        this.attachEventListeners();
    }

    /**
     * Render branded logo for award card
     */
    renderBrandLogo() {
        return `
            <div class="award-brand-logo">
                <span class="logo-top-text">THEME PARK</span>
                <span class="logo-bottom-text">HALL OF SHAME</span>
                <div class="logo-bars">
                    <div class="logo-bar logo-bar-coral"></div>
                    <div class="logo-bar logo-bar-turquoise"></div>
                    <div class="logo-bar logo-bar-gold"></div>
                    <div class="logo-bar logo-bar-pink"></div>
                </div>
            </div>
        `;
    }

    /**
     * Render a single branded award card with Mary Blair geometric background
     */
    renderAwardCard({ type, category, winner, winnerName, location, stats }) {
        const periodLabel = this.getPeriodLabel();

        if (!winner) {
            return `
                <div class="award-card-v3 award-card-${type}">
                    <div class="award-geo-bg">
                        <div class="geo-block geo-block-1"></div>
                        <div class="geo-block geo-block-2"></div>
                        <div class="geo-block geo-block-3"></div>
                        <div class="geo-block geo-block-4"></div>
                    </div>
                    <div class="award-content-v3">
                        ${this.renderBrandLogo()}
                        <div class="award-category-v3">${category}</div>
                        <div class="award-period-v3">${periodLabel}</div>
                        <div class="award-empty-v3">
                            <p>No data available</p>
                        </div>
                    </div>
                </div>
            `;
        }

        // Render stats boxes
        const statsHtml = stats.length > 0 ? `
            <div class="award-stats-v3">
                ${stats.map(stat => `
                    <div class="award-stat-box">
                        <div class="award-stat-value">${stat.value}</div>
                        <div class="award-stat-label">${stat.label}</div>
                    </div>
                `).join('')}
            </div>
        ` : '';

        return `
            <div class="award-card-v3 award-card-${type}">
                <div class="award-geo-bg">
                    <div class="geo-block geo-block-1"></div>
                    <div class="geo-block geo-block-2"></div>
                    <div class="geo-block geo-block-3"></div>
                    <div class="geo-block geo-block-4"></div>
                </div>
                <div class="award-content-v3">
                    ${this.renderBrandLogo()}
                    <div class="award-category-v3">${category}</div>
                    <div class="award-period-v3">${periodLabel}</div>
                    <div class="award-winner-v3">${this.escapeHtml(winnerName)}</div>
                    <div class="award-location-v3">${this.escapeHtml(location || '')}</div>
                    <div class="award-handle-v3">@ThemeParkShame</div>
                    ${statsHtml}
                    <div class="award-data-source">Data provided by ThemeParks.wiki</div>
                </div>
            </div>
        `;
    }

    /**
     * Render the period toggle for awards
     * Only shows completed periods: Yesterday, Last Week, Last Month
     */
    renderPeriodToggle() {
        const periodLabels = {
            'yesterday': 'Yesterday',
            'last_week': 'Last Week',
            'last_month': 'Last Month'
        };

        const buttons = this.validPeriods.map(period => {
            const isActive = this.state.period === period;
            return `<button class="awards-period-btn${isActive ? ' active' : ''}" data-period="${period}">${periodLabels[period]}</button>`;
        }).join('');

        return `
            <div class="awards-period-toggle">
                ${buttons}
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
            lastUpdateEl.textContent = now.toLocaleTimeString('en-US', { timeZone: 'America/Los_Angeles' }) + ' PST';
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
     * Awards only support completed periods: yesterday, last_week, last_month
     */
    updatePeriod(newPeriod) {
        // Awards only support completed periods - map invalid periods to 'yesterday'
        let effectivePeriod = newPeriod;
        if (!this.validPeriods.includes(newPeriod)) {
            effectivePeriod = 'yesterday';
        }

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

        // Period toggle buttons
        const periodBtns = this.container.querySelectorAll('.awards-period-btn');
        periodBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const newPeriod = btn.dataset.period;
                if (newPeriod !== this.state.period) {
                    this.state.period = newPeriod;
                    this.fetchAwardsData();
                }
            });
        });
    }
}

// Export for use in app.js
window.Awards = Awards;
