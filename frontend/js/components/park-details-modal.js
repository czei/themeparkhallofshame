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
    }

    /**
     * Open modal and fetch park details
     */
    async open(parkId, parkName) {
        this.state = {
            isOpen: true,
            loading: true,
            error: null,
            parkDetails: null,
            parkId,
            parkName
        };

        this.render();

        try {
            const response = await this.apiClient.get(`/parks/${parkId}/details`);

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

        const { park, tier_distribution, operating_sessions, current_status } = this.state.parkDetails;

        return `
            <div class="park-details-content">
                ${this.renderParkInfo(park)}
                ${this.renderCurrentStatus(current_status)}
                ${this.renderTierDistribution(tier_distribution)}
                ${this.renderOperatingSessions(operating_sessions)}
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
                this.open(this.state.parkId, this.state.parkName);
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
    }
}

// Initialize when script is loaded
window.ParkDetailsModal = ParkDetailsModal;
