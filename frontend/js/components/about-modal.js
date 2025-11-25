/**
 * Theme Park Hall of Shame - About Modal Component
 * Displays information about the project, data sources, and how to use the app
 */

class AboutModal {
    constructor() {
        this.state = {
            isOpen: false
        };
    }

    /**
     * Open the About modal
     */
    open() {
        this.state.isOpen = true;
        this.render();
    }

    /**
     * Close the About modal
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
        const existingModal = document.getElementById('about-modal');
        if (existingModal) {
            existingModal.remove();
        }

        // Don't render if modal is closed
        if (!this.state.isOpen) return;

        // Create modal element
        const modalHTML = `
            <div id="about-modal" class="modal-overlay active">
                <div class="modal-content about-modal">
                    <div class="modal-header">
                        <h2>About Theme Park Hall of Shame</h2>
                        <button class="modal-close-btn" aria-label="Close modal">&times;</button>
                    </div>

                    <div class="modal-body">
                        ${this.renderAboutContent()}
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
     * Render about content
     */
    renderAboutContent() {
        return `
            <div class="about-content">
                <section class="about-section">
                    <h3>What is This?</h3>
                    <p>
                        <strong>Theme Park Hall of Shame</strong> tracks attraction downtime and reliability
                        across major North American theme parks. Think of it as the "Wall of Shame" for parks
                        with the most unreliable rides.
                    </p>
                    <p>
                        We monitor hundreds of attractions in real-time, tracking when rides go down,
                        how long they stay down, and which parks have the worst reliability records.
                    </p>
                </section>

                <section class="about-section">
                    <h3>Why Track This?</h3>
                    <p>
                        Theme park tickets are expensive, and nothing ruins a day faster than your favorite
                        attractions being closed. This project aims to:
                    </p>
                    <ul>
                        <li>Help guests plan visits by identifying parks with better reliability</li>
                        <li>Hold parks accountable for maintenance and uptime</li>
                        <li>Provide transparent data on attraction performance</li>
                        <li>Celebrate parks that keep their rides running smoothly</li>
                    </ul>
                </section>

                <section class="about-section">
                    <h3>How to Use This App</h3>

                    <div class="feature-description">
                        <h4>Park Rankings</h4>
                        <p>
                            View parks ranked by total downtime. Lower rankings mean better reliability.
                            Filter by time period (Today, 7 Days, 30 Days) and toggle weighted scoring
                            to emphasize major attractions.
                        </p>
                    </div>

                    <div class="feature-description">
                        <h4>Ride Performance</h4>
                        <p>
                            See individual rides ranked by downtime hours. Status badges show which
                            rides are currently running or down. Tier badges indicate attraction importance
                            (Tier 1 = major E-ticket attractions).
                        </p>
                    </div>

                    <div class="feature-description">
                        <h4>Wait Times</h4>
                        <p>
                            Track current wait times, 7-day averages, and peak wait times. Helps identify
                            the busiest attractions and best times to visit.
                        </p>
                    </div>

                    <div class="feature-description">
                        <h4>Park Details</h4>
                        <p>
                            Click "Details" on any park to see tier distribution (how many Tier 1/2/3 rides),
                            current status, and recent operating hours.
                        </p>
                    </div>
                </section>

                <section class="about-section">
                    <h3>Data Sources</h3>
                    <p>
                        All wait time and attraction status data is powered by
                        <a href="https://queue-times.com" target="_blank" rel="noopener" class="external-link">
                            Queue-Times.com
                        </a>, an excellent service that aggregates real-time theme park data.
                    </p>
                    <p>
                        We collect snapshots every 10 minutes and aggregate the data to calculate:
                    </p>
                    <ul>
                        <li>Total downtime hours per ride and park</li>
                        <li>Uptime percentages</li>
                        <li>Trend analysis (is reliability improving or declining?)</li>
                        <li>Operating hours and park schedules</li>
                    </ul>
                </section>

                <section class="about-section">
                    <h3>Understanding Ride Tiers</h3>
                    <div class="tier-explanation">
                        <div class="tier-item-explanation tier-1-explanation">
                            <span class="tier-badge-example tier-1-badge">Tier 1</span>
                            <span class="tier-text">Major E-ticket attractions (e.g., Space Mountain, Hagrid's Motorbike)</span>
                        </div>
                        <div class="tier-item-explanation tier-2-explanation">
                            <span class="tier-badge-example tier-2-badge">Tier 2</span>
                            <span class="tier-text">Standard attractions (most rides fall here)</span>
                        </div>
                        <div class="tier-item-explanation tier-3-explanation">
                            <span class="tier-badge-example tier-3-badge">Tier 3</span>
                            <span class="tier-text">Minor attractions (carousels, kiddie rides, shows)</span>
                        </div>
                    </div>
                    <p class="tier-note">
                        Weighted scoring gives more weight to Tier 1 downtime, as these attractions
                        have the biggest impact on guest experience.
                    </p>
                </section>

                <section class="about-section">
                    <h3>Filters</h3>
                    <p>
                        Use the global filter to view:
                    </p>
                    <ul>
                        <li><strong>All Parks</strong>: Every tracked park in North America</li>
                        <li><strong>Disney & Universal</strong>: Focus on the major resort destinations</li>
                    </ul>
                </section>

                <section class="about-section">
                    <h3>Technical Details</h3>
                    <p>
                        This project is built with:
                    </p>
                    <ul>
                        <li>Python backend with Flask API</li>
                        <li>MySQL database for historical data</li>
                        <li>Vanilla JavaScript frontend (no frameworks!)</li>
                        <li>Real-time data collection every 10 minutes</li>
                        <li>Automated daily, weekly, and monthly aggregations</li>
                    </ul>
                </section>

                <section class="about-section disclaimer">
                    <h3>Disclaimer</h3>
                    <p>
                        This is an independent project and is not affiliated with any theme park,
                        park operator, or Queue-Times.com. All data is publicly available and used
                        for informational and entertainment purposes only.
                    </p>
                    <p>
                        Ride closures happen for many valid reasons including weather, maintenance,
                        technical issues, and safety. This tracker simply provides transparency into
                        these patterns.
                    </p>
                </section>

                <section class="about-section">
                    <p class="about-footer">
                        Made with ☕ by theme park enthusiasts who just want rides to work.
                    </p>
                </section>
            </div>
        `;
    }

    /**
     * Attach event listeners
     */
    attachEventListeners() {
        const modal = document.getElementById('about-modal');
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
window.AboutModal = AboutModal;
