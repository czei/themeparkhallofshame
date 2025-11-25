// Theme Park Hall of Shame - Main Application Controller

document.addEventListener('DOMContentLoaded', () => {
    console.log('Theme Park Hall of Shame - Application Loading...');

    // Global application state
    const globalState = {
        filter: 'all-parks',  // Global filter: 'all-parks' or 'disney-universal'
        period: 'today'       // Time period: 'today', '7days', '30days'
    };

    // Tab switching logic
    const navItems = document.querySelectorAll('.nav-item');
    const appContainer = document.getElementById('app-container');
    let currentComponent = null;

    // Initialize global filter UI
    initGlobalFilter();

    // Initialize time period selector
    initTimePeriodSelector();

    // Initialize About modal
    const aboutModal = new AboutModal();
    const aboutLink = document.getElementById('about-link');
    if (aboutLink) {
        aboutLink.addEventListener('click', (e) => {
            e.preventDefault();
            aboutModal.open();
        });
    }

    navItems.forEach(item => {
        item.addEventListener('click', () => {
            // Remove active class from all nav items
            navItems.forEach(i => i.classList.remove('active'));
            // Add active class to clicked item
            item.classList.add('active');

            // Load appropriate view
            const view = item.dataset.view;
            loadView(view);
        });
    });

    /**
     * Initialize global filter toggle
     */
    function initGlobalFilter() {
        const filterBtns = document.querySelectorAll('.filter-btn');

        filterBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const newFilter = btn.dataset.filter;

                if (newFilter !== globalState.filter) {
                    // Update global state
                    globalState.filter = newFilter;

                    // Update UI
                    updateGlobalFilterUI();

                    // Update current component if it exists and has updateFilter method
                    if (currentComponent && typeof currentComponent.updateFilter === 'function') {
                        currentComponent.updateFilter(globalState.filter);
                    }
                }
            });
        });

        // Set initial UI state
        updateGlobalFilterUI();
    }

    /**
     * Update global filter UI to reflect current state
     */
    function updateGlobalFilterUI() {
        const filterBtns = document.querySelectorAll('.filter-btn');
        filterBtns.forEach(btn => {
            if (btn.dataset.filter === globalState.filter) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
    }

    /**
     * Initialize time period selector
     */
    function initTimePeriodSelector() {
        const timeBtns = document.querySelectorAll('.time-btn');

        timeBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const newPeriod = btn.dataset.period;

                if (newPeriod !== globalState.period) {
                    // Update global state
                    globalState.period = newPeriod;

                    // Update UI
                    updateTimePeriodUI();

                    // Update current component if it exists and has updatePeriod method
                    if (currentComponent && typeof currentComponent.updatePeriod === 'function') {
                        currentComponent.updatePeriod(globalState.period);
                    } else if (currentComponent && currentComponent.state) {
                        // Fallback: directly update state and refetch
                        currentComponent.state.period = globalState.period;
                        if (typeof currentComponent.fetchRankings === 'function') {
                            currentComponent.fetchRankings();
                        } else if (typeof currentComponent.fetchData === 'function') {
                            currentComponent.fetchData();
                        }
                    }
                }
            });
        });

        // Set initial UI state
        updateTimePeriodUI();
    }

    /**
     * Update time period UI to reflect current state
     */
    function updateTimePeriodUI() {
        const timeBtns = document.querySelectorAll('.time-btn');
        timeBtns.forEach(btn => {
            if (btn.dataset.period === globalState.period) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
    }

    async function loadView(viewName) {
        // Clear container
        appContainer.innerHTML = '<div id="view-container"></div>';

        console.log(`Loading view: ${viewName}`);

        try {
            switch(viewName) {
                case 'park-rankings':
                    if (typeof ParkRankings !== 'undefined') {
                        currentComponent = new ParkRankings(apiClient, 'view-container', globalState.filter);
                        await currentComponent.init();
                    } else {
                        throw new Error('ParkRankings component not loaded');
                    }
                    break;

                case 'ride-performance':
                    if (typeof RidePerformance !== 'undefined') {
                        currentComponent = new RidePerformance(apiClient, 'view-container', globalState.filter);
                        await currentComponent.init();
                    } else {
                        throw new Error('RidePerformance component not loaded');
                    }
                    break;

                case 'wait-times':
                    if (typeof WaitTimes !== 'undefined') {
                        currentComponent = new WaitTimes(apiClient, 'view-container', globalState.filter);
                        await currentComponent.init();
                    } else {
                        throw new Error('WaitTimes component not loaded');
                    }
                    break;

                case 'trends':
                    if (typeof Trends !== 'undefined') {
                        currentComponent = new Trends(apiClient, 'view-container', globalState.filter);
                        await currentComponent.init();
                    } else {
                        throw new Error('Trends component not loaded');
                    }
                    break;

                default:
                    appContainer.innerHTML = `
                        <div class="error-view">
                            <p>Unknown view: ${viewName}</p>
                        </div>
                    `;
            }
        } catch (error) {
            console.error(`Error loading view ${viewName}:`, error);
            appContainer.innerHTML = `
                <div class="error-view">
                    <p>Error loading view: ${error.message}</p>
                </div>
            `;
        }
    }

    // Load default view (Park Rankings)
    loadView('park-rankings');
});
