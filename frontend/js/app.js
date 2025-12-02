// Theme Park Hall of Shame - Main Application Controller

document.addEventListener('DOMContentLoaded', () => {
    console.log('Theme Park Hall of Shame - Application Loading...');

    // Global application state
    const globalState = {
        filter: 'all-parks',  // Global filter: 'all-parks' or 'disney-universal'
        period: 'live',       // Time period: 'live', 'today', 'last_week', 'last_month'
        currentView: 'downtime'  // Track current view for filter visibility
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
            globalState.currentView = view;

            // Update filter visibility based on view
            updateFilterVisibility();

            // If switching to awards and currently on 'live', switch to 'today'
            if (view === 'awards' && globalState.period === 'live') {
                globalState.period = 'today';
                updateTimePeriodUI();
            }

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

                    // Clear cache and prefetch with new filter
                    apiClient.clearCache();
                    apiClient.prefetch(globalState.period, globalState.filter);

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

                    // Clear cache and prefetch with new period
                    apiClient.clearCache();
                    apiClient.prefetch(globalState.period, globalState.filter);

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
        const isAwards = globalState.currentView === 'awards';

        timeBtns.forEach(btn => {
            // Hide/disable LIVE button for Awards tab
            if (btn.dataset.period === 'live') {
                if (isAwards) {
                    btn.classList.add('hidden');
                } else {
                    btn.classList.remove('hidden');
                }
            }

            if (btn.dataset.period === globalState.period) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
    }

    /**
     * Update filter visibility based on current view
     * Awards tab hides the park filter (always shows all parks)
     */
    function updateFilterVisibility() {
        const filterRow = document.querySelector('.park-filter');
        const isAwards = globalState.currentView === 'awards';

        if (filterRow) {
            if (isAwards) {
                filterRow.classList.add('hidden');
            } else {
                filterRow.classList.remove('hidden');
            }
        }

        // Also update time period UI to hide/show LIVE button
        updateTimePeriodUI();
    }

    async function loadView(viewName) {
        // Clear container
        appContainer.innerHTML = '<div id="view-container"></div>';

        console.log(`Loading view: ${viewName}`);

        try {
            switch(viewName) {
                case 'downtime':
                    if (typeof Downtime !== 'undefined') {
                        currentComponent = new Downtime(apiClient, 'view-container', globalState.filter);
                        // Sync the period before fetching data
                        currentComponent.state.period = globalState.period;
                        await currentComponent.init();
                    } else {
                        throw new Error('Downtime component not loaded');
                    }
                    break;

                case 'wait-times':
                    if (typeof WaitTimes !== 'undefined') {
                        currentComponent = new WaitTimes(apiClient, 'view-container', globalState.filter);
                        // Sync the period before fetching data
                        currentComponent.state.period = globalState.period;
                        await currentComponent.init();
                    } else {
                        throw new Error('WaitTimes component not loaded');
                    }
                    break;

                case 'awards':
                    if (typeof Awards !== 'undefined') {
                        currentComponent = new Awards(apiClient, 'view-container', globalState.filter);
                        // Awards uses 'today' if 'live' is selected
                        currentComponent.state.period = globalState.period === 'live' ? 'today' : globalState.period;
                        await currentComponent.init();
                    } else {
                        throw new Error('Awards component not loaded');
                    }
                    break;

                case 'charts':
                    if (typeof Charts !== 'undefined') {
                        currentComponent = new Charts(apiClient, 'view-container', globalState.filter);
                        // Sync the period before fetching data
                        currentComponent.state.period = globalState.period;
                        await currentComponent.init();
                        // Store reference for cleanup
                        window.chartsComponent = currentComponent;
                    } else {
                        throw new Error('Charts component not loaded');
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

    // Load default view (Downtime)
    loadView('downtime');

    // Prefetch data for all tabs in the background
    // This makes tab switching instant after initial load
    apiClient.prefetch(globalState.period, globalState.filter);
});
