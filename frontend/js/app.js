// Theme Park Hall of Shame - Main Application Controller

document.addEventListener('DOMContentLoaded', () => {
    console.log('Theme Park Hall of Shame - Application Loading...');

    // Global application state
    const globalState = {
        filter: 'all-parks',  // Global filter: 'all-parks' or 'disney-universal'
        period: 'today',      // Time period: 'live', 'today', 'yesterday', 'last_week', 'last_month'
        currentView: 'downtime',  // Track current view for filter visibility
        entity: 'parks'       // Entity type: 'parks' or 'rides'
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

    // Initialize Search component
    if (typeof Search !== 'undefined') {
        const searchComponent = new Search(apiClient);
        const searchTrigger = document.getElementById('search-trigger');
        if (searchTrigger) {
            searchTrigger.addEventListener('click', () => {
                searchComponent.open();
            });
        }
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

            // If switching to awards and currently on 'live' or 'today', switch to 'yesterday'
            // Awards only show completed periods (yesterday, last_week, last_month)
            if (view === 'awards' && (globalState.period === 'live' || globalState.period === 'today')) {
                globalState.period = 'yesterday';
                updateTimePeriodUI();
            }

            // Save current entity selection before switching views
            if (currentComponent && currentComponent.state && currentComponent.state.entityType) {
                globalState.entity = currentComponent.state.entityType;
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
     * Initialize time period selector (including History dropdown)
     */
    function initTimePeriodSelector() {
        const timeBtns = document.querySelectorAll('.time-btn');
        const historyDropdown = document.querySelector('.history-dropdown');
        const historyBtn = document.getElementById('history-dropdown-btn');
        const dropdownItems = document.querySelectorAll('.dropdown-item');

        // Handle primary time buttons (Live, Today, Yesterday)
        timeBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const newPeriod = btn.dataset.period;
                selectPeriod(newPeriod);
            });
        });

        // Handle History dropdown toggle
        if (historyBtn) {
            historyBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                historyDropdown.classList.toggle('open');
            });
        }

        // Handle dropdown items (Last Week, Last Month)
        dropdownItems.forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                const newPeriod = item.dataset.period;
                selectPeriod(newPeriod);
                // Close dropdown
                historyDropdown.classList.remove('open');
            });
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (historyDropdown && !historyDropdown.contains(e.target)) {
                historyDropdown.classList.remove('open');
            }
        });

        // Set initial UI state
        updateTimePeriodUI();
    }

    /**
     * Select a time period and update the view
     */
    function selectPeriod(newPeriod) {
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
    }

    /**
     * Update time period UI to reflect current state
     */
    function updateTimePeriodUI() {
        const timeBtns = document.querySelectorAll('.time-btn');
        const historyBtn = document.getElementById('history-dropdown-btn');
        const dropdownItems = document.querySelectorAll('.dropdown-item');
        const isAwards = globalState.currentView === 'awards';

        // Check if current period is a "history" period (dropdown item)
        const isHistoryPeriod = ['last_week', 'last_month'].includes(globalState.period);

        // Update primary time buttons
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

        // Update History button (active if any dropdown item is selected)
        if (historyBtn) {
            if (isHistoryPeriod) {
                historyBtn.classList.add('active');
            } else {
                historyBtn.classList.remove('active');
            }
        }

        // Update dropdown items
        dropdownItems.forEach(item => {
            if (item.dataset.period === globalState.period) {
                item.classList.add('active');
            } else {
                item.classList.remove('active');
            }
        });
    }

    /**
     * Update filter visibility based on current view
     * Awards tab hides the park filter AND time selector (has its own inline toggle)
     */
    function updateFilterVisibility() {
        const filterRow = document.querySelector('.park-filter');
        const timeSelector = document.querySelector('.time-selector');
        const isAwards = globalState.currentView === 'awards';

        if (filterRow) {
            if (isAwards) {
                filterRow.classList.add('hidden');
            } else {
                filterRow.classList.remove('hidden');
            }
        }

        // Hide global time selector for Awards (it has its own inline period toggle)
        if (timeSelector) {
            if (isAwards) {
                timeSelector.classList.add('hidden');
            } else {
                timeSelector.classList.remove('hidden');
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
                        // Sync the period and entity before fetching data
                        currentComponent.state.period = globalState.period;
                        currentComponent.state.entityType = globalState.entity;
                        await currentComponent.init();
                    } else {
                        throw new Error('Downtime component not loaded');
                    }
                    break;

                case 'wait-times':
                    if (typeof WaitTimes !== 'undefined') {
                        currentComponent = new WaitTimes(apiClient, 'view-container', globalState.filter);
                        // Sync the period and entity before fetching data
                        currentComponent.state.period = globalState.period;
                        currentComponent.state.entityType = globalState.entity;
                        await currentComponent.init();
                    } else {
                        throw new Error('WaitTimes component not loaded');
                    }
                    break;

                case 'awards':
                    if (typeof Awards !== 'undefined') {
                        currentComponent = new Awards(apiClient, 'view-container', globalState.filter);
                        // Awards only supports completed periods (yesterday, last_week, last_month)
                        // Map 'live' and 'today' to 'yesterday'
                        const validAwardsPeriods = ['yesterday', 'last_week', 'last_month'];
                        currentComponent.state.period = validAwardsPeriods.includes(globalState.period)
                            ? globalState.period
                            : 'yesterday';
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
