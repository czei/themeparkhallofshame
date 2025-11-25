// Theme Park Hall of Shame - Main Application Controller

document.addEventListener('DOMContentLoaded', () => {
    console.log('Theme Park Hall of Shame - Application Loading...');

    // Global application state
    const globalState = {
        filter: 'all-parks'  // Global filter: 'all-parks' or 'disney-universal'
    };

    // Tab switching logic
    const navTabs = document.querySelectorAll('.nav-tab');
    const appContainer = document.getElementById('app-container');
    let currentComponent = null;

    // Initialize global filter UI
    initGlobalFilter();

    navTabs.forEach(tab => {
        tab.addEventListener('click', () => {
            // Remove active class from all tabs
            navTabs.forEach(t => t.classList.remove('active'));
            // Add active class to clicked tab
            tab.classList.add('active');

            // Load appropriate view
            const view = tab.dataset.view;
            loadView(view);
        });
    });

    /**
     * Initialize global filter toggle
     */
    function initGlobalFilter() {
        const globalFilterBtns = document.querySelectorAll('.global-filter-btn');

        globalFilterBtns.forEach(btn => {
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
        const globalFilterBtns = document.querySelectorAll('.global-filter-btn');
        globalFilterBtns.forEach(btn => {
            if (btn.dataset.filter === globalState.filter) {
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
                    appContainer.innerHTML = `
                        <div class="placeholder-view">
                            <h2>Trends</h2>
                            <p>Coming soon! This will show downtime trends over time.</p>
                        </div>
                    `;
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
