// Theme Park Hall of Shame - Main Application Controller

document.addEventListener('DOMContentLoaded', () => {
    console.log('Theme Park Hall of Shame - Application Loading...');

    // Tab switching logic
    const navTabs = document.querySelectorAll('.nav-tab');
    const appContainer = document.getElementById('app-container');
    let currentComponent = null;

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

    async function loadView(viewName) {
        // Clear container
        appContainer.innerHTML = '<div id="view-container"></div>';

        console.log(`Loading view: ${viewName}`);

        try {
            switch(viewName) {
                case 'park-rankings':
                    if (typeof ParkRankings !== 'undefined') {
                        currentComponent = new ParkRankings(apiClient, 'view-container');
                        await currentComponent.init();
                    } else {
                        throw new Error('ParkRankings component not loaded');
                    }
                    break;

                case 'ride-performance':
                    if (typeof RidePerformance !== 'undefined') {
                        currentComponent = new RidePerformance(apiClient, 'view-container');
                        await currentComponent.init();
                    } else {
                        throw new Error('RidePerformance component not loaded');
                    }
                    break;

                case 'wait-times':
                    appContainer.innerHTML = `
                        <div class="placeholder-view">
                            <h2>Wait Times</h2>
                            <p>Coming soon! This will show live and historical wait times.</p>
                        </div>
                    `;
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
