/**
 * Theme Park Hall of Shame - Search Component
 * Global search modal with Fuse.js fuzzy search for parks and rides.
 * Activated by: search icon click or Cmd/Ctrl+K keyboard shortcut.
 */

class Search {
    constructor(apiClient) {
        this.apiClient = apiClient;
        this.fuse = null;
        this.searchIndex = null;
        this.isLoading = false;
        this.selectedIndex = 0;
        this.results = [];

        // Create modal elements
        this.createModal();
        this.attachEventListeners();
    }

    /**
     * Create the search modal HTML structure
     */
    createModal() {
        // Create modal container
        const modal = document.createElement('div');
        modal.className = 'search-modal-overlay';
        modal.id = 'search-modal';
        modal.innerHTML = `
            <div class="search-modal">
                <div class="search-input-wrapper">
                    <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="11" cy="11" r="8"></circle>
                        <path d="m21 21-4.35-4.35"></path>
                    </svg>
                    <input
                        type="text"
                        class="search-input"
                        id="search-input"
                        placeholder="Search parks and rides..."
                        autocomplete="off"
                        spellcheck="false"
                    >
                    <div class="search-shortcut">
                        <kbd>esc</kbd>
                    </div>
                </div>
                <div class="search-results" id="search-results">
                    <div class="search-hint">
                        Start typing to search...
                    </div>
                </div>
                <div class="search-footer">
                    <span class="search-footer-hint">
                        <kbd>&uarr;</kbd><kbd>&darr;</kbd> to navigate
                        <kbd>enter</kbd> to select
                        <kbd>esc</kbd> to close
                    </span>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        // Store references
        this.modal = modal;
        this.input = modal.querySelector('#search-input');
        this.resultsContainer = modal.querySelector('#search-results');
    }

    /**
     * Attach event listeners for search functionality
     */
    attachEventListeners() {
        // Close on overlay click
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.close();
            }
        });

        // Keyboard shortcut to open (Cmd/Ctrl+K)
        document.addEventListener('keydown', (e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
                e.preventDefault();
                this.open();
            }

            // ESC to close
            if (e.key === 'Escape' && this.isOpen()) {
                this.close();
            }
        });

        // Input handling
        this.input.addEventListener('input', (e) => {
            this.handleSearch(e.target.value);
        });

        // Keyboard navigation in results
        this.input.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                this.navigateResults(1);
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                this.navigateResults(-1);
            } else if (e.key === 'Enter') {
                e.preventDefault();
                this.selectResult();
            }
        });
    }

    /**
     * Check if modal is currently open
     */
    isOpen() {
        return this.modal.classList.contains('active');
    }

    /**
     * Open the search modal
     */
    async open() {
        this.modal.classList.add('active');
        document.body.style.overflow = 'hidden';
        this.input.value = '';
        this.selectedIndex = 0;
        this.results = [];
        this.renderResults([]);

        // Focus input after animation
        setTimeout(() => {
            this.input.focus();
        }, 50);

        // Load search index if not loaded
        if (!this.searchIndex) {
            await this.loadSearchIndex();
        }
    }

    /**
     * Close the search modal
     */
    close() {
        this.modal.classList.remove('active');
        document.body.style.overflow = '';
        this.input.value = '';
        this.results = [];
    }

    /**
     * Load search index from API and initialize Fuse.js
     */
    async loadSearchIndex() {
        if (this.isLoading) return;

        this.isLoading = true;
        this.renderLoadingState();

        try {
            const response = await this.apiClient.get('/search/index');

            if (response.success) {
                // Combine parks and rides into single searchable array
                this.searchIndex = [
                    ...response.parks,
                    ...response.rides
                ];

                // Initialize Fuse.js with fuzzy search config
                this.fuse = new Fuse(this.searchIndex, {
                    keys: [
                        { name: 'name', weight: 0.7 },
                        { name: 'park_name', weight: 0.3 },
                        { name: 'location', weight: 0.2 }
                    ],
                    threshold: 0.3,
                    distance: 100,
                    includeScore: true,
                    minMatchCharLength: 1
                });

                this.renderResults([]);
            } else {
                this.renderErrorState('Failed to load search data');
            }
        } catch (error) {
            console.error('Failed to load search index:', error);
            this.renderErrorState('Failed to load search data');
        } finally {
            this.isLoading = false;
        }
    }

    /**
     * Handle search input
     */
    handleSearch(query) {
        if (!query || query.length < 1) {
            this.results = [];
            this.renderResults([]);
            return;
        }

        if (!this.fuse) {
            return;
        }

        // Perform fuzzy search
        const fuseResults = this.fuse.search(query);

        // Limit to top 10 results, grouped by type
        this.results = fuseResults.slice(0, 10).map(r => r.item);
        this.selectedIndex = 0;
        this.renderResults(this.results);
    }

    /**
     * Render search results
     */
    renderResults(results) {
        if (this.isLoading) return;

        if (results.length === 0 && !this.input.value) {
            this.resultsContainer.innerHTML = `
                <div class="search-hint">
                    Start typing to search parks and rides...
                </div>
            `;
            return;
        }

        if (results.length === 0 && this.input.value) {
            this.resultsContainer.innerHTML = `
                <div class="search-no-results">
                    No results found for "${this.escapeHtml(this.input.value)}"
                </div>
            `;
            return;
        }

        // Group results by type
        const parks = results.filter(r => r.type === 'park');
        const rides = results.filter(r => r.type === 'ride');

        let html = '';
        let globalIndex = 0;

        if (parks.length > 0) {
            html += '<div class="search-group"><div class="search-group-label">Parks</div>';
            parks.forEach((park) => {
                const isSelected = globalIndex === this.selectedIndex;
                html += this.renderResultItem(park, globalIndex, isSelected);
                globalIndex++;
            });
            html += '</div>';
        }

        if (rides.length > 0) {
            html += '<div class="search-group"><div class="search-group-label">Rides</div>';
            rides.forEach((ride) => {
                const isSelected = globalIndex === this.selectedIndex;
                html += this.renderResultItem(ride, globalIndex, isSelected);
                globalIndex++;
            });
            html += '</div>';
        }

        this.resultsContainer.innerHTML = html;

        // Attach click handlers
        this.resultsContainer.querySelectorAll('.search-result-item').forEach((item, index) => {
            item.addEventListener('click', () => {
                this.selectedIndex = parseInt(item.dataset.index);
                this.selectResult();
            });
            item.addEventListener('mouseenter', () => {
                this.selectedIndex = parseInt(item.dataset.index);
                this.updateSelectedHighlight();
            });
        });
    }

    /**
     * Render a single search result item
     */
    renderResultItem(item, index, isSelected) {
        const subtitle = item.type === 'park' ? item.location : item.park_name;
        const icon = item.type === 'park'
            ? '<svg class="result-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 21h18M9 8h1M9 12h1M9 16h1M14 8h1M14 12h1M14 16h1M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16"></path></svg>'
            : '<svg class="result-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><path d="M12 6v6l4 2"></path></svg>';

        return `
            <div class="search-result-item ${isSelected ? 'selected' : ''}" data-index="${index}" data-url="${item.url}">
                ${icon}
                <div class="result-content">
                    <div class="result-name">${this.escapeHtml(item.name)}</div>
                    <div class="result-subtitle">${this.escapeHtml(subtitle || '')}</div>
                </div>
                <div class="result-type-badge">${item.type}</div>
            </div>
        `;
    }

    /**
     * Render loading state
     */
    renderLoadingState() {
        this.resultsContainer.innerHTML = `
            <div class="search-loading">
                <div class="search-spinner"></div>
                <span>Loading search index...</span>
            </div>
        `;
    }

    /**
     * Render error state
     */
    renderErrorState(message) {
        this.resultsContainer.innerHTML = `
            <div class="search-error">
                <span>${message}</span>
                <button class="search-retry-btn">Retry</button>
            </div>
        `;

        this.resultsContainer.querySelector('.search-retry-btn')?.addEventListener('click', () => {
            this.loadSearchIndex();
        });
    }

    /**
     * Navigate through results with keyboard
     */
    navigateResults(direction) {
        if (this.results.length === 0) return;

        this.selectedIndex += direction;

        if (this.selectedIndex < 0) {
            this.selectedIndex = this.results.length - 1;
        } else if (this.selectedIndex >= this.results.length) {
            this.selectedIndex = 0;
        }

        this.updateSelectedHighlight();
    }

    /**
     * Update visual highlight for selected result
     */
    updateSelectedHighlight() {
        const items = this.resultsContainer.querySelectorAll('.search-result-item');
        items.forEach((item, index) => {
            item.classList.toggle('selected', index === this.selectedIndex);
        });

        // Scroll selected item into view
        const selectedItem = items[this.selectedIndex];
        if (selectedItem) {
            selectedItem.scrollIntoView({ block: 'nearest' });
        }
    }

    /**
     * Select the current result (navigate to its URL)
     */
    selectResult() {
        if (this.results.length === 0) return;

        const selected = this.results[this.selectedIndex];
        if (selected && selected.url) {
            this.close();
            window.location.href = selected.url;
        }
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
}

// Export for use in app.js
window.Search = Search;
