/**
 * Theme Park Hall of Shame - About Modal Component
 * Displays information about the project, data sources, and how to use the app
 */

class AboutModal {
    constructor() {
        this.state = {
            isOpen: false
        };
        this.contentCache = null;
    }

    /**
     * Open the About modal
     */
    async open() {
        this.state.isOpen = true;
        await this.loadContent();
        this.render();
    }

    /**
     * Load about content from external HTML file
     */
    async loadContent() {
        if (this.contentCache) return;

        try {
            const response = await fetch('/about-content.html');
            if (!response.ok) {
                throw new Error(`Failed to load about content: ${response.status}`);
            }
            this.contentCache = await response.text();
        } catch (error) {
            console.error('Error loading about content:', error);
            this.contentCache = '<p>Error loading content. Please try again.</p>';
        }
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
     * Render about content from cached HTML
     */
    renderAboutContent() {
        return this.contentCache || '<p>Loading...</p>';
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
