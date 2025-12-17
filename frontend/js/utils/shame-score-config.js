/**
 * Shame Score Configuration
 * =========================
 *
 * Centralized configuration for shame score color thresholds and styling.
 * Used by both downtime tables and heatmap visualizations.
 */

const ShameScoreConfig = {
    /**
     * Color thresholds for shame scores.
     * Thresholds are evaluated in order from highest to lowest.
     */
    thresholds: [
        { min: 2.0, color: '#FF6B5A', cssClass: 'shame-critical', label: 'Critical' },  // Red
        { min: 1.0, color: '#FFB627', cssClass: 'shame-high', label: 'High' },          // Orange
        { min: 0.5, color: '#FFE066', cssClass: 'shame-medium', label: 'Medium' },      // Yellow
        { min: 0.0, color: '#E8F7F8', cssClass: 'shame-low', label: 'Low' }             // Very light blue
    ],

    /**
     * Get color for a shame score value.
     * @param {number} score - Shame score value
     * @returns {string} - Hex color code
     */
    getColor(score) {
        if (score === null || score === undefined || isNaN(score)) {
            return '#ffffff'; // White for no data
        }

        const numScore = Number(score);

        // Find matching threshold (evaluated from highest to lowest)
        for (const threshold of this.thresholds) {
            if (numScore >= threshold.min) {
                return threshold.color;
            }
        }

        return '#ffffff'; // Fallback to white
    },

    /**
     * Get CSS class for a shame score value.
     * @param {number} score - Shame score value
     * @returns {string} - CSS class name
     */
    getCssClass(score) {
        if (score === null || score === undefined || isNaN(score)) {
            return '';
        }

        const numScore = Number(score);

        // Find matching threshold (evaluated from highest to lowest)
        for (const threshold of this.thresholds) {
            if (numScore >= threshold.min) {
                return threshold.cssClass;
            }
        }

        return '';
    },

    /**
     * Get label for a shame score value.
     * @param {number} score - Shame score value
     * @returns {string} - Human-readable label
     */
    getLabel(score) {
        if (score === null || score === undefined || isNaN(score)) {
            return 'None';
        }

        const numScore = Number(score);

        // Find matching threshold (evaluated from highest to lowest)
        for (const threshold of this.thresholds) {
            if (numScore >= threshold.min) {
                return threshold.label;
            }
        }

        return 'None';
    }
};
