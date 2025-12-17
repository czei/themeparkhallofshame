/**
 * Metrics Color Configuration
 * ============================
 *
 * Centralized configuration for metric color thresholds and styling.
 * Used by both tables and heatmap visualizations.
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

/**
 * Wait Time Color Configuration
 * ==============================
 *
 * Centralized color configuration for wait time heatmaps.
 * Applies to both park and ride wait time visualizations.
 */
const WaitTimeConfig = {
    /**
     * Get color for wait time value (in minutes).
     * >= 60 minutes: Red
     * < 60 minutes: Smooth gradient from white to orange
     *
     * @param {number} minutes - Wait time in minutes
     * @returns {string} - Hex color code
     */
    getColor(minutes) {
        if (minutes === null || minutes === undefined || isNaN(minutes) || minutes === 0) {
            return '#ffffff'; // White for zero/no data
        }

        const numMinutes = Number(minutes);

        // >= 60 minutes: Red
        if (numMinutes >= 60) {
            return '#FF6B5A'; // Red
        }

        // 0-60 minutes: Smooth gradient from white to orange
        // White (#ffffff) → Light Yellow → Yellow → Light Orange → Orange (#FFB627)
        const ratio = numMinutes / 60; // 0.0 to 1.0

        if (ratio < 0.25) {
            // White to light yellow (0-15 minutes)
            const t = ratio / 0.25;
            return `rgb(255, 255, ${Math.round(255 - t * 50)})`;
        } else if (ratio < 0.5) {
            // Light yellow to yellow (15-30 minutes)
            const t = (ratio - 0.25) / 0.25;
            return `rgb(255, ${Math.round(255 - t * 50)}, ${Math.round(205 - t * 105)})`;
        } else if (ratio < 0.75) {
            // Yellow to light orange (30-45 minutes)
            const t = (ratio - 0.5) / 0.25;
            return `rgb(255, ${Math.round(205 - t * 24)}, ${Math.round(100 - t * 61)})`;
        } else {
            // Light orange to orange (45-60 minutes)
            const t = (ratio - 0.75) / 0.25;
            const r = 255;
            const g = Math.round(181 - t * 0); // Keep at 181 (0xB5)
            const b = Math.round(39 - t * 0);  // Keep at 39 (0x27)
            return `rgb(${r}, ${g}, ${b})`; // #FFB527 ≈ #FFB627
        }
    }
};
