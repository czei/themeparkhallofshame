/**
 * Heatmap Renderer Utility
 * ========================
 *
 * Reusable class for rendering heatmap visualizations.
 * Extracted from park-detail.html for reuse across the application.
 *
 * Usage:
 * ```js
 * const renderer = new HeatmapRenderer({
 *   entities: [{entity_id: 1, entity_name: "Magic Kingdom", rank: 1, total_value: 56.8, location: "Orlando, FL"}],
 *   timeLabels: ["6:00", "7:00", ...],
 *   matrix: [[45, 52, 68, ...]],
 *   metric: 'avg_wait_time_minutes',
 *   granularity: 'hourly',
 *   onCellClick: (entityId, entityType, timeLabel) => { ... },
 *   getEntityLabel: (entity) => entity.entity_name,
 *   getTierBadge: (entity) => entity.tier ? `<span class="tier-badge tier-${entity.tier}">T${entity.tier}</span>` : ''
 * });
 * renderer.render(document.getElementById('heatmap-container'));
 * ```
 */
class HeatmapRenderer {
    constructor({
        entities,        // [{entity_id, entity_name, rank, total_value, ...}]
        timeLabels,      // ["6:00", "7:00", ...] or ["Dec 05", ...]
        matrix,          // 2D array [entity][time] with numeric values
        metric,          // 'downtime_hours' | 'avg_wait_time_minutes'
        metricUnit,      // 'hours' | 'minutes'
        granularity,     // 'hourly' | 'daily'
        onCellClick = null,     // Optional: (entityId, entityType, timeLabel) => void
        getEntityLabel = (entity) => entity.entity_name,  // Function to extract entity label
        getTierBadge = null     // Optional: (entity) => HTML string for tier badge
    }) {
        this.entities = entities;
        this.timeLabels = timeLabels;
        this.matrix = matrix;
        this.metric = metric;
        this.metricUnit = metricUnit || 'minutes';
        this.granularity = granularity;
        this.onCellClick = onCellClick;
        this.getEntityLabel = getEntityLabel;
        this.getTierBadge = getTierBadge;

        this.isDowntime = metric === 'downtime_hours';
    }

    /**
     * Render the heatmap into the specified container element.
     */
    render(container) {
        if (!container) {
            console.error('HeatmapRenderer: No container provided');
            return;
        }

        // Check if we have data
        if (!this.entities || this.entities.length === 0) {
            container.innerHTML = '<p style="text-align: center; color: #6c757d; padding: 2rem;">No data available for this period</p>';
            return;
        }

        const maxValue = this._calculateMaxValue();
        const columnWidth = this._calculateColumnWidth();

        // Build grid layout
        const numCols = this.timeLabels.length + 2; // entity name + time columns + total
        container.style.cssText = `
            display: grid;
            grid-template-columns: 180px repeat(${this.timeLabels.length}, ${columnWidth}px) 60px;
            gap: 1px;
            background: #e9ecef;
            border-radius: 8px;
            overflow: hidden;
            min-width: fit-content;
        `;

        // Render header row
        let html = this._renderHeaderRow();

        // Render data rows
        html += this._renderDataRows(maxValue);

        // Render legend
        html += this._renderLegend();

        container.innerHTML = html;

        // Attach click handlers if provided
        if (this.onCellClick) {
            this._attachClickHandlers(container);
        }
    }

    /**
     * Calculate the maximum value in the matrix for color scaling.
     */
    _calculateMaxValue() {
        const allValues = this.matrix
            .flat()
            .filter(v => v !== null && v !== undefined)
            .map(v => parseFloat(v) || 0);

        // Set minimum max value based on metric type
        const minMax = this.isDowntime ? 0.5 : 30;
        return Math.max(...allValues, minMax);
    }

    /**
     * Calculate column width based on number of time labels.
     * Wider columns for daily data (fewer columns), narrower for hourly.
     */
    _calculateColumnWidth() {
        if (this.timeLabels.length <= 7) return 100;  // Daily (week)
        if (this.timeLabels.length <= 14) return 60;  // Daily (2 weeks)
        return 40;  // Hourly or monthly daily
    }

    /**
     * Render the header row with time labels.
     */
    _renderHeaderRow() {
        let html = '<div class="heatmap-header-cell ride-name">Entity</div>';

        this.timeLabels.forEach(label => {
            html += `<div class="heatmap-header-cell">${this._escapeHtml(label)}</div>`;
        });

        html += '<div class="heatmap-header-cell total-col">Total</div>';
        return html;
    }

    /**
     * Render data rows for each entity.
     */
    _renderDataRows(maxValue) {
        let html = '';

        this.entities.forEach((entity, entityIdx) => {
            const label = this.getEntityLabel(entity);
            const tierBadge = this.getTierBadge ? this.getTierBadge(entity) : '';
            const total = entity.total_value || this._calculateTotal(this.matrix[entityIdx]);

            // Entity name cell with optional tier badge
            html += `<div class="heatmap-ride-name" title="${this._escapeHtml(label)}" data-entity-id="${entity.entity_id}">
                ${tierBadge}
                <span style="overflow: hidden; text-overflow: ellipsis;">${this._escapeHtml(label)}</span>
            </div>`;

            // Time cells
            this.matrix[entityIdx].forEach((value, timeIdx) => {
                const numValue = parseFloat(value) || 0;
                const color = this._getColor(numValue, maxValue);
                const textColor = numValue > maxValue * 0.5 ? '#fff' : '#212529';
                const displayValue = this._formatCellValue(numValue);
                const tooltipText = this._formatTooltipText(label, this.timeLabels[timeIdx], numValue);

                html += `<div class="heatmap-cell"
                    style="background: ${color}; color: ${textColor};"
                    data-entity-id="${entity.entity_id}"
                    data-time-label="${this._escapeHtml(this.timeLabels[timeIdx])}"
                    data-value="${numValue}">
                    ${displayValue}
                    <div class="heatmap-tooltip">${tooltipText}</div>
                </div>`;
            });

            // Total cell
            const maxTotal = this.isDowntime
                ? this.timeLabels.length
                : this.timeLabels.length * 120;
            const totalColor = this._getColor(total, maxValue * this.timeLabels.length * 0.3);
            const totalTextColor = total > maxValue * this.timeLabels.length * 0.15 ? '#fff' : '#212529';
            const totalDisplay = this._formatTotalValue(total);

            html += `<div class="heatmap-cell total-cell"
                style="background: ${totalColor}; color: ${totalTextColor};"
                data-entity-id="${entity.entity_id}">
                ${totalDisplay}
            </div>`;
        });

        return html;
    }

    /**
     * Render the color scale legend.
     */
    _renderLegend() {
        const metricLabel = this.isDowntime ? 'Downtime:' : 'Wait Time:';
        const maxLabel = this.isDowntime ? 'High' : 'Long';

        return `
            <div style="grid-column: 1 / -1; padding: 12px 0 4px 0;">
                <div class="heatmap-legend">
                    <span>${metricLabel}</span>
                    <span>None</span>
                    <div class="heatmap-legend-scale">
                        <div style="background: #ffffff;"></div>
                        <div style="background: #ffffc8;"></div>
                        <div style="background: #ffeb64;"></div>
                        <div style="background: #ffcd32;"></div>
                        <div style="background: #ff7d14;"></div>
                        <div style="background: #dc3514;"></div>
                    </div>
                    <span>${maxLabel}</span>
                </div>
            </div>
        `;
    }

    /**
     * Calculate total value for an entity (sum across time period).
     */
    _calculateTotal(dataArray) {
        return dataArray.reduce((sum, v) => sum + (parseFloat(v) || 0), 0);
    }

    /**
     * Get color for a value based on ratio to max value.
     * Gradient: white → light yellow → yellow → orange → red
     */
    _getColor(value, max) {
        if (value === null || value === undefined || value === 0) {
            return '#ffffff'; // White for zero/no data
        }

        const ratio = Math.min(value / max, 1);

        if (ratio < 0.25) {
            // White to light yellow
            const t = ratio / 0.25;
            return `rgb(255, 255, ${Math.round(255 - t * 50)})`;
        } else if (ratio < 0.5) {
            // Light yellow to yellow
            const t = (ratio - 0.25) / 0.25;
            return `rgb(255, ${Math.round(255 - t * 50)}, ${Math.round(205 - t * 105)})`;
        } else if (ratio < 0.75) {
            // Yellow to orange
            const t = (ratio - 0.5) / 0.25;
            return `rgb(255, ${Math.round(205 - t * 80)}, ${Math.round(100 - t * 50)})`;
        } else {
            // Orange to red
            const t = (ratio - 0.75) / 0.25;
            return `rgb(${Math.round(255 - t * 35)}, ${Math.round(125 - t * 80)}, ${Math.round(50 - t * 30)})`;
        }
    }

    /**
     * Format a cell value for display.
     */
    _formatCellValue(value) {
        if (value === 0) return '';

        if (this.isDowntime) {
            return value > 0 ? value.toFixed(1) : '';
        } else {
            return value > 0 ? Math.round(value) : '';
        }
    }

    /**
     * Format a total value for display.
     */
    _formatTotalValue(total) {
        if (this.isDowntime) {
            return total.toFixed(1) + 'h';
        } else {
            return Math.round(total) + 'm';
        }
    }

    /**
     * Format tooltip text.
     */
    _formatTooltipText(entityLabel, timeLabel, value) {
        if (this.isDowntime) {
            return `${entityLabel} @ ${timeLabel}: ${value.toFixed(2)}h down`;
        } else {
            return `${entityLabel} @ ${timeLabel}: ${Math.round(value)} min wait`;
        }
    }

    /**
     * Attach click handlers to cells and entity names.
     */
    _attachClickHandlers(container) {
        // Click handlers for heatmap cells (time period cells)
        const cells = container.querySelectorAll('.heatmap-cell[data-entity-id]');

        cells.forEach(cell => {
            cell.style.cursor = 'pointer';
            cell.addEventListener('click', () => {
                const entityId = parseInt(cell.dataset.entityId);
                const timeLabel = cell.dataset.timeLabel;
                const entity = this.entities.find(e => e.entity_id === entityId);

                if (entity && this.onCellClick) {
                    // Determine entity type based on presence of location vs park_name
                    const entityType = entity.location ? 'park' : 'ride';
                    this.onCellClick(entityId, entityType, timeLabel);
                }
            });
        });

        // Click handlers for entity names (park/ride name cells in first column)
        const entityNames = container.querySelectorAll('.heatmap-ride-name[data-entity-id]');

        entityNames.forEach(nameCell => {
            nameCell.style.cursor = 'pointer';
            nameCell.addEventListener('click', () => {
                const entityId = parseInt(nameCell.dataset.entityId);
                const entity = this.entities.find(e => e.entity_id === entityId);

                if (entity && this.onCellClick) {
                    // Determine entity type based on presence of location vs park_name
                    const entityType = entity.location ? 'park' : 'ride';
                    // Pass null for timeLabel since entity name click is not time-specific
                    this.onCellClick(entityId, entityType, null);
                }
            });
        });
    }

    /**
     * Escape HTML to prevent XSS.
     */
    _escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}
