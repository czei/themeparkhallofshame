"""
Heatmap Transformation Utilities
=================================

Helpers for transforming Chart.js format data into heatmap matrix format.

This module provides lightweight transformation functions that convert
existing chart query responses into the matrix structure needed for heatmaps,
without duplicating SQL query logic.
"""

from typing import Dict, Any, List, Optional


def transform_chart_to_heatmap(
    chart_data: Dict[str, Any],
    period: str,
    metric: str,
    metric_unit: str = "minutes"
) -> Dict[str, Any]:
    """
    Transform Chart.js format to heatmap matrix format.

    Converts the datasets array structure used by Chart.js into the
    matrix structure needed for heatmap visualizations, while preserving
    all entity metadata and adding ranking information.

    Args:
        chart_data: Chart.js format data with labels and datasets
            {
              "labels": ["Dec 09", "Dec 10", ...],
              "datasets": [
                {"label": "Magic Kingdom", "data": [45, 52, 68, ...]},
                {"label": "EPCOT", "data": [38, 41, 55, ...]}
              ],
              "granularity": "daily" | "hourly"
            }
        period: Period identifier (today, yesterday, last_week, last_month)
        metric: Metric name (e.g., 'avg_wait_time_minutes', 'downtime_hours')
        metric_unit: Unit of measurement ("minutes" or "hours")

    Returns:
        Heatmap format data:
        {
          "success": true,
          "period": "last_week",
          "granularity": "daily",
          "title": "Top 10 Parks by Average Wait Time (Last Week)",
          "metric": "avg_wait_time_minutes",
          "metric_unit": "minutes",
          "timezone": "America/Los_Angeles",
          "entities": [
            {
              "entity_id": 1,
              "entity_name": "Magic Kingdom",
              "rank": 1,
              "total_value": 55.0,
              "location": "Orlando, FL",  # for parks
              "tier": 1  # for rides
            }
          ],
          "time_labels": ["Dec 09", "Dec 10", ...],
          "matrix": [
            [45, 52, 68, ...],  # Magic Kingdom
            [38, 41, 55, ...]   # EPCOT
          ]
        }

    Note:
        - Entity metadata (entity_id, location, tier) must be included in
          the datasets by the chart query classes.
        - Ranking is computed based on total_value (average of non-null values).
        - Missing values (None) are preserved in the matrix.
    """
    # Extract datasets and labels
    datasets = chart_data.get("datasets", [])
    time_labels = chart_data.get("labels", [])
    granularity = chart_data.get("granularity", "daily")

    # Convert datasets array to matrix (convert string values to float)
    matrix = []
    for dataset in datasets:
        row = []
        for value in dataset["data"]:
            if value is None:
                row.append(None)
            else:
                # Convert to float to handle both numeric and string inputs from SQL
                row.append(float(value))
        matrix.append(row)

    # Build entities with ranking and metadata
    entities = []
    for idx, dataset in enumerate(datasets):
        # Calculate total value (average across time period)
        # Convert string values to float first
        values = [float(v) for v in dataset["data"] if v is not None]
        total_value = sum(values) / len(values) if values else 0.0

        entity = {
            "entity_id": dataset.get("entity_id"),
            "entity_name": dataset["label"],
            "rank": idx + 1,
            "total_value": round(total_value, 1)
        }

        # Add optional metadata (location for parks, tier for rides)
        if "location" in dataset:
            entity["location"] = dataset["location"]
        if "tier" in dataset:
            entity["tier"] = dataset["tier"]
        if "park_name" in dataset:
            entity["park_name"] = dataset["park_name"]

        entities.append(entity)

    # Generate title
    period_display = period.replace("_", " ").title()
    entity_type = "Parks" if ("park" in metric or (datasets and "location" in datasets[0])) else "Rides"
    metric_display = "Wait Time" if "wait" in metric else "Downtime"
    title = f"Top {len(entities)} {entity_type} by {metric_display} ({period_display})"

    return {
        "success": True,
        "period": period,
        "granularity": granularity,
        "title": title,
        "metric": metric,
        "metric_unit": metric_unit,
        "timezone": "America/Los_Angeles",
        "entities": entities,
        "time_labels": time_labels,
        "matrix": matrix
    }


def validate_heatmap_period(period: str) -> bool:
    """
    Validate that the period is supported for heatmaps.

    Heatmaps are not available for LIVE period because they require
    time-series data with multiple time points.

    Args:
        period: Period identifier to validate

    Returns:
        True if period is valid for heatmaps, False otherwise
    """
    valid_periods = ['today', 'yesterday', 'last_week', 'last_month']
    return period in valid_periods
