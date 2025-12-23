# Feature Specification: Wait Time Pattern Analysis and Predictive Modeling

**Feature Branch**: `005-wait-time-analysis`
**Created**: 2025-12-21
**Status**: Draft - Blocked by features 003 and 004
**Input**: User description: "wait time pattern analysis and predictive modeling"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Discover Wait Time Correlations (Priority: P1)

The system needs to identify which factors (weather, day of week, school holidays, special events, ride tier) actually correlate with wait times and measure correlation strength.

**Why this priority**: This is foundational analysis that determines what data points matter for prediction. Without correlation analysis, we're guessing which factors to use in models. This validates or disproves assumptions (e.g., does weather really impact wait times?).

**Independent Test**: Can be fully tested by running correlation analysis on historical data (90+ days), generating correlation coefficients for each factor-wait time pair, and validating that results match hand-calculated correlations on sample data.

**Acceptance Scenarios**:

1. **Given** 90 days of historical wait time and weather data, **When** correlation analysis runs, **Then** system produces correlation coefficients (-1 to +1) for each weather variable (temperature, precipitation, UV index) vs. wait times
2. **Given** historical data spans weekdays and weekends, **When** day-of-week analysis runs, **Then** system identifies which days have significantly higher/lower average wait times with statistical significance (p < 0.05)
3. **Given** historical data includes school holiday periods, **When** holiday correlation analysis runs, **Then** system quantifies wait time increase during school breaks vs. non-holiday periods
4. **Given** historical data includes special events (After Hours, holiday parties), **When** event correlation analysis runs, **Then** system measures wait time impact of each event type
5. **Given** multiple potential correlations exist, **When** filtering for statistical significance, **Then** system excludes correlations with p-value > 0.05 (spurious correlations)

---

### User Story 2 - Identify Temporal Wait Time Patterns (Priority: P1)

The system needs to discover repeating patterns in wait times across different time scales (hour of day, day of week, month of year) to enable time-based predictions.

**Why this priority**: Temporal patterns are the strongest predictors of wait times (e.g., rides are always busiest 11am-2pm). This analysis enables "what time should I ride X?" recommendations essential for visit optimization.

**Independent Test**: Can be tested by extracting hourly/daily/monthly patterns from historical data, visualizing pattern curves, and validating that patterns are statistically distinct (ANOVA F-test shows significant time-of-day effect).

**Acceptance Scenarios**:

1. **Given** historical wait time data, **When** hourly pattern analysis runs, **Then** system produces average wait time by hour of day (0-23) with confidence intervals showing peak hours (typically 11am-3pm)
2. **Given** data spans full weeks, **When** day-of-week pattern analysis runs, **Then** system identifies weekday vs. weekend differences with effect size (Cohen's d > 0.5 for meaningful difference)
3. **Given** data spans multiple months, **When** seasonal pattern analysis runs, **Then** system detects summer vs. winter vs. spring/fall wait time differences for seasonal rides
4. **Given** ride tier classifications exist (Tier 1/2/3), **When** tier-based pattern analysis runs, **Then** system shows Tier 1 rides have 2-3x higher average waits than Tier 3 rides
5. **Given** patterns are discovered, **When** pattern stability is measured, **Then** system reports which patterns are consistent (low variance) vs. volatile (high variance) across time periods

---

### User Story 3 - Build and Validate Predictive Models (Priority: P1)

The system needs to train predictive models that forecast wait times based on identified correlations and patterns, then validate prediction accuracy against held-out test data.

**Why this priority**: This is the core deliverable enabling feature 006 (visit optimization). Models must achieve ±15 minute accuracy for 80% of predictions to be useful for planning. Validation proves models work before deployment.

**Independent Test**: Can be tested by training models on 70% of historical data, predicting on held-out 30%, and measuring prediction error (MAE, RMSE) against target accuracy thresholds.

**Acceptance Scenarios**:

1. **Given** historical data with identified correlations, **When** baseline model trains using time-of-day and day-of-week only, **Then** baseline achieves Mean Absolute Error (MAE) < 20 minutes on test set
2. **Given** weather data is available, **When** enhanced model trains with weather variables added, **Then** enhanced model improves MAE by 15%+ vs. baseline (measuring weather impact)
3. **Given** multiple model approaches are available (linear regression, decision tree, ensemble), **When** model comparison runs, **Then** system selects best-performing model based on test set accuracy
4. **Given** predictions include confidence intervals, **When** model outputs predictions, **Then** 80% of actual wait times fall within predicted confidence interval (model calibration)
5. **Given** models are trained per park and per ride tier, **When** validation runs, **Then** Tier 1 predictions achieve ±15 min accuracy for 80% of predictions, Tier 2/3 achieve ±10 min (lower variance)

---

### User Story 4 - Rank Feature Importance (Priority: P2)

The system needs to identify which input variables have the strongest predictive power, guiding future data collection priorities and model simplification.

**Why this priority**: Not all collected data points matter equally. Feature importance analysis tells us "temperature matters more than wind speed" or "school holidays matter more than UV index," enabling focused data collection and model optimization.

**Independent Test**: Can be tested by computing feature importance scores (permutation importance, SHAP values), ranking features by importance, and validating that removing low-importance features doesn't degrade model accuracy.

**Acceptance Scenarios**:

1. **Given** a trained model with multiple input features, **When** feature importance analysis runs, **Then** system produces ranked list of features by importance score (0-1 normalized)
2. **Given** feature importance scores exist, **When** top 5 features are identified, **Then** these features explain 80%+ of model predictive power (Pareto principle)
3. **Given** low-importance features are identified (score < 0.05), **When** these features are removed from model, **Then** model accuracy degrades by < 2% (features were noise)
4. **Given** weather variables have varying importance, **When** analysis completes, **Then** system identifies which weather factors matter (e.g., precipitation > temperature > wind)
5. **Given** feature importance varies by park, **When** park-specific analysis runs, **Then** system identifies park-specific patterns (e.g., Florida parks more weather-sensitive than California)

---

### User Story 5 - Monitor Model Performance Over Time (Priority: P2)

The system needs to track prediction accuracy on live data, detect model drift (accuracy degradation), and trigger retraining when models become stale.

**Why this priority**: Models trained on historical data degrade over time as park operations, visitor behavior, and external factors change. Monitoring prevents silent failures where predictions become unreliable without detection.

**Independent Test**: Can be tested by comparing predicted vs. actual wait times daily, tracking rolling MAE over 30-day window, and triggering retraining alert when MAE exceeds threshold by 25%+.

**Acceptance Scenarios**:

1. **Given** model makes daily predictions, **When** actual wait times are observed, **Then** system calculates prediction error (predicted - actual) for each prediction
2. **Given** prediction errors accumulate over 30 days, **When** rolling MAE is calculated, **Then** system tracks trend (improving, stable, degrading)
3. **Given** rolling MAE exceeds trained baseline MAE by 25%, **When** drift detection triggers, **Then** system flags model for retraining and alerts monitoring
4. **Given** model is retrained quarterly, **When** new model is deployed, **Then** system compares new model accuracy vs. old model on recent data before promoting
5. **Given** multiple parks have models, **When** monitoring dashboard is viewed, **Then** system shows per-park accuracy metrics and highlights underperforming models

---

### Edge Cases

- What happens when insufficient historical data exists for a new park (< 90 days)?
- How does the system handle rides that recently changed tier classification?
- What happens when weather data has gaps (missing observations)?
- How are outlier wait times (10x normal) handled in pattern analysis?
- What happens when a special event type occurs for the first time (no historical pattern)?
- How does the system handle rides that are seasonal (only operate May-September)?
- What happens when ride capacity changes (new trains added, hourly throughput increases)?
- How are predictions adjusted for rides with ongoing refurbishment?
- What happens when school calendar data is missing for a region?
- How does the system handle prediction requests for dates beyond weather forecast horizon (7-14 days)?

## Requirements *(mandatory)*

### Functional Requirements

**Correlation Analysis:**
- **FR-001**: System MUST calculate Pearson correlation coefficients between wait times and all available factors (weather, day-of-week, school holidays, special events, ride tier)
- **FR-002**: System MUST compute statistical significance (p-values) for all correlations and filter out non-significant correlations (p > 0.05)
- **FR-003**: System MUST measure correlation strength and classify as weak (|r| < 0.3), moderate (0.3-0.7), or strong (|r| > 0.7)
- **FR-004**: System MUST detect and flag spurious correlations (high correlation but non-causal relationship)

**Pattern Discovery:**
- **FR-005**: System MUST extract hourly wait time patterns (average and variance by hour 0-23) for each ride
- **FR-006**: System MUST identify day-of-week effects using ANOVA and quantify effect size (Cohen's d)
- **FR-007**: System MUST detect seasonal patterns for rides that show monthly variance (seasonal attractions, weather-dependent rides)
- **FR-008**: System MUST measure ride tier impact on wait times and validate tier classifications reflect actual demand
- **FR-009**: System MUST identify special event patterns (holiday parties, after-hours events) and quantify wait time multipliers

**Predictive Modeling:**
- **FR-010**: System MUST train baseline predictive models using time-based features only (hour, day-of-week, month)
- **FR-011**: System MUST train enhanced models incorporating weather forecasts, school holidays, and special events
- **FR-012**: System MUST split historical data into training (70%), validation (15%), and test (15%) sets for unbiased evaluation
- **FR-013**: System MUST calculate prediction error metrics (MAE, RMSE, MAPE) on test set
- **FR-014**: System MUST generate confidence intervals for predictions based on historical prediction variance
- **FR-015**: System MUST compare multiple modeling approaches and select best-performing model per park/ride tier
- **FR-016**: System MUST achieve target accuracy: ±15 minutes for 80% of predictions, ±30 minutes for 95% of predictions

**Feature Importance:**
- **FR-017**: System MUST compute feature importance scores using permutation importance or SHAP values
- **FR-018**: System MUST rank features by predictive power and identify top contributors (top 20% of features)
- **FR-019**: System MUST measure cumulative importance (verify top N features explain 80%+ of variance)
- **FR-020**: System MUST identify low-value features (importance < 0.05) that can be excluded to simplify models

**Model Monitoring:**
- **FR-021**: System MUST track daily prediction errors (predicted vs. actual) for all active models
- **FR-022**: System MUST calculate rolling 30-day MAE and compare to baseline MAE from training
- **FR-023**: System MUST detect model drift when rolling MAE exceeds baseline by 25%+ and trigger retraining alert
- **FR-024**: System MUST log model performance metrics to monitoring dashboard
- **FR-025**: System MUST support A/B testing of new models vs. current models before promotion

**Output Generation:**
- **FR-026**: System MUST generate analysis reports showing correlations, patterns, and model accuracy for stakeholder review
- **FR-027**: System MUST export trained models in format consumable by feature 006 (visit optimization)
- **FR-028**: System MUST provide prediction API accepting date, park, ride, and conditions as input, returning predicted wait time with confidence interval
- **FR-029**: System MUST document data quality requirements (minimum 90 days history, < 10% missing data) for reliable predictions

### Key Entities

- **Correlation Analysis Result**: Statistical relationship between a factor and wait times. Includes factor_name (weather_temperature, day_of_week, school_holiday, etc.), correlation_coefficient (-1 to +1), p_value (statistical significance), sample_size, date_range_analyzed. Identifies which factors matter.

- **Temporal Pattern**: Repeating wait time pattern across time dimension. Includes pattern_type (HOURLY/DAILY/SEASONAL), time_granularity (hour 0-23, day Mon-Sun, month Jan-Dec), average_wait_by_period (array of averages), confidence_intervals, variance, pattern_stability_score (0-1). Enables "when to ride" recommendations.

- **Predictive Model**: Trained model forecasting wait times. Includes model_id, model_type (LINEAR_REGRESSION/DECISION_TREE/ENSEMBLE), training_date, input_features (list of variables), target_accuracy (MAE/RMSE targets), test_set_accuracy (actual performance), confidence_interval_width, applicable_parks (model scope), applicable_ride_tiers. Core artifact for predictions.

- **Feature Importance Ranking**: Ranked list of input variables by predictive power. Includes feature_name, importance_score (0-1 normalized), rank (1 to N), cumulative_importance (running total), statistical_confidence. Guides data collection priorities.

- **Model Performance Metric**: Accuracy measurement over time. Includes metric_date, predictions_made_count, mean_absolute_error, root_mean_squared_error, predictions_within_15min_pct, predictions_within_30min_pct, drift_indicator (boolean), model_version. Tracks model health.

- **Prediction Request/Response**: Input/output for wait time prediction. Request includes prediction_date, park_id, ride_id, weather_forecast, is_school_holiday, special_event_type. Response includes predicted_wait_minutes, confidence_interval_lower, confidence_interval_upper, prediction_confidence (0-1), data_quality_indicator, factors_considered.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Correlation analysis identifies at least 5 statistically significant factors (p < 0.05) that correlate with wait times across all parks

- **SC-002**: Temporal pattern analysis shows hour-of-day effect explains 40%+ of wait time variance (measured via R-squared)

- **SC-003**: Predictive models achieve Mean Absolute Error < 15 minutes for 80% of predictions on test set

- **SC-004**: Predictive models achieve Mean Absolute Error < 30 minutes for 95% of predictions on test set

- **SC-005**: Enhanced models (with weather/events) improve prediction accuracy by 15%+ compared to baseline (time-only) models

- **SC-006**: Feature importance analysis shows top 5 features explain 80%+ of model predictive power

- **SC-007**: Model drift detection identifies degrading models within 30 days of accuracy decline (before MAE exceeds threshold by 50%)

- **SC-008**: Analysis completes for all tracked parks within 48 hours of feature 003 data availability

- **SC-009**: Prediction API responds within 500ms for 95% of prediction requests

- **SC-010**: Model retraining (when triggered) completes within 6 hours and improves accuracy by 10%+ vs. stale model

## Scope & Boundaries

### In Scope

- Statistical correlation analysis between wait times and potential influence factors
- Temporal pattern discovery (hourly, daily, seasonal)
- Predictive model training and validation
- Feature importance ranking
- Model performance monitoring and drift detection
- Prediction API for downstream features (005)
- Analysis reporting and visualization of findings
- Model comparison and selection (testing multiple approaches)
- Confidence interval generation for predictions

### Out of Scope (Future Enhancements)

- Real-time model retraining (models update quarterly, not live)
- Deep learning / neural network models (starting with interpretable statistical models)
- Crowd density prediction (number of people in park, not just wait times)
- Individual visitor behavior modeling (predicting which rides a person will choose)
- Integration with external crowd prediction services
- Custom model tuning per individual ride (starting with park/tier level models)
- Causal inference analysis (correlation vs. causation - future research)

## Assumptions & Dependencies

### Assumptions

- At least 90 days of historical wait time data is available from existing system (feature 001) before model training
- Weather data (temperature, precipitation, UV index) is collected and available (feature 002/003)
- School calendar data for major metro areas is available and accurate
- Special event data is collected and classified (feature 003)
- Ride tier classifications (1/2/3) are assigned and maintained
- Statistical significance threshold p < 0.05 is appropriate for correlation filtering
- Target prediction accuracy (±15 min for 80%) is achievable with available data quality
- Models can be retrained quarterly without requiring daily updates
- Prediction horizon is 7-14 days (limited by weather forecast availability)
- Parks operate consistently enough that patterns learned from historical data remain valid

### Dependencies

**CRITICAL BLOCKING DEPENDENCY:**
- **Feature 004 - Comprehensive ThemeParks.wiki Data Collection**: Requires attraction metadata, queue data (all types), show schedules, park schedules, and weather data to be collected and available for analysis

**OTHER DEPENDENCIES:**
- **Feature 002 - Weather Data Collection**: Requires weather observations and forecasts (completed)
- **Historical Wait Time Data**: Requires minimum 90 days of wait time snapshots from existing ride_status_snapshots table
- **School Calendar Data**: Requires school holiday calendars for correlation analysis
- **Computing Resources**: Requires sufficient computation for model training (acceptable for quarterly training runs)
- **Statistical Libraries**: Requires standard statistical/ML libraries for correlation, modeling, validation

### External Factors

- Historical data quality impacts model accuracy (gaps, missing data reduce reliability)
- Park operational changes not reflected in data (new ride, capacity changes) cause prediction errors
- Unexpected events (pandemics, extreme weather, viral trends) create patterns not in training data
- Visitor behavior shifts over time (model drift) require periodic retraining
- Weather forecast accuracy limits prediction horizon (7-14 day forecasts are less reliable)
- School calendar changes (snow days, unexpected breaks) not captured in data reduce holiday predictions
