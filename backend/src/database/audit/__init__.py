"""
Data Accuracy Audit Framework
=============================

This module provides tools for ensuring absolute accuracy of Theme Park
Hall of Shame rankings. Even a single error could undermine credibility.

Components:
- views.sql: SQL views for auditable calculation paths
- validation_checks.py: Hard validation rules that halt on violations
- anomaly_detector.py: Statistical anomaly detection (Z-scores, sudden changes)
- computation_trace.py: Step-by-step calculation traces for user audits

Usage:
    from database.audit import ValidationChecker, AnomalyDetector

    # Run all validation checks
    checker = ValidationChecker(conn)
    results = checker.run_all_checks(target_date)

    # Check for anomalies
    detector = AnomalyDetector(conn)
    anomalies = detector.detect_anomalies(target_date)

How to Add Validation Rules:
1. Add rule definition to VALIDATION_RULES in validation_checks.py
2. Rule format: {name: {query, max_rows, severity, message}}
3. Severity: CRITICAL (halt), WARNING (flag), INFO (log)
"""

from .validation_checks import ValidationChecker, VALIDATION_RULES
from .anomaly_detector import AnomalyDetector
from .computation_trace import ComputationTracer

__all__ = [
    "ValidationChecker",
    "VALIDATION_RULES",
    "AnomalyDetector",
    "ComputationTracer",
]
