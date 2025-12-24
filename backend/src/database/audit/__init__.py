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
- aggregate_verification.py: Verify aggregates match raw snapshot calculations

Usage:
    from database.audit import ValidationChecker, AnomalyDetector

    # Run all validation checks
    checker = ValidationChecker(conn)
    results = checker.run_all_checks(target_date)

    # Check for anomalies
    detector = AnomalyDetector(conn)
    anomalies = detector.detect_anomalies(target_date)

How to Add Validation Rules:
1. Add rule metadata to VALIDATION_RULES_METADATA in validation_checks.py
2. Add corresponding _query_{name} method to ValidationChecker class
3. Rule format: {name: {max_rows, severity, message}}
4. Severity: CRITICAL (halt), WARNING (flag), INFO (log)
"""

from .validation_checks import ValidationChecker, VALIDATION_RULES_METADATA
from .anomaly_detector import AnomalyDetector
from .computation_trace import ComputationTracer
from .aggregate_verification import AggregateVerifier, AggregateAuditResult, AuditSummary

__all__ = [
    "ValidationChecker",
    "VALIDATION_RULES_METADATA",
    "AnomalyDetector",
    "ComputationTracer",
    "AggregateVerifier",
    "AggregateAuditResult",
    "AuditSummary",
]
