"""Create data_quality_log table

Revision ID: 004f_data_quality_log
Revises: 004e_storage_metrics
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T011 - Create data_quality_log table for import quality tracking
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004f_data_quality_log'
down_revision = '004e_storage_metrics'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create issue type ENUM (different from existing data_quality_issues)
    log_issue_type_enum = sa.Enum(
        'GAP', 'DUPLICATE', 'INVALID', 'MISSING_FIELD', 'PARSE_ERROR', 'MAPPING_FAILED',
        name='log_issue_type_enum'
    )
    log_issue_type_enum.create(op.get_bind(), checkfirst=True)

    # Create resolution status ENUM
    resolution_status_enum = sa.Enum(
        'OPEN', 'INVESTIGATING', 'RESOLVED', 'WONTFIX',
        name='resolution_status_enum'
    )
    resolution_status_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'data_quality_log',
        sa.Column('log_id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            'import_id',
            sa.String(20),
            nullable=True,
            index=True,
            comment="Associated import (NULL for live collection issues)"
        ),
        sa.Column(
            'issue_type',
            log_issue_type_enum,
            nullable=False,
            comment="Type of quality issue"
        ),
        sa.Column(
            'entity_type',
            sa.String(50),
            nullable=False,
            comment="Entity type: ride, park, snapshot, etc."
        ),
        sa.Column(
            'entity_id',
            sa.Integer(),
            nullable=True,
            comment="Internal entity ID (if applicable)"
        ),
        sa.Column(
            'external_id',
            sa.String(36),
            nullable=True,
            comment="External ID (ThemeParks.wiki UUID)"
        ),
        sa.Column(
            'timestamp_start',
            sa.DateTime(),
            nullable=False,
            comment="Start of affected time range"
        ),
        sa.Column(
            'timestamp_end',
            sa.DateTime(),
            nullable=True,
            comment="End of affected time range (NULL if point-in-time)"
        ),
        sa.Column(
            'description',
            sa.Text(),
            nullable=False,
            comment="Human-readable description of the issue"
        ),
        sa.Column(
            'raw_data',
            sa.JSON(),
            nullable=True,
            comment="Original data that caused the issue (for debugging)"
        ),
        sa.Column(
            'resolution_status',
            resolution_status_enum,
            nullable=False,
            server_default='OPEN'
        ),
        sa.Column(
            'resolution_notes',
            sa.Text(),
            nullable=True,
            comment="Notes about how the issue was resolved"
        ),
        sa.Column(
            'created_at',
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now()
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now()
        ),
        comment="Data quality issues and gaps tracking for import/collection"
    )

    # Create indexes
    op.create_index(
        'idx_quality_log_type',
        'data_quality_log',
        ['issue_type']
    )
    op.create_index(
        'idx_quality_log_status',
        'data_quality_log',
        ['resolution_status']
    )
    op.create_index(
        'idx_quality_log_time',
        'data_quality_log',
        ['timestamp_start', 'timestamp_end']
    )


def downgrade() -> None:
    op.drop_table('data_quality_log')
    sa.Enum(
        'GAP', 'DUPLICATE', 'INVALID', 'MISSING_FIELD', 'PARSE_ERROR', 'MAPPING_FAILED',
        name='log_issue_type_enum'
    ).drop(op.get_bind(), checkfirst=True)
    sa.Enum(
        'OPEN', 'INVESTIGATING', 'RESOLVED', 'WONTFIX',
        name='resolution_status_enum'
    ).drop(op.get_bind(), checkfirst=True)
