"""Create import_checkpoints table

Revision ID: 004b_import_checkpoints
Revises: 004a_data_source
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T007 - Create import_checkpoints table for resumable historical imports
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004b_import_checkpoints'
down_revision = '004a_data_source'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create import status ENUM
    import_status_enum = sa.Enum(
        'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PAUSED',
        name='import_status_enum'
    )
    import_status_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'import_checkpoints',
        sa.Column('checkpoint_id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            'import_id',
            sa.String(20),
            unique=True,
            nullable=False,
            comment="Public-facing import identifier (e.g., 'imp_abc123')"
        ),
        sa.Column(
            'destination_uuid',
            sa.String(36),
            nullable=False,
            comment="ThemeParks.wiki destination UUID being imported"
        ),
        sa.Column(
            'park_id',
            sa.Integer(),
            sa.ForeignKey('parks.park_id'),
            nullable=True,
            comment="Internal park_id if mapped, NULL if destination-level import"
        ),
        sa.Column(
            'last_processed_date',
            sa.Date(),
            nullable=True,
            comment="Last successfully processed date"
        ),
        sa.Column(
            'last_processed_file',
            sa.String(255),
            nullable=True,
            comment="Last processed S3 file path"
        ),
        sa.Column(
            'records_imported',
            sa.Integer(),
            nullable=False,
            default=0,
            server_default='0',
            comment="Total records successfully imported"
        ),
        sa.Column(
            'errors_encountered',
            sa.Integer(),
            nullable=False,
            default=0,
            server_default='0',
            comment="Total errors encountered during import"
        ),
        sa.Column(
            'status',
            import_status_enum,
            nullable=False,
            server_default='PENDING'
        ),
        sa.Column(
            'started_at',
            sa.DateTime(),
            nullable=True,
            comment="When import was started"
        ),
        sa.Column(
            'completed_at',
            sa.DateTime(),
            nullable=True,
            comment="When import completed (success or failure)"
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
        comment="Tracks historical import progress for resumable imports"
    )

    # Create indexes
    op.create_index(
        'idx_import_status',
        'import_checkpoints',
        ['status']
    )
    op.create_index(
        'idx_import_destination',
        'import_checkpoints',
        ['destination_uuid']
    )


def downgrade() -> None:
    op.drop_table('import_checkpoints')
    sa.Enum(
        'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PAUSED',
        name='import_status_enum'
    ).drop(op.get_bind(), checkfirst=True)
