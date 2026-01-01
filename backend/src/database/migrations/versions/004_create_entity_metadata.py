"""Create entity_metadata table

Revision ID: 004c_entity_metadata
Revises: 004b_import_checkpoints
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T008 - Create entity_metadata table for attraction coordinates and attributes
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004c_entity_metadata'
down_revision = '004b_import_checkpoints'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create indoor/outdoor ENUM
    indoor_outdoor_enum = sa.Enum('INDOOR', 'OUTDOOR', 'HYBRID', name='indoor_outdoor_enum')
    indoor_outdoor_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'entity_metadata',
        sa.Column('metadata_id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            'ride_id',
            sa.Integer(),
            sa.ForeignKey('rides.ride_id', ondelete='CASCADE'),
            nullable=False,
            comment="Internal ride ID"
        ),
        sa.Column(
            'themeparks_wiki_id',
            sa.String(36),
            unique=True,
            nullable=False,
            comment="ThemeParks.wiki entity UUID"
        ),
        sa.Column(
            'entity_name',
            sa.String(255),
            nullable=False,
            comment="Entity name from ThemeParks.wiki"
        ),
        sa.Column(
            'entity_type',
            sa.String(50),
            nullable=False,
            comment="Entity type: ATTRACTION, SHOW, RESTAURANT, etc."
        ),
        sa.Column(
            'latitude',
            sa.Numeric(10, 7),
            nullable=True,
            comment="Latitude coordinate"
        ),
        sa.Column(
            'longitude',
            sa.Numeric(10, 7),
            nullable=True,
            comment="Longitude coordinate"
        ),
        sa.Column(
            'indoor_outdoor',
            indoor_outdoor_enum,
            nullable=True,
            comment="Indoor/outdoor classification"
        ),
        sa.Column(
            'height_min_cm',
            sa.Integer(),
            nullable=True,
            comment="Minimum height requirement in centimeters"
        ),
        sa.Column(
            'height_max_cm',
            sa.Integer(),
            nullable=True,
            comment="Maximum height requirement in centimeters"
        ),
        sa.Column(
            'tags',
            sa.JSON(),
            nullable=True,
            comment="Additional tags from ThemeParks.wiki"
        ),
        sa.Column(
            'last_synced',
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
            comment="When metadata was last synced from ThemeParks.wiki"
        ),
        sa.Column(
            'version',
            sa.Integer(),
            nullable=False,
            default=1,
            server_default='1',
            comment="Version counter for change tracking"
        ),
        comment="Comprehensive attraction metadata from ThemeParks.wiki"
    )

    # Create indexes
    op.create_index(
        'idx_metadata_ride',
        'entity_metadata',
        ['ride_id']
    )
    op.create_index(
        'idx_metadata_type',
        'entity_metadata',
        ['entity_type']
    )
    # Spatial index would require MySQL 8.0+ with POINT column type
    # For now, use composite index for coordinate range queries
    op.create_index(
        'idx_metadata_coords',
        'entity_metadata',
        ['latitude', 'longitude']
    )


def downgrade() -> None:
    op.drop_table('entity_metadata')
    sa.Enum('INDOOR', 'OUTDOOR', 'HYBRID', name='indoor_outdoor_enum').drop(
        op.get_bind(), checkfirst=True
    )
