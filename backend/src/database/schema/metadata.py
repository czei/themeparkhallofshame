"""
SQLAlchemy MetaData Instance
============================

Shared MetaData object for all table definitions.

All Table objects should use this metadata instance to enable:
- Schema reflection
- DDL generation
- Cross-table relationships
"""

from sqlalchemy import MetaData

# Shared metadata instance for all tables
# Naming convention for constraints (helps with migrations)
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)
