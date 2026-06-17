"""init schema

Revision ID: 0001_init_schema
Revises: 
Create Date: 2026-06-16 00:00:00.000000
"""
from alembic import op
import os

# revision identifiers, used by Alembic.
revision = '0001_init_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # execute the raw SQL schema (relative path)
    base = os.path.dirname(os.path.dirname(__file__))
    sql_path = os.path.normpath(os.path.join(base, '..', 'sql', 'schema.sql'))
    if not os.path.isabs(sql_path):
        sql_path = os.path.abspath(sql_path)

    with open(sql_path, 'r') as f:
        sql = f.read()
    op.execute(sql)


def downgrade() -> None:
    op.execute("""
    DROP TABLE IF EXISTS pipeline_events;
    DROP TABLE IF EXISTS pipelines;
    """)
