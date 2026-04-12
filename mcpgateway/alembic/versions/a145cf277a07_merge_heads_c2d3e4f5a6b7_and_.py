"""merge_heads_c2d3e4f5a6b7_and_d80ddfa65ddb

Revision ID: a145cf277a07
Revises: c2d3e4f5a6b7, d80ddfa65ddb
Create Date: 2026-04-10 17:30:34.370402

"""

# Standard
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "a145cf277a07"
down_revision: Union[str, Sequence[str], None] = ("c2d3e4f5a6b7", "d80ddfa65ddb")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""


def downgrade() -> None:
    """Downgrade schema."""
