from time import sleep

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import create_engine

DB_BASE = "postgresql://postgres@postgres"
DB_NAME = "hubble"
TEMPLATE_DB = f"{DB_NAME}_template"
ALEMBIC_DIR = "/app/alembic"

postgres = create_engine(f"{DB_BASE}/postgres")
with postgres.connect() as connection:
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(
        text(f"DROP DATABASE IF EXISTS {TEMPLATE_DB} WITH (FORCE)")
    )
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(
        text(f"DROP DATABASE IF EXISTS {DB_NAME} WITH (FORCE)")
    )
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(text(f"CREATE DATABASE {TEMPLATE_DB}"))

    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", ALEMBIC_DIR)
    alembic_cfg.set_main_option("sqlalchemy.url", f"{DB_BASE}/{TEMPLATE_DB}")
    command.upgrade(alembic_cfg, "head")

    connection.execution_options(isolation_level="AUTOCOMMIT").execute(
        text(f"CREATE DATABASE {DB_NAME} TEMPLATE {TEMPLATE_DB}")
    )
while True:
    sleep(60)
