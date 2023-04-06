from time import sleep

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import create_engine

DB_BASE = "postgresql://postgres@postgres"
DB_NAME = "polaris_template"
ALEMBIC_DIR = "/app/alembic"

postgres = create_engine(f"{DB_BASE}/postgres", isolation_level="AUTOCOMMIT")
with postgres.connect() as connection:
    connection.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME} WITH (FORCE)"))
    connection.execute(text(f"CREATE DATABASE {DB_NAME}"))

alembic_cfg = Config()
alembic_cfg.set_main_option("script_location", ALEMBIC_DIR)
alembic_cfg.set_main_option("sqlalchemy.url", f"{DB_BASE}/{DB_NAME}")
command.upgrade(alembic_cfg, "head")

while True:
    sleep(60)
