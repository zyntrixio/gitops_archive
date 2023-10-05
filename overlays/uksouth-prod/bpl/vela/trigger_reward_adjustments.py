import logging
from datetime import datetime, timezone
from typing import Optional, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

import typer
from retry_tasks_lib.utils.synchronous import (
    enqueue_many_retry_tasks,
    sync_create_many_tasks,
)
from sqlalchemy import create_engine, text
from sqlalchemy.future import select
from sqlalchemy.pool import NullPool
from vela.core.config import redis_raw, settings
from vela.db.session import SyncSessionMaker as VelaSessionMaker
from vela.models.retailer import Campaign, RetailerRewards

logger = logging.getLogger(__name__)
cli = typer.Typer()


polaris_engine = create_engine(
    urlparse(settings.SQLALCHEMY_DATABASE_URI)._replace(path="/polaris").geturl(),
    pool_pre_ping=True,
    future=True,
    poolclass=NullPool,
)


def fetch_account_holder_ids_from_polaris(
    campaign_slug: str, min_balance: int | None
) -> list[UUID]:
    params = {"campaign_slug": campaign_slug}
    sql = """
        SELECT ah.account_holder_uuid
        FROM account_holder ah
        JOIN account_holder_campaign_balance cb ON ah.id = cb.account_holder_id
        WHERE cb.campaign_slug = :campaign_slug
    """
    if min_balance:
        params["min_balance"] = min_balance
        sql += "\n AND cb.balance >= :min_balance"

    with polaris_engine.connect() as conn:
        account_holders_uuids = cast(list[UUID], conn.scalars(text(sql), params).all())

    logger.info("fetched %d account holders uuids", len(account_holders_uuids))
    return account_holders_uuids


@cli.command(no_args_is_help=True)
def create_and_enqueue_reward_adjustment_tasks(
    campaign_slug: str = typer.Argument(
        ...,
        help="Slug of the campaign for which to trigger rewards check.",
    ),
    min_balance: Optional[int] = typer.Argument(  # noqa: UP007
        None,
        help="Optional minimum balance required to trigger rewards check in pence (100 = 1 pound).",
    ),
) -> None:
    now = datetime.now(tz=timezone.utc)
    account_holders_uuids = fetch_account_holder_ids_from_polaris(
        campaign_slug, min_balance
    )

    with VelaSessionMaker() as db_session:
        retailer_slug = db_session.execute(
            select(RetailerRewards.slug)
            .join(Campaign)
            .where(Campaign.slug == campaign_slug)
        ).scalar_one()

        tasks = sync_create_many_tasks(
            db_session,
            task_type_name=settings.REWARD_ADJUSTMENT_TASK_NAME,
            params_list=[
                {
                    "account_holder_uuid": account_holder_uuid,
                    "retailer_slug": retailer_slug,
                    "processed_transaction_id": "Goal Updated",
                    "campaign_slug": campaign_slug,
                    "adjustment_amount": 0,
                    "pre_allocation_token": uuid4(),
                    "transaction_datetime": now,
                }
                for account_holder_uuid in account_holders_uuids
            ],
        )
        db_session.commit()
        logger.info(
            "created %d %s RetryTasks", len(tasks), settings.REWARD_ADJUSTMENT_TASK_NAME
        )
        enqueue_many_retry_tasks(
            db_session,
            retry_tasks_ids=[task.retry_task_id for task in tasks],
            connection=redis_raw,
        )

    logger.info("Tasks enqueued successfully")


if __name__ == "__main__":
    cli()
