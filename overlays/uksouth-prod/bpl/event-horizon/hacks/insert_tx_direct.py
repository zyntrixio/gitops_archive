import argparse

from cosmos_message_lib.schemas import ActivitySchema, utc_datetime
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from event_horizon.hubble.db.models import Activity
from event_horizon.hubble.db.models import Base as HubbleModelBase
from event_horizon.hubble.db.session import SyncSessionMaker as HubbleSyncSessionMaker
from event_horizon.hubble.db.session import engine as hubble_sync_engine
from event_horizon.polaris.db.models import AccountHolder, AccountHolderTransactionHistory
from event_horizon.polaris.db.models import Base as PolarisModelBase
from event_horizon.polaris.db.session import engine as polaris_sync_engine

# Parse script args
parser = argparse.ArgumentParser(description="Process cleaned tx data")
parser.add_argument("-d", "--dryrun", action="store_true", help="Dry run script")
args = parser.parse_args()
# Check dryrun mode
dry_run = args.dryrun if args.dryrun is not None else False


# DB config
PolarisModelBase.prepare(polaris_sync_engine, reflect=True)
HubbleModelBase.prepare(hubble_sync_engine, reflect=True)
PolarisSyncSessionMaker = sessionmaker(bind=polaris_sync_engine, future=True, expire_on_commit=False)

# Activity identifiers
TX_IDS_TO_FETCH = (
    "bpl-viator-c4a13f2f52ee0078198ccafa57119ba28b989a70",
    "bpl-viator-44e7cc414e85357e15485fc96ec730938b20ea0e",
    "bpl-viator-7bb7f3bfe22515f73daff27276812e4b0cb3cc90",
    "bpl-viator-620fa02496eb299212b7e82b385ce8070a84b3fd",
    "bpl-viator-b19c4e0aecc80fb3306adaed67498813353bc0cb",
    "bpl-viator-838a8c69ab2203eefd2c64ac5633ef19a9cef7f1",
    "bpl-viator-1361fb5fe825db28debbb66946ee1c3483e6d92b",
    "bpl-viator-cc49c77366ad1ed45c53e52735f6268c2bd47d67",
    "bpl-viator-b2b1be66e5a0240f25d9e1e5d1ca846ff2bb53cf",
    "bpl-viator-16f3c6ea65d0c45b4fa023277e533ebd544414d9",
    "bpl-viator-801c87255f4b08e08effbe91ba78287b544bb898",
    "bpl-viator-991ff572b1f6aecd66f5c5c6bef8cc359dd849aa",
)


class EarnedSchema(BaseModel):
    value: str
    type: str  # noqa: A003


class TransactionHistorySchema(BaseModel):
    transaction_id: str
    datetime: utc_datetime
    amount: str
    amount_currency: str
    location_name: str = Field(..., alias="store_name")
    earned: list[EarnedSchema]


def fetch_activities_for_ids() -> list[Activity]:
    with HubbleSyncSessionMaker() as hubble_db_session:
        stmt = select(Activity).where(Activity.activity_identifier.in_(TX_IDS_TO_FETCH))
        result = hubble_db_session.execute(stmt).scalars().all()
        return result


def insert_tx_history_polairs(payload: TransactionHistorySchema, account_holder_uuid: str) -> bool:
    with PolarisSyncSessionMaker() as polaris_db_session:
        result = polaris_db_session.execute(
            select(AccountHolderTransactionHistory.id).where(
                AccountHolderTransactionHistory.transaction_id == payload.transaction_id
            )
        ).one_or_none()

    if not result:
        if dry_run:
            print(f"Will insert tx history with transaction_id: {payload.transaction_id}")
            return False
        else:
            values_to_insert = payload.dict()
            result = polaris_db_session.execute(
                insert(AccountHolderTransactionHistory)
                .on_conflict_do_nothing()
                .values(
                    account_holder_id=select(AccountHolder.id)
                    .where(AccountHolder.account_holder_uuid == account_holder_uuid)
                    .scalar_subquery(),
                    **values_to_insert,
                )
                .returning(AccountHolderTransactionHistory.id)
            )
            try:
                polaris_db_session.commit()
                return bool(result.rowcount)
            except Exception as ex:
                print(f"Failed to insert tx history with id: {payload.transaction_id}")
                return False
    else:
        print(f"Found matching record in Transaction history table with transaction_id: {payload.transaction_id}")
        return False


def main() -> None:
    activities = fetch_activities_for_ids()

    inserted_count = 0
    for activity in activities:
        validated = ActivitySchema(
            id=activity.id,
            type=activity.type,
            datetime=activity.datetime,
            underlying_datetime=activity.underlying_datetime,
            summary=activity.summary,
            reasons=activity.reasons,
            activity_identifier=activity.activity_identifier,
            user_id=activity.user_id,
            associated_value=activity.associated_value,
            retailer=activity.retailer,
            campaigns=activity.campaigns,
            data=activity.data,
        )
        if validated.type == "TX_IMPORT":
            continue

        account_holder_uuid = validated.user_id
        payload = TransactionHistorySchema(**validated.data)

        inserted = insert_tx_history_polairs(payload, account_holder_uuid)
        if inserted:
            inserted_count += 1

    print(f"Total tx history inserted to polaris: {inserted_count}")


main()
