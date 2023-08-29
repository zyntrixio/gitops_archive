import argparse
import json

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from event_horizon.activity_utils.tasks import sync_send_activity
from event_horizon.hubble.db.models import Activity
from event_horizon.hubble.db.models import Base as HubbleModelBase
from event_horizon.hubble.db.session import SyncSessionMaker as HubbleSyncSessionMaker
from event_horizon.hubble.db.session import engine as hubble_sync_engine
from event_horizon.polaris.db.models import AccountHolderTransactionHistory
from event_horizon.polaris.db.models import Base as PolarisModelBase
from event_horizon.polaris.db.session import engine as polaris_sync_engine

# Parse script args
parser = argparse.ArgumentParser(description="Process cleaned tx data")
parser.add_argument("-f", "--filename", help="Name of the file to process")
parser.add_argument("-d", "--dryrun", action="store_true", help="Dry run script")
args = parser.parse_args()
# Define the input file path
input_file_path = args.filename
# Check dryrun mode
dry_run = args.dryrun if args.dryrun is not None else False

##############################################
# RABBIT CONFIG
TX_IMPORT_ROUTING_KEY: str = "activity.vela.tx.import"
TX_HISTORY_ROUTING_KEY: str = "activity.vela.tx.processed"
##############################################

# DB config
PolarisModelBase.prepare(polaris_sync_engine, reflect=True)
HubbleModelBase.prepare(hubble_sync_engine, reflect=True)
PolarisSyncSessionMaker = sessionmaker(bind=polaris_sync_engine, future=True, expire_on_commit=False)

# Read the JSON file and load data into memory
with open(input_file_path) as json_file:
    data = json.load(json_file)

tx_history_event_count = 0
tx_import_event_count = 0

tx_ids = set()

for event in data:
    transaction_id = event["activity_identifier"]
    # Check polaris and hubble to make sure transaction_id doesn't exist
    with HubbleSyncSessionMaker() as hubble_db_session, PolarisSyncSessionMaker() as polaris_db_session:
        # Query polaris table
        polaris_result = polaris_db_session.execute(
            select(AccountHolderTransactionHistory.id).where(
                AccountHolderTransactionHistory.transaction_id == transaction_id
            )
        ).one_or_none()

        # Query hubble table
        hubble_result = (
            hubble_db_session.execute(
                select(Activity.id).where(
                    Activity.activity_identifier == transaction_id, Activity.type != "REFUND_NOT_RECOUPED"
                )
            )
            .scalars()
            .all()
        )

    if not polaris_result and not hubble_result:
        tx_ids.add(transaction_id)

        match event["type"]:
            case "TX_IMPORT":
                tx_import_event_count += 1
                routing_key = TX_IMPORT_ROUTING_KEY
            case "TX_HISTORY":
                tx_history_event_count += 1
                routing_key = TX_HISTORY_ROUTING_KEY
            case _:
                raise ValueError("Cannot send for this type")

        # Formatting
        event["datetime"] = datetime.strptime(event["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        event["underlying_datetime"] = datetime.strptime(event["underlying_datetime"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )

        if not dry_run:
            sync_send_activity(event, routing_key=routing_key)
        else:
            print(f"Will enqueue event with transaction_id: {transaction_id}")

if not dry_run:
    print(f"tx_ids for events queued: {list(tx_ids)}")
    print(f"No. of TX_IMPORT events queued: {tx_import_event_count}")
    print(f"No. of TX_HISTORY events queued: {tx_history_event_count}")
else:
    print(f"tx_ids for events to be queued: {list(tx_ids)}")
    print(f"No. of TX_IMPORT events to be queued: {tx_import_event_count}")
    print(f"No. of TX_HISTORY events to be queued: {tx_history_event_count}")
