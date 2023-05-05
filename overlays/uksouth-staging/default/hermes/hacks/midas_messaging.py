import logging
import uuid

from django.conf import settings
from olympus_messaging import JoinApplication, Message

from api_messaging.message_broker import SendingService

logger = logging.getLogger("messaging")

message_sender = SendingService(
    dsn=settings.RABBIT_DSN,
    log_to=logger,
)


def to_midas(message: Message) -> None:
    if message.metadata["loyalty-plan"] == "iceland-bonus-card":
        print("Iceland Bonus Card detected, implementing hack hold")
        message_sender.send(message.body, message.metadata, f"{settings.MIDAS_QUEUE_NAME}-hold")
    else:
        message_sender.send(message.body, message.metadata, settings.MIDAS_QUEUE_NAME)


def send_midas_join_request(
    channel: str, bink_user_id: int, request_id: int, loyalty_plan: str, account_id: str, encrypted_credentials: str
) -> None:

    message = JoinApplication(
        channel=channel,
        transaction_id=str(uuid.uuid1()),
        bink_user_id=str(bink_user_id),
        request_id=str(request_id),
        loyalty_plan=loyalty_plan,
        account_id=account_id,
        join_data={"encrypted_credentials": encrypted_credentials},
    )

    to_midas(message)
