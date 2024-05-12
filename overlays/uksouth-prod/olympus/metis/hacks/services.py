import asyncio
import os
import time
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, cast

import httpx
import requests
from loguru import logger
from metis.agents.amex import Amex
from metis.agents.exceptions import OAuthError
from metis.agents.mastercard import MasterCard
from metis.agents.visa_offers import Visa, VOPResultStatus
from metis.enums import RetryTypes
from metis.hermes import get_provider_status_mappings, put_account_status
from metis.prometheus.metrics import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    mastercard_reactivate_counter,
    mastercard_reactivate_response_time_histogram,
    payment_card_enrolment_counter,
    payment_card_enrolment_reponse_time_histogram,
    push_metrics,
    unenrolment_counter,
    unenrolment_response_time_histogram,
)
from metis.settings import settings
from metis.vault import Secrets, fetch_and_set_secret, get_azure_client
from requests import RequestException

if TYPE_CHECKING:
    from typing import TypedDict

    class ActiveAgentsType(TypedDict):
        mastercard: type[MasterCard]
        amex: type[Amex]
        visa: type[Visa]


ACTIVE_AGENTS: "ActiveAgentsType" = {
    "mastercard": MasterCard,
    "amex": Amex,
    "visa": Visa,
}

pid = os.getpid()
XML_HEADER = {"Content-Type": "application/xml"}


def push_mastercard_reactivate_metrics(
    response: dict, card_info: dict, request_time_taken: float
) -> None:
    if card_info["partner_slug"] != "mastercard":
        return

    mastercard_reactivate_response_time_histogram.labels(
        status=response["status_code"]
    ).observe(request_time_taken)

    if response["status_code"] == 200:
        mastercard_reactivate_counter.labels(status=STATUS_SUCCESS).inc()
    else:
        mastercard_reactivate_counter.labels(status=STATUS_FAILED).inc()

    push_metrics(pid)


def push_unenrol_metrics_non_vop(
    response: dict, card_info: dict, request_time_taken: float
) -> None:
    unenrolment_response_time_histogram.labels(
        provider=card_info["partner_slug"], status=response["status_code"]
    ).observe(request_time_taken)

    if response["status_code"] == 200:
        unenrolment_counter.labels(
            provider=card_info["partner_slug"], status=STATUS_SUCCESS
        ).inc()
    else:
        unenrolment_counter.labels(
            provider=card_info["partner_slug"], status=STATUS_FAILED
        ).inc()

    push_metrics(pid)


def get_spreedly_url(partner_slug: str | None) -> str:
    if (
        partner_slug == "visa"
        and settings.VOP_SPREEDLY_BASE_URL
        and not settings.STUBBED_VOP_URL
    ):
        return settings.VOP_SPREEDLY_BASE_URL
    return settings.SPREEDLY_BASE_URL


def refresh_oauth_credentials() -> None:
    if settings.AZURE_VAULT_URL:
        secret_defs = ["spreedly_oauth_password", "spreedly_oauth_username"]

        client = get_azure_client()

        for secret_name in secret_defs:
            try:
                secret_def = Secrets.SECRETS_DEF[secret_name]
                fetch_and_set_secret(client, secret_name, secret_def)
                logger.info("{} refreshed from Vault.", secret_name)
            except Exception as e:
                logger.error(
                    "Failed to get {} from Vault. Exception: {}", secret_name, e
                )

    else:
        logger.error(
            "Vault retry attempt due to Oauth error when AZURE_VAULT_URL not set. Have you set the"
            " SPREEDLY_BASE_URL to your local Pelops?"
        )


async def async_send_request(  # noqa: PLR0913
    method: str,
    url: str,
    headers: dict,
    request_data: dict | str | None = None,
    log_response: bool = True,
    timeout: tuple = (5, 10),
) -> httpx.Response:
    logger.info("{} Spreedly Request to URL: {}", method, url)
    params = {
        "method": method,
        "url": url,
        "headers": headers,
        "timeout": timeout,
    }

    if request_data:
        params["data"] = request_data

    resp = await _async_send_retry_spreedly_request(
        **params,  # type: ignore [arg-type]
        auth=(Secrets.spreedly_oauth_username, Secrets.spreedly_oauth_password),
    )
    if log_response:
        try:
            logger.info("Spreedly {} status code: {}", method, resp.status_code)
            logger.debug("Response content:\n{resp.text}")
        except AttributeError as e:
            logger.info(
                "Spreedly {} to URL: {} failed response object error {}", method, url, e
            )

    return resp


async def _async_send_retry_spreedly_request(  # noqa: PLR0913
    method: Literal["GET", "DELETE", "POST", "PUT"],
    url: str,
    headers: dict[str, str],
    timeout: tuple[float, float],
    data: str | None = None,
    auth: tuple[str, str] | None = None,
    cert: tuple[str, str] | None = None,
) -> httpx.Response:
    attempts = 0
    get_auth_attempts = 0

    while attempts < 4:
        attempts += 1
        try:
            async with httpx.AsyncClient(**({"cert": cert} if cert else {})) as client:  # type: ignore [arg-type]
                resp = await client.request(
                    method=method,
                    url=url,
                    data=data,  # type: ignore [arg-type]
                    headers=headers,
                    timeout=timeout,  # type: ignore [arg-type]
                    auth=auth,
                )

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            retry = True
            logger.error(
                "Spreedly {}, url:{}, Retryable exception {} attempt {}",
                method,
                url,
                e,
                attempts,
            )

        else:
            if resp.status_code == 401:
                logger.info(
                    "Spreedly {} status code: {}, reloading oauth password from Vault",
                    method,
                    resp.status_code,
                )
                refresh_oauth_credentials()
                get_auth_attempts += 1
                if get_auth_attempts > 3:
                    await asyncio.sleep(2**get_auth_attempts - 2)
                if get_auth_attempts > 10:
                    break
                attempts = 0
                retry = True
            elif resp.status_code in (500, 501, 502, 503, 504, 492):
                logger.error(
                    "Spreedly {}, url:{}, status code: {}, Retryable error attempt {}",
                    method,
                    url,
                    resp.status_code,
                    attempts,
                )
                retry = True
            else:
                retry = False
        if retry:
            await asyncio.sleep(
                3**attempts - 1
            )  # 4 attempts at 2s, 8s, 26s, 63s or 0s if if oauth error

        else:
            break

    return resp


def send_request(  # noqa: PLR0913
    method: str,
    url: str,
    headers: dict,
    request_data: dict | str | None = None,
    log_response: bool = True,
    timeout: tuple[int, int] = (5, 10),
) -> requests.Response:
    logger.info("{} Spreedly Request to URL: {}", method, url)
    params = {"method": method, "url": url, "headers": headers, "timeout": timeout}
    if request_data:
        params["data"] = request_data

    resp = _send_retry_spreedly_request(
        **params,  # type: ignore [arg-type]
        auth=(Secrets.spreedly_oauth_username, Secrets.spreedly_oauth_password),
    )
    if log_response:
        try:
            logger.info("Spreedly {} status code: {}", method, resp.status_code)
            logger.debug("Response content:\n{}", resp.text)
        except AttributeError as e:
            logger.info(
                "Spreedly {} to URL: {} failed response object error {}", method, url, e
            )

    return resp


def _send_retry_spreedly_request(  # noqa: PLR0913
    method: Literal["GET", "DELETE", "POST", "PUT"],
    url: str,
    headers: dict[str, str],
    timeout: tuple[int, int],
    data: str | None = None,
    auth: tuple[str, str] | None = None,
    cert: tuple[str, str] | None = None,
) -> requests.Response:
    attempts = 0
    get_auth_attempts = 0
    resp = None
    while attempts < 4:
        attempts += 1

        try:
            resp = requests.request(
                method=method,
                url=url,
                data=data,
                headers=headers,
                timeout=timeout,
                auth=auth,
                cert=cert,
            )
        except (requests.Timeout, ConnectionError) as e:
            retry = True
            resp = None
            logger.error(
                "Spreedly {}, url:{}, Retryable exception {} attempt {}",
                method,
                url,
                e,
                attempts,
            )
        else:
            if resp.status_code == 401:
                logger.info(
                    "Spreedly {} status code: {}, reloading oauth password from Vault",
                    method,
                    resp.status_code,
                )
                refresh_oauth_credentials()
                get_auth_attempts += 1
                if get_auth_attempts > 3:
                    time.sleep(2**get_auth_attempts - 2)
                if get_auth_attempts > 10:
                    break
                attempts = 0
                retry = True
            elif resp.status_code in (500, 501, 502, 503, 504, 492):
                logger.error(
                    "Spreedly {}, url:{}, status code: {}, Retryable error attempt {}",
                    method,
                    url,
                    resp.status_code,
                    attempts,
                )
                retry = True
            else:
                retry = False
        if retry:
            time.sleep(
                3**attempts - 1
            )  # 4 attempts at 2s, 8s, 26s, 63s or 0s if if oauth error

        else:
            break

    if resp is None:
        raise ValueError(f"Failed {method} {url} request.")

    return resp


async def create_receiver(hostname: str, receiver_type: str) -> httpx.Response:
    """
    Creates a receiver on the Spreedly environment.
    This is a single call for each Payment card endpoint, Eg MasterCard, Visa and Amex = 3 receivers created.
    This generates a token which LA would store and use for sending credit card details, without the PAN, to
    the payment provider endsite. This creates the proxy service, Spreedly use this to attach the PAN.
    """
    url = f"{settings.SPREEDLY_BASE_URL}/receivers.xml"
    xml_data = f"<receiver><receiver_type>{receiver_type}</receiver_type><hostnames>{hostname}</hostnames></receiver>"
    return await async_send_request(
        "POST", url, XML_HEADER, xml_data, log_response=False
    )


async def create_prod_receiver(receiver_type: str) -> httpx.Response:
    """
    Creates a receiver on the Spreedly environment.
    This is a single call for each Payment card endpoint, Eg MasterCard, Visa and Amex = 3 receivers created.
    This generates a token which LA would store and use for sending credit card details, without the PAN, to
    the payment provider endsite. This creates the proxy service, Spreedly use this to attach the PAN.
    """
    url = f"{settings.SPREEDLY_BASE_URL}/receivers.xml"
    xml_data = f"<receiver><receiver_type>{receiver_type}</receiver_type></receiver>"
    return await async_send_request(
        "POST", url, XML_HEADER, xml_data, log_response=False
    )


def create_sftp_receiver(sftp_details: dict) -> requests.Response:
    """
    Creates a receiver on the Spreedly environment.
    This is a single call to create a receiver for an SFTP process.
    """
    url = f"{settings.SPREEDLY_BASE_URL}/receivers.xml"
    xml_data = (
        "<receiver>"
        "  <receiver_type>{receiver_type}</receiver_type>"
        "  <hostnames>{hostnames}</hostnames>"
        "  <protocol>"
        "    <user>{username}</user>"
        "    <password>{password}</password>"
        "  </protocol>"
        "</receiver>"
    ).format(**sftp_details)
    return send_request("POST", url, XML_HEADER, xml_data, log_response=False)


def get_hermes_data(resp: dict, card_id: int) -> dict:
    hermes_data = {"card_id": card_id, "response_action": "Add"}

    if resp.get("response_state"):
        hermes_data["response_state"] = resp["response_state"]

    other_data = resp.get("other_data", {})
    if other_data.get("agent_card_uid"):
        hermes_data["agent_card_uid"] = other_data["agent_card_uid"]

    if resp.get("status_code"):
        hermes_data["response_status_code"] = resp["status_code"]

    if resp.get("agent_status_code"):
        hermes_data["response_status"] = resp["agent_status_code"]

    if resp.get("message"):
        hermes_data["response_message"] = resp["message"]

    return hermes_data


def add_card(card_info: dict) -> dict | None:
    """
    Once the receiver has been created and token sent back, we can pass in card details, without PAN.
    Receiver_tokens kept in settings.py.
    """
    logger.info("Start Add card for {}", card_info["partner_slug"])

    agent_instance = get_agent(card_info["partner_slug"])
    header = agent_instance.header
    url = f"{get_spreedly_url(card_info['partner_slug'])}/receivers/{agent_instance.receiver_token()}"

    logger.info("Create request data {}", card_info)
    try:
        request_data = agent_instance.add_card_body(card_info)
    except OAuthError:
        # TODO: get this from gaia
        put_account_status(5, card_id=card_info["id"])
        return None
    logger.info("POST URL {}, header: {} *-* {}", url, header, request_data)

    request_start_time = perf_counter()
    req_resp = send_request("POST", url, header, request_data)
    request_time_taken = perf_counter() - request_start_time

    # get the status mapping for this provider from hermes.
    status_mapping = get_provider_status_mappings(card_info["partner_slug"])

    try:
        resp = agent_instance.response_handler(req_resp, "Add", status_mapping)
    except AttributeError:
        resp = {"status_code": 504, "message": "Bad or no response from Spreedly"}

    # Set card_payment status in hermes using 'id' HERMES_URL
    if resp["status_code"] == 200:
        logger.info("Card added successfully, calling Hermes to activate card.")
        card_status_code = 1
        payment_card_enrolment_counter.labels(
            provider=card_info["partner_slug"],
            status=STATUS_SUCCESS,
        ).inc()
    else:
        logger.info("Card add unsuccessful, calling Hermes to set card status.")
        card_status_code = resp.get("bink_status", 0)  # Defaults to pending
        payment_card_enrolment_counter.labels(
            provider=card_info["partner_slug"],
            status=STATUS_FAILED,
        ).inc()

    hermes_data = get_hermes_data(resp, card_info["id"])
    # Ensure that Spreedly 422/408 responses get retried WAL-2992/WAL-3118
    if req_resp.status_code in (422, 408):
        hermes_data["response_state"] = "Retry"

    if card_info.get("retry_id"):
        hermes_data["retry_id"] = card_info["retry_id"]

    reply = put_account_status(card_status_code, **hermes_data)

    logger.info(
        f"Sent add request to hermes status {reply.status_code}: data "
        f'{" ".join([":".join([x, str(y)]) for x, y in hermes_data.items()])}'
    )

    payment_card_enrolment_reponse_time_histogram.labels(
        provider=card_info["partner_slug"], status=resp["status_code"]
    ).observe(request_time_taken)

    push_metrics(pid)

    # Return response effect as in task but useful for test cases
    return resp


def hermes_unenroll_call_back(  # noqa: PLR0913
    card_info: dict,
    action_name: str,
    deactivated_list: list,
    deactivate_errors: dict,
    response_state: str,
    status_code: int | None,
    agent_status_code: str,
    agent_message: str,
    _: Any,
    *,
    retry_type: RetryTypes,
) -> set:
    # Set card_payment status in hermes using 'id' HERMES_URL
    if status_code != 201:
        logger.info(
            "Error in unenrol call back to Hermes VOP Card id: {} {} unsuccessful.  Response state {} {}, {}, {}",
            card_info["id"],
            action_name,
            response_state,
            status_code,
            agent_status_code,
            agent_message,
        )
    hermes_status_data = {
        "card_id": card_info["id"],
        "response_state": response_state,
        "response_status": agent_status_code,
        "response_message": agent_message,
        "response_action": action_name,
        "deactivated_list": deactivated_list,
        "deactivate_errors": deactivate_errors,
        "retry_type": retry_type.value,
    }
    if card_info.get("retry_id"):
        hermes_status_data["retry_id"] = card_info["retry_id"]

    put_account_status(None, **hermes_status_data)

    return {response_state, status_code}


def _remove_visa_card(
    card_info: dict, action_name: str, retry_type: RetryTypes
) -> dict | None:
    # Note the other agents call Spreedly to Unenrol. This is incorrect as Spreedly should not
    # be used as a Proxy to pass unmodified messages to the Agent. The use in add/enrol is an
    # example of correct because Spreedly inserts the PAN when forwarding our message to the Agent.
    # Note there is no longer any requirement to redact the card with with Spreedly so only VOP
    # needs to be called to unenrol a card.

    # Currenly only VOP will need to deactivate first - it would do no harm on upgrading for all accounts to look to
    # see if there are activations but we will leave this until Metis has a common unenroll/delete code again

    # If there are activations in the list we must make sure they are deactivated first before unenrolling
    # It is probably better not to unenroll if any de-activations fail.  That way if a card with same PAN as a
    # deleted card is added it will not go active and pick up old activations (VOP retains this and re-links it!)
    # We will retry this call until all de-activations are done then unenrol.  We call back after each deactivation
    # so that if we retry only the remaining activations will be sent to this service

    agent_instance = Visa()
    activations = card_info.get("activations")
    deactivated_list = []
    deactivate_errors = {}
    if activations:
        all_deactivated = True
        for activation_index, deactivation_card_info in activations.items():
            logger.info("VOP Metis Unenrol Request - deactivating {}", activation_index)
            deactivation_card_info["payment_token"] = card_info["payment_token"]
            deactivation_card_info["id"] = card_info["id"]
            response_status, status_code, agent_response_code, agent_message, _ = (
                agent_instance.deactivate_card(deactivation_card_info)
            )
            if response_status == VOPResultStatus.SUCCESS.value:
                deactivated_list.append(activation_index)
            else:
                deactivate_errors[activation_index] = {
                    "response_status": response_status,
                    "agent_response_code": agent_response_code,
                    "agent_response_message": agent_message,
                }
                if response_status == VOPResultStatus.RETRY.value:
                    all_deactivated = False
                    # Only if you can retry the deactivation will we allow it to block the unenroll
                elif response_status == VOPResultStatus.FAILED.value:
                    logger.error(
                        f"VOP Metis Unenrol Request for {card_info['id']}"
                        f"- permanent deactivation fail {activation_index}"
                    )
        if not all_deactivated:
            message = "Cannot unenrol some Activations still active and can be retried"
            logger.info("VOP Unenroll fail for {} {}", card_info["id"], message)

            status_code, response_state = hermes_unenroll_call_back(
                card_info,
                action_name,
                deactivated_list,
                deactivate_errors,
                VOPResultStatus.RETRY.value,
                None,
                "",
                message,
                "",
                retry_type=retry_type,
            )
            return {"response_status": response_state, "status_code": status_code}

    # Do hermes call back of unenroll now that there are no outstanding activations
    response_state, status_code = hermes_unenroll_call_back(
        card_info,
        action_name,
        deactivated_list,
        deactivate_errors,
        *agent_instance.un_enroll(card_info, action_name, pid),
        retry_type=retry_type,
    )

    # put_account_status sends a async response back to Hermes.
    # The return values below are not functional as this runs in a celery task.
    # However, they have been kept for compatibility with other agents and to assist testing
    return {"response_status": response_state, "status_code": status_code}


def remove_card(
    card_info: dict, retry_type: RetryTypes = RetryTypes.REMOVE
) -> dict | None:
    logger.info("Start Remove card for {}", card_info["partner_slug"])
    action_name = "Delete"

    if card_info["partner_slug"] == "visa":
        return _remove_visa_card(card_info, action_name, retry_type=retry_type)

    agent_instance = cast(Amex | MasterCard, get_agent(card_info["partner_slug"]))
    header = agent_instance.header
    # Older call used with Agents prior to VOP which proxy through Spreedly
    url = f"{settings.SPREEDLY_BASE_URL}/receivers/{agent_instance.receiver_token()}"

    try:
        request_data = agent_instance.remove_card_body(card_info)
    except OAuthError:
        # TODO: get this from gaia
        put_account_status(5, card_id=card_info["id"], retry_type=retry_type.value)
        return None

    request_start_time = perf_counter()
    req_resp = send_request("POST", url, header, request_data)
    request_time_taken = perf_counter() - request_start_time

    # get the status mapping for this provider from hermes.
    status_mapping = get_provider_status_mappings(card_info["partner_slug"])
    resp = agent_instance.response_handler(req_resp, action_name, status_mapping)

    # Push unenrol metrics for amex and mastercard
    push_unenrol_metrics_non_vop(resp, card_info, request_time_taken)

    # @todo View this when looking at Metis re-design
    # This response does nothing as it is in an celery task.  No message is returned to Hermes.
    # getting status mapping is wrong as it is not returned nor would it be used by Hermes.

    return resp


def reactivate_card(card_info: dict) -> dict:
    logger.info("Start reactivate card for {}", card_info["partner_slug"])
    if card_info["partner_slug"] != "mastercard":
        raise ValueError("Only MasterCard supports reactivation.")

    agent_instance = MasterCard()

    header = agent_instance.header
    url = f"{get_spreedly_url(card_info['partner_slug'])}/receivers/{agent_instance.receiver_token()}"
    request_data = agent_instance.reactivate_card_body(card_info)

    request_start_time = perf_counter()
    req_resp = send_request("POST", url, header, request_data)
    request_total_time = perf_counter() - request_start_time

    # get the status mapping for this provider from hermes.
    status_mapping = get_provider_status_mappings(card_info["partner_slug"])

    resp = agent_instance.response_handler(req_resp, "Reactivate", status_mapping)
    # Set card_payment status in hermes using 'id' HERMES_URL
    if resp["status_code"] == 200:
        logger.info("Card added successfully, calling Hermes to activate card.")
        # TODO: get this from gaia
        card_status_code = 1
    else:
        logger.info("Card add unsuccessful, calling Hermes to set card status.")
        card_status_code = resp["bink_status"]
    put_account_status(card_status_code, card_id=card_info["id"])
    push_mastercard_reactivate_metrics(resp, card_info, request_total_time)

    return resp


def get_agent(
    partner_slug: Literal["amex", "mastercard", "visa"],
) -> Amex | MasterCard | Visa:
    agent_class = ACTIVE_AGENTS[partner_slug]
    return agent_class()


async def retain_payment_method_token(
    payment_method_token: str, partner_slug: str | None = None
) -> httpx.Response:
    url = f"{get_spreedly_url(partner_slug)}/payment_methods/{payment_method_token}/retain.json"
    return await async_send_request("PUT", url, {"Content-Type": "application/json"})


def redact_card(card_info: dict) -> None:
    logger.info("Start redact for card {}", card_info["id"])
    try:
        redact_resp = send_request(
            method="PUT",
            url=f"{settings.SPREEDLY_BASE_URL}/payment_methods/{card_info['payment_token']}/redact.json",
            headers={"Content-Type": "application/json"},
        )
    except RequestException:
        # something went wrong, send retry to hermes
        put_account_status(
            5,
            card_info["id"],
            response_action=card_info["action_code"],
            response_state=VOPResultStatus.RETRY.value,
            retry_type=RetryTypes.REDACT.value,
        )

    else:
        if redact_resp.status_code == 404:
            # Payment Account not found, nothing to redact.
            return

        if (
            (200 <= redact_resp.status_code < 300)
            and (resp_json := redact_resp.json())
            and (
                resp_json["transaction"]["succeeded"]
                or resp_json["transaction"]["payment_method"]["storage_state"]
                == "redacted"
            )
        ):
            # Redacted successully.
            return

        # something else went wrong, send retry to hermes
        put_account_status(
            6,
            card_info["id"],
            response_action=card_info["action_code"],
            response_state=VOPResultStatus.RETRY.value,
            retry_type=RetryTypes.REDACT.value,
        )
