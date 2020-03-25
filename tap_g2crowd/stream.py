import requests
from dateutil import parser
import time
from ratelimit import limits
import ratelimit
import singer
import backoff


SESSION = requests.Session()
LOGGER = singer.get_logger()
BASE_URL = "https://data.g2.com"
PAGE_NUMBER = "page%5Bnumber%5D="
PAGE_SIZE = "page%5Bsize%5D="
ENDPOINTS = {
    "companies": "/api/v1/ahoy/companies",
    "remote_events_streams": "/api/v1/ahoy/remote-event-streams",
    "track_prospects": "/api/v1/attribution_tracking/remote-conversions",
    "users": "/api/v1/users",
    "vendors": "/api/v1/vendors",
}
REPLICATION_PATH = {
    "remote_events_streams": ["attributes", "time"],
    "track_prospects": ["attributes", "last_seen", "occurred_at"],
    "users": ["attributes", "updated_at"],
    "vendor": ["attributes", "updated_at"],
}


def get_url(endpoint, page_number, page_size):
    return f"{BASE_URL}{endpoint}?{PAGE_NUMBER}{page_number}&{PAGE_SIZE}{page_size}"


def get_companies(api_key, endpoint):
    page_number = 1
    while True:
        url = get_url(endpoint, page_number, 10)
        companies = call_api(api_key, url)
        if len(companies) == 10:
            # g2crowd companies endpoint pagination doesn't work properly
            # Default data size for each page is ten,
            # but when the data reaches the end, the page return all the data
            # And the pagination doesn't stop
            page_number += 1
            continue
        else:
            replication_value = map(get_replication_value, companies)
            return zip(companies, replication_value)


def get_replication_value(obj: dict, path_to_replication_key=None, default=None):
    if not path_to_replication_key:
        return default
    for path_element in path_to_replication_key:
        obj = obj.get(path_element, None)
        if not obj:
            return default
    return parser.isoparse(obj)


def get_records(api_key, endpoint, tap_stream_id):
    path = REPLICATION_PATH[tap_stream_id]
    page_number = 1
    while True:
        url = get_url(endpoint, page_number, 25)
        records = call_api(api_key, url)
        if records:
            replication_value = map(
                lambda record: get_replication_value(
                    obj=record, path_to_replication_key=path
                ),
                records,
            )
            yield from zip(records, replication_value)
            page_number += 1
        else:
            break


def streams(api_key, tap_stream_id):
    endpoint = ENDPOINTS[tap_stream_id]
    if tap_stream_id == "companies":
        yield from get_companies(api_key, endpoint)
    else:
        yield from get_records(api_key, endpoint, tap_stream_id)


@backoff.on_exception(
    backoff.expo,
    (
        requests.exceptions.RequestException,
        requests.exceptions.HTTPError,
        ratelimit.exception.RateLimitException,
    ),
)
@limits(calls=100, period=1)
def call_api(api_key, url):

    response = SESSION.get(
        url,
        headers={
            "Accept": "application/vnd.api+json",
            "Authorization": f"Token token={api_key}",
        },
    )
    response.raise_for_status()
    response = response.json()
    data = response["data"]
    return data
