import requests
from dateutil import parser
import time
from ratelimit import limits
import ratelimit
import singer
import backoff
import sys


LOGGER = singer.get_logger()


class Stream:
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

    def __init__(self, api_key, tap_stream_id, companies_endpoints):
        self.api_key = api_key
        self.tap_stream_id = tap_stream_id
        self.companies_endpoints = companies_endpoints

    def get_url(self, endpoint, page_number=1, page_size=1):
        return f"{self.BASE_URL}{endpoint}?{self.PAGE_NUMBER}{page_number}&{self.PAGE_SIZE}{page_size}"

    def get_companies(self):
        if not self.companies_endpoints:
            LOGGER.warn(
                f"no remote_events_streams data to get companies data"
            )
            sys.exit(0)
        for company_url in self.companies_endpoints:
            company = self.call_api(company_url)
            yield company, None

    def get_replication_value(
        self, obj: dict, path_to_replication_key=None, default=None
    ):
        if not path_to_replication_key:
            return default
        for path_element in path_to_replication_key:
            obj = obj.get(path_element, None)
            if not obj:
                return default
        return obj

    def get_records(self, endpoint):
        path = self.REPLICATION_PATH[self.tap_stream_id]
        page_number = 1
        while True:
            url = self.get_url(endpoint, page_number, 25)
            records = self.call_api(url)
            if records:
                replication_value = map(
                    lambda record: parser.isoparse(
                        self.get_replication_value(
                            obj=record, path_to_replication_key=path
                        )
                    ),
                    records,
                )
                yield from zip(records, replication_value)
                page_number += 1
            else:
                break

    def streams(self):
        endpoint = self.ENDPOINTS[self.tap_stream_id]
        if self.tap_stream_id == "companies":
            yield from self.get_companies()
        else:
            yield from self.get_records(endpoint)

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
    )
    @limits(calls=100, period=1)
    def call_api(self, url):

        response = self.SESSION.get(
            url,
            headers={
                "Accept": "application/vnd.api+json",
                "Authorization": f"Token token={self.api_key}",
            },
        )
        response.raise_for_status()
        response = response.json()
        data = response["data"]
        return data
