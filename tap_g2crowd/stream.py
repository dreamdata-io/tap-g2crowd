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
    BASE_URL = "https://data.g2.com"

    def __init__(self, api_key: str, companies_endpoints: List):
        self.SESSION = requests.Session()
        self.api_key = api_key
        self.companies_endpoints = companies_endpoints

    def streams(self, tap_stream_id: str):
        if tap_stream_id == "track_prospects":
            yield from self.get_track_prospects()
        elif tap_stream_id == "remote_events_streams":
            yield from self.get_remote_events()
        elif tap_stream_id == "companies":
            yield from self.get_companies()
        else:
            raise NotImplementedError(f"unknown stream_id: {tap_stream_id}")

    def get_track_prospects(self):
        endpoint = "/api/v1/attribution_tracking/remote-conversions"
        replication_path = ["attributes", "last_seen", "occurred_at"]
        params = {"page[number]": 1, "page[size]": 100}
        yield from self.get_records(
            endpoint=endpoint, params=params, replication_path=replication_path
        )

    def get_remote_events(self):
        endpoint = "/api/v1/ahoy/remote-event-streams"
        replication_path = ["attributes", "time"]
        params = {"page[number]": 1, "page[size]": 25}
        yield from self.get_records(
            endpoint=endpoint,
            params=params,
            replication_path=replication_path,
        )

    def get_companies(self):
        # companies data is retrieved according to remote-event-streams
        if not self.companies_endpoints:
            LOGGER.warn(f"no remote_events_streams data to get companies data")
            return None, None
        for company_url in self.companies_endpoints:
            company = self.call_api(company_url)
            yield company, None

    def get_value(self, obj: dict, path: Optional[List] = None, default=None):
        if not path:
            return default
        for path_element in path:
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
