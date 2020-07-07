import requests
from dateutil import parser
from ratelimit import limits
import ratelimit
import singer
import backoff
import sys
from typing import Dict, Optional, List


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

    def get_records(
        self,
        endpoint: str,
        params: Dict,
        replication_path: List,
    ):
        for record in self.pagination(endpoint, params):
            yield record, parser.isoparse(
                self.get_value(obj=record, path=replication_path)
            )

    def pagination(self, endpoint: str, params: Dict):
        while True:
            records = self.call_api(f"{self.BASE_URL}{endpoint}", params)
            if records:
                yield from records
                params["page[number]"] += 1
            else:
                break

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
        max_tries=5,
    )
    @limits(calls=100, period=1)
    def call_api(self, url: str, params: Optional[Dict] = None):
        response = self.SESSION.get(
            url,
            headers={
                "Accept": "application/vnd.api+json",
                "Authorization": f"Token token={self.api_key}",
            },
            params=params,
        )
        response.raise_for_status()
        response = response.json()
        data = response["data"]
        return data
