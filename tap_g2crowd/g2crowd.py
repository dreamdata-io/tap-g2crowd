import singer
from typing import Union, Dict, Optional
from datetime import timedelta, datetime
from dateutil import parser
from tap_g2crowd.stream import Stream
import pytz

LOGGER = singer.get_logger()



class G2Crowd:
    def __init__(self, config: Dict):
        self.config = config
        self.companies_endpoints = []

    def do_sync(
        self, tap_stream_id: str, stream_config: Dict, state: Optional[datetime] = None
    ):
        stream = Stream(self.config["api_key"], self.companies_endpoints)
        bookmark_key = stream_config.get("bookmark_key")
        prev_bookmark = None
        start_date, end_date = self.__get_start_end(
            state=state, bookmark_key=bookmark_key, tap_stream_id=tap_stream_id
        )
        with singer.metrics.record_counter(tap_stream_id) as counter:
            try:
                data = stream.streams(tap_stream_id)

                for record, replication_value in data:

                    if replication_value and (
                        (start_date >= replication_value)
                        or (end_date < replication_value)
                    ):
                        continue
                    if tap_stream_id == "remote_events_streams":# companies endpoints is retrieved according to remote-event-streams
                        self.companies_endpoints.append(
                            stream.get_value(
                                record, ["relationships", "company", "links", "related"]
                            )
                        )

                    singer.write_record(tap_stream_id, record)
                    counter.increment(1)
                    if not replication_value:
                        continue
                    new_bookmark = replication_value
                    if not prev_bookmark:
                        prev_bookmark = new_bookmark

                    if prev_bookmark < new_bookmark:
                        state = self.__advance_bookmark(
                            state=state,
                            bookmark=prev_bookmark,
                            bookmark_key=bookmark_key,
                            tap_stream_id=tap_stream_id,
                        )
                        prev_bookmark = new_bookmark
                return self.__advance_bookmark(
                    state=state,
                    bookmark=prev_bookmark,
                    bookmark_key=bookmark_key,
                    tap_stream_id=tap_stream_id,
                )

            except Exception:
                self.__advance_bookmark(
                    state=state,
                    bookmark=prev_bookmark,
                    bookmark_key=bookmark_key,
                    tap_stream_id=tap_stream_id,
                )
                raise

    def __get_start_end(self, state: dict, bookmark_key: str, tap_stream_id: str):
        end_date = pytz.utc.localize(datetime.utcnow())
        LOGGER.info(f"sync data until: {end_date}")

        config_start_date = self.config.get("start_date")
        if config_start_date:
            config_start_date = parser.isoparse(config_start_date)
        else:
            config_start_date = datetime.utcnow() + timedelta(weeks=4)

        if not state:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        account_record = state["bookmarks"].get(tap_stream_id, None)
        if not account_record:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        current_bookmark = account_record.get(bookmark_key, None)
        if not current_bookmark:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        start_date = parser.isoparse(current_bookmark)
        LOGGER.info(f"using 'start_date' from previous state: {start_date}")
        return start_date, end_date

    def __advance_bookmark(
        self,
        state: dict,
        bookmark: Union[str, datetime, None],
        bookmark_key: str,
        tap_stream_id: str,
    ):
        if not bookmark:
            singer.write_state(state)
            return state

        if isinstance(bookmark, datetime):
            bookmark_datetime = bookmark
        elif isinstance(bookmark, str):
            bookmark_datetime = parser.isoparse(bookmark)
        else:
            raise ValueError(
                f"bookmark is of type {type(bookmark)} but must be either string or datetime"
            )

        state = singer.write_bookmark(
            state, tap_stream_id, bookmark_key, bookmark_datetime.isoformat()
        )
        singer.write_state(state)
        return state
