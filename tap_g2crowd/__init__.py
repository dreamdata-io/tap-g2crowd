#!/usr/bin/env python3
import singer
from singer import utils
from typing import Optional, Dict
from tap_g2crowd.g2crowd import G2Crowd


STREAMS = {
    "track_prospects": {"bookmark_key": "occurred_at"},
    "remote_events_streams": {"bookmark_key": "time"},
    "companies": {},
}
REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]
LOGGER = singer.get_logger()


def sync(config: Dict, state: Optional[Dict] = None):
    g2crowd = G2Crowd(config)

    for tap_stream_id, stream_config in STREAMS.items():

        LOGGER.info(f"syncing {tap_stream_id}")
        g2crowd.do_sync(
            state=state, tap_stream_id=tap_stream_id, stream_config=stream_config
        )


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    sync(args.config, args.state)


if __name__ == "__main__":
    main()
