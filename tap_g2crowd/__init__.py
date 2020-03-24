#!/usr/bin/env python3
import os
import sys
import json
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
from tap_g2crowd.g2crowd import G2Crowd

KEY_PROPERTIES = "id"
STREAMS = {
    "companies": {},
    "remote_events_streams": {"valid_replication_keys": ["time"]},
    "track_prospects": {"valid_replication_keys": ["occurred_at"]},
    "users": {"valid_replication_keys": ["updated_at"]},
    "vendors": {"valid_replication_keys": ["updated_at"]},
}
REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover() -> Catalog:
    schemas = load_schemas()
    streams = []

    for tap_stream_id, props in STREAMS.items():
        valid_replication_keys = props.get("valid_replication_keys", [])
        schema = schemas[tap_stream_id]
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=KEY_PROPERTIES,
            valid_replication_keys=valid_replication_keys,
            replication_method="FULL_TABLE"
            if not valid_replication_keys
            else "INCREMENTAL",
        )
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=KEY_PROPERTIES,
                schema=Schema.from_dict(schema),
                metadata=mdata,
            )
        )
    return Catalog(streams)


def sync(catalog, config, state=None):
    for catalog_entry in catalog.streams:
        if not catalog_entry.is_selected():
            continue
        # Loop over streams in catalog
        LOGGER.info(f"syncing {catalog_entry.tap_stream_id}")
        g2crowd = G2Crowd(catalog_entry, config)
        g2crowd.stream(state)
    return


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(catalog, args.config, args.state)


if __name__ == "__main__":
    main()
