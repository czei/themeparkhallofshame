#!/usr/bin/env python3
"""
Report ThemeParks.wiki fields we are NOT storing for Disney parks/rides.

This script fetches Disney destinations, inspects:
  - /entity/{id}
  - /entity/{id}/children
  - /entity/{id}/live
  - /entity/{id}/schedule
and compares observed fields against a stored-field allowlist.

Usage:
  python -m scripts.report_themeparks_wiki_gaps
  python -m scripts.report_themeparks_wiki_gaps --destination-filter disney --max-parks 5
"""

import sys
import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from collector.themeparks_wiki_client import get_themeparks_wiki_client


STORED_FIELDS = {
    "entity": {
        "id",
        "name",
        "timezone",
        "location.latitude",
        "location.longitude",
    },
    "children": {
        "children[].id",
        "children[].name",
        "children[].entityType",
    },
    "live": {
        "liveData[].id",
        "liveData[].name",
        "liveData[].entityType",
        "liveData[].status",
        "liveData[].lastUpdated",
        "liveData[].queue.STANDBY.waitTime",
    },
    "schedule": {
        "schedule[].date",
        "schedule[].openingTime",
        "schedule[].closingTime",
        "schedule[].type",
    },
}


def flatten_keys(value: Any, prefix: str = "") -> Set[str]:
    """
    Flatten nested dict/list keys into dotted paths.

    Example:
      {"a": {"b": 1}, "c": [{"d": 2}]} -> {"a", "a.b", "c[]", "c[].d"}
    """
    keys: Set[str] = set()

    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else key
            keys.add(path)
            keys.update(flatten_keys(child, path))
        return keys

    if isinstance(value, list):
        list_prefix = f"{prefix}[]" if prefix else "[]"
        keys.add(list_prefix)
        for item in value:
            keys.update(flatten_keys(item, list_prefix))
        return keys

    return keys


def filter_disney_destinations(destinations: Iterable[Dict[str, Any]], keyword: str) -> List[Dict[str, Any]]:
    keyword = keyword.lower()
    filtered = []
    for dest in destinations:
        name = (dest.get("name") or "").lower()
        slug = (dest.get("slug") or "").lower()
        if keyword in name or keyword in slug:
            filtered.append(dest)
    return filtered


def gather_keys_for_parks(
    client,
    parks: Iterable[Dict[str, Any]],
    max_parks: int,
) -> Dict[str, Set[str]]:
    observed = {
        "entity": set(),
        "children": set(),
        "live": set(),
        "schedule": set(),
    }

    for idx, park in enumerate(parks):
        if max_parks and idx >= max_parks:
            break

        park_id = park.get("id")
        park_name = park.get("name", "Unknown Park")
        if not park_id:
            logger.warning(f"Skipping park without id: {park_name}")
            continue

        try:
            entity_doc = client.get_entity(park_id)
            observed["entity"].update(flatten_keys(entity_doc))
        except Exception as exc:
            logger.warning(f"Failed to fetch entity document for {park_name}: {exc}")

        try:
            children_doc = client.get_entity_children(park_id)
            observed["children"].update(flatten_keys({"children": children_doc}))
        except Exception as exc:
            logger.warning(f"Failed to fetch children for {park_name}: {exc}")

        try:
            live_doc = client.get_entity_live(park_id)
            observed["live"].update(flatten_keys(live_doc))
        except Exception as exc:
            logger.warning(f"Failed to fetch live data for {park_name}: {exc}")

        try:
            schedule_doc = client.get_entity_schedule(park_id)
            observed["schedule"].update(flatten_keys(schedule_doc))
        except Exception as exc:
            logger.warning(f"Failed to fetch schedule for {park_name}: {exc}")

    return observed


def compute_missing(observed: Set[str], stored: Set[str]) -> List[str]:
    return sorted(field for field in observed if field not in stored)


def format_report(observed: Dict[str, Set[str]], stats: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("ThemeParks.wiki Field Gap Report (Disney)")
    lines.append(f"Destinations scanned: {stats['destinations_scanned']}")
    lines.append(f"Parks scanned: {stats['parks_scanned']}")
    lines.append("")

    for section in ("entity", "children", "live", "schedule"):
        missing = compute_missing(observed[section], STORED_FIELDS[section])
        lines.append(f"[{section}] missing fields ({len(missing)}):")
        for field in missing:
            lines.append(f"  - {field}")
        lines.append("")

    return "\n".join(lines).rstrip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Report ThemeParks.wiki fields we do not store.")
    parser.add_argument("--destination-filter", default="disney", help="Destination name/slug filter (default: disney)")
    parser.add_argument("--max-parks", type=int, default=0, help="Limit number of parks scanned (0 = no limit)")
    args = parser.parse_args()

    client = get_themeparks_wiki_client()

    destinations = client.get_destinations()
    disney_destinations = filter_disney_destinations(destinations, args.destination_filter)

    parks: List[Dict[str, Any]] = []
    for dest in disney_destinations:
        parks.extend(dest.get("parks", []))

    observed = gather_keys_for_parks(client, parks, args.max_parks)

    stats = {
        "destinations_scanned": len(disney_destinations),
        "parks_scanned": min(len(parks), args.max_parks) if args.max_parks else len(parks),
    }

    report = format_report(observed, stats)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

