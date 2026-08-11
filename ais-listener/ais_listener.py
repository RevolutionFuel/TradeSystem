#!/usr/bin/env python3
"""
AIS position listener for the Revolution Fuel Trade System.

Connects to AISStream.io (a free live AIS WebSocket feed) in groups of up
to 50 MMSIs per connection (AISStream's documented per-subscription limit),
covering every vessel with track_ais=true and a known MMSI. Runs
continuously as a Render Background Worker — this is NOT something that
can run as a Supabase Edge Function, since those can't hold a persistent
connection open.

For every PositionReport received:
  - The vessel's cached "last known position" (on the vessels row itself)
    is always updated, so a UI can show "current position" with a single
    row lookup, no join needed.
  - A historical row is only inserted roughly every 6 hours per vessel
    (HISTORICAL_SAVE_INTERVAL below) — not on every single AIS transmission,
    which could otherwise be every few seconds while underway.

Design choices worth knowing if you're picking this up later:
  - The tracked-vessel/MMSI list is loaded once at startup. It's re-checked
    periodically (VESSEL_REFRESH_INTERVAL) purely to detect changes; if the
    set of MMSIs has changed (a vessel added/removed/re-flagged), the whole
    process exits and lets Render's automatic restart pick up the new list.
    This is deliberately simpler than live-reshuffling WebSocket
    subscriptions across running connections, at the cost of a brief
    (seconds) reconnect gap when the vessel list changes — which happens
    rarely, so this trade-off is a reasonable one.
  - AISStream's own timestamp format is not standard ISO 8601 (it's a Go-
    style string like "2024-05-20 09:21:31.781972101 +0000 UTC") — see
    parse_ais_timestamp() below.
  - Scaling from ~250 vessels to 1000+ later is just MMSI_PER_CONNECTION
    producing more groups automatically — no code change needed, only more
    concurrent connections (and a correspondingly larger AISStream/Render
    plan if their free/starter tiers cap concurrent connections).
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import websockets
from supabase import create_client, Client

SUPABASE_URL = "https://dxaajzdolalessivlseg.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY")

MMSI_PER_CONNECTION = 50
HISTORICAL_SAVE_INTERVAL = timedelta(hours=6)
VESSEL_REFRESH_INTERVAL_SECONDS = 300  # 5 minutes
AISSTREAM_URI = "wss://stream.aisstream.io/v0/stream"

if not SUPABASE_SERVICE_ROLE_KEY:
    print("FATAL: SUPABASE_SERVICE_ROLE_KEY environment variable is not set.", file=sys.stderr)
    sys.exit(1)
if not AISSTREAM_API_KEY:
    print("FATAL: AISSTREAM_API_KEY environment variable is not set.", file=sys.stderr)
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def write_alert(source: str, message: str) -> None:
    """Best-effort - if this itself fails, log it but don't let alerting
    problems interrupt the actual listener."""
    try:
        supabase.table("system_alerts").insert({
            "source": source, "severity": "error", "message": message
        }).execute()
    except Exception as e:
        print(f"[warn] could not write alert to database: {e}")

# In-memory state, seeded from the database at startup.
mmsi_to_vessel: dict[str, str] = {}   # MMSI (string) -> vessel id
last_historical_save: dict[str, datetime] = {}  # vessel id -> ais_timestamp of last saved historical row


def load_tracked_vessels() -> dict[str, str]:
    """Every vessel with track_ais=true and a usable MMSI, keyed by MMSI."""
    res = (
        supabase.table("vessels")
        .select("id, mmsi")
        .eq("track_ais", True)
        .is_("deleted_at", "null")
        .execute()
    )
    return {str(v["mmsi"]): v["id"] for v in res.data if v.get("mmsi")}


def seed_last_historical_save() -> None:
    """On startup, load each vessel's most recent historical save time, so a
    restart doesn't immediately re-save a fresh historical row for every
    vessel regardless of when it last actually got one."""
    res = (
        supabase.table("vessel_positions")
        .select("vessel_id, ais_timestamp")
        .order("ais_timestamp", desc=True)
        .execute()
    )
    seen = set()
    for row in res.data:
        vid = row["vessel_id"]
        if vid in seen:
            continue
        seen.add(vid)
        last_historical_save[vid] = parse_iso(row["ais_timestamp"])


def parse_iso(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_ais_timestamp(ts_str: str) -> datetime:
    """AISStream's MetaData.time_utc looks like:
    "2024-05-20 09:21:31.781972101 +0000 UTC" - not standard ISO 8601.
    Falls back to "now" if the format is ever unexpectedly different,
    rather than crashing the whole listener over one malformed message."""
    try:
        base = ts_str.split(" +")[0]  # "2024-05-20 09:21:31.781972101"
        if "." in base:
            date_part, frac = base.split(".")
            frac = (frac + "000000")[:6]  # pad/truncate to microsecond precision
            dt = datetime.strptime(f"{date_part}.{frac}", "%Y-%m-%d %H:%M:%S.%f")
        else:
            dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"[warn] could not parse AIS timestamp {ts_str!r}: {e} - using current time instead")
        return datetime.now(timezone.utc)


def chunk(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def handle_position_report(vessel_id: str, mmsi: str, report: dict, ais_ts: datetime) -> None:
    lat = report.get("Latitude")
    lon = report.get("Longitude")
    if lat is None or lon is None:
        return
    sog = report.get("Sog")
    cog = report.get("Cog")
    heading = report.get("TrueHeading")
    nav_status = report.get("NavigationalStatus")
    nav_status_str = str(nav_status) if nav_status is not None else None
    received_at = datetime.now(timezone.utc).isoformat()

    # Always keep the vessel's cached "latest position" current - this is
    # what the app's UI reads for a fast "where is this vessel now" lookup,
    # every message, not just the ones that also get a historical row.
    try:
        supabase.table("vessels").update({
            "last_position_lat": lat,
            "last_position_lon": lon,
            "last_position_sog": sog,
            "last_position_cog": cog,
            "last_position_heading": heading,
            "last_position_nav_status": nav_status_str,
            "last_position_ais_timestamp": ais_ts.isoformat(),
            "last_position_received_at": received_at,
        }).eq("id", vessel_id).execute()
    except Exception as e:
        print(f"[error] failed to update cached position for vessel {vessel_id}: {e}")

    # Only log a historical row roughly every 6 hours per vessel - not every
    # single AIS transmission, which could be every few seconds underway.
    prev = last_historical_save.get(vessel_id)
    if prev is not None and (ais_ts - prev) < HISTORICAL_SAVE_INTERVAL:
        return
    try:
        supabase.table("vessel_positions").insert({
            "vessel_id": vessel_id,
            "mmsi": mmsi,
            "latitude": lat,
            "longitude": lon,
            "sog": sog,
            "cog": cog,
            "heading": heading,
            "nav_status": nav_status_str,
            "ais_timestamp": ais_ts.isoformat(),
            "received_at": received_at,
            "source": "aisstream",
        }).execute()
        last_historical_save[vessel_id] = ais_ts
    except Exception as e:
        # A unique-constraint clash on (vessel_id, ais_timestamp) can
        # legitimately happen if two messages report the identical
        # timestamp - not a real error, nothing to do about it.
        print(f"[warn] could not save historical position for vessel {vessel_id}: {e}")


async def run_connection(mmsi_group: list[str], group_index: int) -> None:
    """One persistent WebSocket connection, subscribed to up to 50 MMSIs.
    Reconnects with exponential backoff on any error or disconnect. If
    failures are sustained (backoff hits its ceiling - roughly 6+
    consecutive failures within minutes, not just one network blip), writes
    a database alert once per failure streak, since that pattern usually
    means something systematic is wrong (e.g. an invalid/revoked
    AISSTREAM_API_KEY) rather than an ordinary transient disconnect."""
    backoff = 5
    already_alerted = False
    while True:
        try:
            async with websockets.connect(AISSTREAM_URI, ping_interval=20, ping_timeout=20) as ws:
                subscribe_msg = {
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": [[[-90, -180], [90, 180]]],  # required by AISStream even when filtering by MMSI
                    "FiltersShipMMSI": mmsi_group,
                    "FilterMessageTypes": ["PositionReport"],
                }
                await ws.send(json.dumps(subscribe_msg))
                print(f"[group {group_index}] subscribed to {len(mmsi_group)} MMSIs")
                backoff = 5  # reset once a connection succeeds
                already_alerted = False
                total_received = 0
                position_reports = 0
                matched_vessels = 0
                last_heartbeat = time.time()
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        print(f"[group {group_index}] heartbeat: connection still open, but 0 messages "
                              f"of any kind received in the last 60s (totals so far: {total_received} "
                              f"messages, {position_reports} position reports, {matched_vessels} matched "
                              f"our tracked vessels)")
                        last_heartbeat = time.time()
                        continue
                    total_received += 1
                    if time.time() - last_heartbeat > 60:
                        print(f"[group {group_index}] heartbeat: {total_received} messages received so far "
                              f"({position_reports} position reports, {matched_vessels} matched our tracked "
                              f"vessels)")
                        last_heartbeat = time.time()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("MessageType") != "PositionReport":
                        continue
                    position_reports += 1
                    metadata = msg.get("MetaData", {})
                    mmsi = str(metadata.get("MMSI", ""))
                    vessel_id = mmsi_to_vessel.get(mmsi)
                    if not vessel_id:
                        continue
                    matched_vessels += 1
                    report = msg.get("Message", {}).get("PositionReport", {})
                    ais_ts = parse_ais_timestamp(metadata.get("time_utc", ""))
                    handle_position_report(vessel_id, mmsi, report, ais_ts)
        except Exception as e:
            print(f"[group {group_index}] connection error: {e} - reconnecting in {backoff}s")
            if backoff >= 300 and not already_alerted:
                write_alert("aisstream", f"The AIS listener (connection group {group_index}) has "
                            f"been failing to connect repeatedly for several minutes - this usually "
                            f"means AISSTREAM_API_KEY is invalid or has been revoked. Check/replace "
                            f"it in the 'ais-listener' Background Worker's environment variables on "
                            f"Render. Last error: {e}")
                already_alerted = True
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


async def refresh_loop() -> None:
    """Periodically checks whether the tracked-vessel MMSI list has changed.
    If it has, exits the whole process deliberately - see the module
    docstring for why this is a reasonable simplification rather than
    live-reshuffling connections."""
    while True:
        await asyncio.sleep(VESSEL_REFRESH_INTERVAL_SECONDS)
        try:
            current = load_tracked_vessels()
            if set(current.keys()) != set(mmsi_to_vessel.keys()):
                print(f"[refresh] tracked MMSI list changed ({len(mmsi_to_vessel)} -> {len(current)} vessels) - restarting to apply")
                sys.exit(0)
        except Exception as e:
            print(f"[refresh] check failed (will retry next interval): {e}")


async def main() -> None:
    global mmsi_to_vessel
    mmsi_to_vessel = load_tracked_vessels()
    seed_last_historical_save()
    print(f"Starting AIS listener for {len(mmsi_to_vessel)} tracked vessels")

    if not mmsi_to_vessel:
        print("No vessels currently have track_ais=true and a usable MMSI - idling, will check again periodically.")

    groups = list(chunk(list(mmsi_to_vessel.keys()), MMSI_PER_CONNECTION))
    print(f"Opening {len(groups)} AISStream connection(s) ({MMSI_PER_CONNECTION} MMSIs/connection max)")

    tasks = [asyncio.create_task(run_connection(g, i)) for i, g in enumerate(groups)]
    tasks.append(asyncio.create_task(refresh_loop()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
