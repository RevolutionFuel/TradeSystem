#!/usr/bin/env python3
"""
Periodic vessel identity/specs refresh, via VesselAPI.

Unlike ais_listener.py (a persistent process), this is a short-lived script
meant to run on a schedule (a Render Cron Job, not a Background Worker) —
it does its work and exits. Two jobs in one pass, since both need the same
per-vessel API call anyway:

  1. Resolves MMSI from IMO for any vessel that doesn't have one yet
     (fills in mmsi_verified_at when it does).
  2. Refreshes callsign, dimensions, tonnage, and year built for every
     vessel with a valid IMO — cheap to keep current since we're already
     calling the API for MMSI resolution anyway, and genuinely useful for
     a fuel broker (draught/length/beam matter for port and berth
     suitability).

Only ever touches vessels with a real, checksum-valid IMO — MMSI can't be
looked up from the "official numbers" some smaller yachts use instead of a
genuine IMO, and there's no reliable way to resolve those automatically.

Run this every few months (quarterly is a reasonable default) via Render's
Cron Job service — see README.md for setup. It is NOT meant to run
continuously; that's what ais_listener.py is for.
"""

import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from supabase import create_client, Client

# Force unbuffered output - otherwise Python holds print() output back until
# the buffer fills or the process exits when stdout isn't a real terminal
# (like here), which would make this script's whole point - watching it
# work through the vessel list - invisible until the very end.
sys.stdout.reconfigure(line_buffering=True)

SUPABASE_URL = "https://dxaajzdolalessivlseg.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
VESSELAPI_KEY = os.environ.get("VESSELAPI_KEY")

VESSELAPI_BASE = "https://api.vesselapi.com/v1"
REQUEST_DELAY_SECONDS = 0.5  # be a reasonable neighbour on the free tier
# Hard cap on lookups per run - VesselAPI's free tier is 150 calls/month,
# and this script has no visibility into how many have already been used
# this month (including by other runs, or manual testing). Defaulting well
# under the monthly limit so one run can never accidentally exhaust it -
# override via env var once actual usage patterns are better understood.
MAX_LOOKUPS_PER_RUN = int(os.environ.get("MAX_LOOKUPS_PER_RUN", "150"))
# Vessels are processed oldest-checked-first (see the query ordering in
# main()), so running this monthly at the free tier's full 150/month budget
# steadily works through the whole fleet and then starts refreshing the
# oldest data again - a genuine rotation, not the same vessels every time.

if not SUPABASE_SERVICE_ROLE_KEY:
    print("FATAL: SUPABASE_SERVICE_ROLE_KEY environment variable is not set.", file=sys.stderr)
    sys.exit(1)
if not VESSELAPI_KEY:
    print("FATAL: VESSELAPI_KEY environment variable is not set.", file=sys.stderr)
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def is_valid_imo(imo: str | None) -> bool:
    """The real IMO check-digit rule: 7 digits, where the 7th is a checksum
    of the first 6 (each multiplied by its position weight 7..2, summed,
    mod 10). This is the actual validity test - NOT a numeric-range guess,
    which is unreliable (confirmed the hard way earlier in this project)."""
    if not imo or not re.match(r"^\d{7}$", imo):
        return False
    digits = [int(c) for c in imo]
    checksum = sum(digits[i] * (7 - i) for i in range(6))
    return checksum % 10 == digits[6]


class VesselApiAuthError(Exception):
    """Raised when VesselAPI rejects the key itself (401/403) - distinct
    from an ordinary per-vessel failure, since every remaining vessel this
    run would fail the exact same way. No point burning through the whole
    batch one at a time to discover that; better to stop immediately with
    an unmistakable message."""
    pass


def fetch_vessel_details(imo: str, debug: bool = False) -> dict | None:
    """Looks up a vessel by IMO via VesselAPI's vessel lookup endpoint.
    Returns None if not found or on error (never raises - a single bad
    lookup shouldn't stop the whole batch). When debug=True, prints the
    raw response regardless of outcome - used for the first lookup of a
    run, so a systematic problem (wrong param name, auth issue, unexpected
    response shape) is visible immediately rather than silently producing
    188 identical "not found" results with no clue why."""
    try:
        res = requests.get(
            f"{VESSELAPI_BASE}/vessel/{imo}",
            headers={"Authorization": f"Bearer {VESSELAPI_KEY}"},
            params={"filter.idType": "imo"},
            timeout=15,
        )
        if debug:
            print(f"[debug] GET {res.url}")
            print(f"[debug] status={res.status_code} body={res.text[:500]}")
        if res.status_code == 404:
            return None
        if res.status_code in (401, 403):
            raise VesselApiAuthError(
                f"VesselAPI rejected the API key (status {res.status_code}): {res.text[:200]}\n"
                f"This usually means the key has expired or been revoked - VesselAPI keys are "
                f"valid for 90 days. Generate a new one at vesselapi.com and update VESSELAPI_KEY "
                f"in this Cron Job's environment variables."
            )
        if not res.ok:
            print(f"[warn] VesselAPI returned {res.status_code} for IMO {imo}: {res.text[:200]}")
            return None
        data = res.json()
        # Handle whichever shape this endpoint actually returns - confirmed
        # inconsistent in practice: some vessels come back as a flat object,
        # others wrapped in a "vessel" key, others (maybe) in "data"/
        # "results". Checking all known wrapper keys rather than assuming
        # any one shape is reliable.
        if isinstance(data, dict):
            for wrapper_key in ("vessel", "data", "results"):
                if wrapper_key in data:
                    candidate = data[wrapper_key]
                    if isinstance(candidate, list):
                        return candidate[0] if candidate else None
                    return candidate
            return data if data else None
        if isinstance(data, list):
            return data[0] if data else None
        return None
    except requests.RequestException as e:
        print(f"[warn] request failed for IMO {imo}: {e}")
        return None


def write_alert(source: str, message: str) -> None:
    """Best-effort - if this itself fails, log it but don't let alerting
    problems crash the actual work the script is doing."""
    try:
        supabase.table("system_alerts").insert({
            "source": source, "severity": "error", "message": message
        }).execute()
    except Exception as e:
        print(f"[warn] could not write alert to database: {e}")


def main() -> None:
    res = (
        supabase.table("vessels")
        .select("id, name, imo, mmsi, track_ais, specs_updated_at")
        .is_("deleted_at", "null")
        .order("specs_updated_at", desc=False, nullsfirst=True)
        .execute()
    )
    all_vessels = res.data

    candidates = [v for v in all_vessels if is_valid_imo(v.get("imo"))]
    skipped_invalid_imo = len(all_vessels) - len(candidates)
    if len(candidates) > MAX_LOOKUPS_PER_RUN:
        print(f"{len(candidates)} vessels have a valid IMO, but this run is capped at "
              f"{MAX_LOOKUPS_PER_RUN} lookups (set MAX_LOOKUPS_PER_RUN to change this) - "
              f"processing the first {MAX_LOOKUPS_PER_RUN} now, rest on a future run.")
        candidates = candidates[:MAX_LOOKUPS_PER_RUN]
    print(f"{len(all_vessels)} total vessels, processing {len(candidates)} this run "
          f"({skipped_invalid_imo} skipped - no usable IMO to look up by)")

    updated = 0
    not_found = 0
    for i, v in enumerate(candidates):
        try:
            details = fetch_vessel_details(v["imo"], debug=(i == 0))
        except VesselApiAuthError as e:
            print("=" * 70)
            print(f"[STOPPING] {e}")
            print(f"Processed {i} of {len(candidates)} vessels before this happened; "
                  f"{updated} updated, {not_found} not found so far this run.")
            print("=" * 70)
            write_alert("vesselapi", f"VesselAPI key was rejected during the monthly vessel "
                        f"specs refresh - it's likely expired (keys last ~90 days) or revoked. "
                        f"Generate a new one at vesselapi.com and update VESSELAPI_KEY in the "
                        f"'Update Vessel Details' Cron Job's environment variables on Render. "
                        f"Detail: {e}")
            return
        time.sleep(REQUEST_DELAY_SECONDS)
        if details is None:
            not_found += 1
            continue

        update_row: dict = {"specs_updated_at": datetime.now(timezone.utc).isoformat()}

        new_mmsi = details.get("mmsi")
        if new_mmsi:
            new_mmsi = str(new_mmsi)
            if new_mmsi != v.get("mmsi"):
                update_row["mmsi"] = new_mmsi
                update_row["mmsi_verified_at"] = datetime.now(timezone.utc).isoformat()
            # AISStream is a flat subscription, free regardless of vessel
            # count (unlike VesselAPI's per-lookup billing) - any vessel
            # with a usable MMSI should be tracked, no reason to hold back.
            if not v.get("track_ais"):
                update_row["track_ais"] = True

        if details.get("call_sign"):
            update_row["callsign"] = details["call_sign"]
        if details.get("length") is not None:
            update_row["length_m"] = details["length"]
        if details.get("breadth") is not None:
            update_row["beam_m"] = details["breadth"]
        if details.get("draught_calculated_avg") is not None:
            update_row["draught_m"] = details["draught_calculated_avg"]
        if details.get("gross_tonnage") is not None:
            update_row["gross_tonnage"] = details["gross_tonnage"]
        if details.get("deadweight") is not None:
            update_row["deadweight_tonnes"] = details["deadweight"]
        if details.get("year_built") is not None:
            update_row["year_built"] = details["year_built"]

        try:
            supabase.table("vessels").update(update_row).eq("id", v["id"]).execute()
            updated += 1
            print(f"[ok] {v['name']} (IMO {v['imo']}) - {len(update_row) - 1} field(s) refreshed"
                  + (f", MMSI {'set' if not v.get('mmsi') else 'changed'} to {update_row.get('mmsi')}" if "mmsi" in update_row else ""))
        except Exception as e:
            print(f"[error] failed to update {v['name']} ({v['id']}): {e}")

    print(f"\nDone. {updated} vessels updated, {not_found} not found in VesselAPI, "
          f"{skipped_invalid_imo} skipped (no valid IMO).")


if __name__ == "__main__":
    main()
