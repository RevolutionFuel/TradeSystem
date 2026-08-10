# AIS Position Listener

A persistent background process that connects to [AISStream.io](https://aisstream.io)'s
free live AIS WebSocket feed, tracks every vessel with `track_ais = true` and
a known MMSI, and logs their positions into Supabase — both a continuously
updated "latest position" cache on the vessel row, and a historical log
(roughly every 6 hours per vessel) for building a track over time.

This is a real, always-on process. It cannot run as a Supabase Edge
Function (those can't hold a persistent WebSocket connection open) —
it's deployed here as a Render Background Worker instead.

## Deploying on Render

1. In the Render dashboard, **New +** → **Background Worker**.
2. Connect the `RevolutionFuel/TradeSystem` GitHub repo.
3. **Root Directory**: `ais-listener`
4. **Runtime**: Python 3
5. **Build Command**: `pip install -r requirements.txt`
6. **Start Command**: `python3 ais_listener.py`
7. **Instance Type**: Starter ($7/month) — the free tier sleeps on
   inactivity, which would kill the persistent connection.
8. Under **Environment**, add two environment variables:
   - `SUPABASE_SERVICE_ROLE_KEY` — from Supabase dashboard → Project
     Settings → API → `service_role` key. This bypasses all row-level
     security, so treat it like a password.
   - `AISSTREAM_API_KEY` — from your AISStream.io dashboard.
9. Create the worker. Check the **Logs** tab after a minute or two — you
   should see `Starting AIS listener for N tracked vessels` followed by
   one `subscribed to ... MMSIs` line per connection (one per 50 vessels).

## How it decides what to track

Any vessel with `track_ais = true` and a non-empty `mmsi` in the Trade
System's Manage Lists (Vessels) is tracked automatically. The listener
re-checks this list every 5 minutes; if it's changed, the process
restarts itself and Render brings it back up with the new list. No
manual restart needed when a vessel is added, removed, or its MMSI
is corrected.

## Scaling beyond ~250 vessels

Nothing to change in the code — `MMSI_PER_CONNECTION` (50, AISStream's own
subscription limit) determines how many concurrent WebSocket connections
open automatically as the tracked-vessel count grows. Going from 250 to
1,000 vessels just means ~20 connections instead of ~5. Worth checking
AISStream's plan limits on concurrent connections if the count gets large,
and Render's worker resource limits if it does.

## Local testing

```bash
cd ais-listener
pip install -r requirements.txt
export SUPABASE_SERVICE_ROLE_KEY="..."
export AISSTREAM_API_KEY="..."
python3 ais_listener.py
```
