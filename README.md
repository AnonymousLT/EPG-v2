EPG Viewer (XMLTV) – Multi‑source merge, UI, and exports

EPG Viewer is a Node/Express app with a web UI for browsing, merging, and exporting EPG (XMLTV) from one or more sources. It supports playlist‑driven channel lists (M3U), per‑channel mapping to multiple EPG feeds, time‑window filtering, zone‑aware time shifting, and a fast export path with on‑disk caching.

Key goals:
- Make EPGs easy to preview and use with IPTV apps
- Allow multiple EPG sources and per‑channel assignment
- Produce fast, reliable exports (gz/xml) with proper time handling and caching


Features
- Streaming XMLTV parsing (SAX) with gzip handling for huge feeds
- Multi‑source EPG with per‑channel mapping (sourceId, epgChannelId)
- Zone‑aware time shifting (DST‑correct) and numeric offset shifting modes
- All exported timestamps normalized to +0000 to avoid IPTV offset issues
- Time windows (default: past 7 days, next 3) and full export (no limits)
- History backfill: retains snapshots of each source and can backfill past days from local history when an upstream feed only serves today+future
- Disk mirroring with ETag/Last‑Modified; reuses local files when unchanged
- Export prewarm/background build; cached .gz served immediately
- Web UI with tabs (General, Export, Sources, Assignments) and bulk actions


Requirements
- Node.js 18+ (Node 20+ recommended)
- macOS/Linux/Windows


Quick start
1) Install and run

cd epg-viewer
npm install
npm run dev

2) Open the app
- http://localhost:3333

3) Paste your playlist URL (M3U). The app detects the url-tvg EPG automatically unless you disable it in Settings.


UI overview
- Viewer
  - Left: searchable channel list with optional group filter
  - Right: EPG grid with independent scrolling; day navigation (Prev/Today/Next)
- Settings
  - Tabs: General, Export, Sources, Assignments
  - General: playlist URL, option to use EPG from playlist, past/future day window, host to display/export
  - Export: export URLs (gz/xml), Full EPG toggle, Prewarm button with live status, History backfill toggle and retention
  - Sources: add/enable/priority, rescan to fetch source channel lists (used for mapping suggestions)
  - Assignments:
    - Per‑row: Source (searchable), EPG Channel (searchable), Offset (minutes), Shift Mode (Wall/Offset), Zone (IANA)
    - Bulk bar: apply Source/Offset/Mode/Zone to selected or all channels
    - All changes auto‑save; EPG Channel suggestions populate after selecting a Source


Time shifting modes and zones
- Shift Wall Clock (recommended)
  - Adjusts wall time by offsetMinutes in the specified zoneId (IANA), honoring DST at the programme instant.
  - Export writes the correct numeric offset for that instant, then normalizes to +0000 for IPTV compatibility.
- Shift Numeric Offset
  - Keeps the wall‑time digits and only adjusts the numeric ±HHMM offset by offsetMinutes.
- Zone (IANA) examples: Europe/Vilnius, Europe/Riga, Europe/London, America/New_York.
  - Use the broadcaster’s local time zone for correct DST behavior.


Exports
- GZip XMLTV (recommended): /epg.xml.gz
- Plain XMLTV: /epg.xml

Parameters:
- Windowed: ?pastDays=7&futureDays=3
- Full (no limits): ?full=1
- Optional (when not using server defaults): ?playlist=... or ?epg=...

Examples:
# Windowed
curl -s 'http://localhost:3333/epg.xml.gz?pastDays=7&futureDays=3' -o epg.gz

# Full
curl -s 'http://localhost:3333/epg.xml.gz?full=1' -o full.gz

# Validate / inspect
gzip -t epg.gz && echo OK
gunzip -c epg.gz | head -n 5

Notes:
- Exported timestamps are normalized to +0000 to avoid IPTV apps misapplying offsets. Wall‑times remain correct based on your shift mode/zone.
- Prewarming builds the export in the background; subsequent downloads stream from disk immediately.


History backfill (for sources that only serve today+future)
- The app mirrors each EPG source to disk and, on change, rotates the previous file into a timestamped snapshot.
- When exporting past days, the exporter can backfill from recent snapshots to cover the requested window.
- Controls: Settings → Export
  - Backfill past days from history (on/off)
  - History retention (days) — default 21 days

How it works:
- On mirror update, the old file is renamed as data/mirror/<hash>.YYYYMMDDHHmmss.xmltv[.gz].
- Exports scan recent snapshots (newest first) and stream‑parse within the past window, merging/deduping programmes.
- Cache keys include snapshot timestamps so exports invalidate as history rotates.

Limitations:
- If you start today, you cannot reconstruct the last week retroactively. Backfill will fill as your history accumulates.


API reference
- Channels (from playlist)
  - GET /api/channels?playlist=...
- Preview merged EPG (windowed, JSON)
  - GET /api/epg?playlist=...
  - GET /api/epg?playlist=...&debug=1 (debug IDs)
- Per‑channel daily JSON
  - GET /api/epg/channel?id=<playlistChannelId>&from=<ISO>&to=<ISO>&playlist=...
- Exports
  - GET /epg.xml.gz (or /api/export/epg.xml.gz) — supports pastDays, futureDays, full, playlist, epg
  - GET /epg.xml (or /api/export/epg.xml)
  - POST /api/export/prewarm → { pastDays, futureDays, playlist, epg, full }
  - GET /api/export/status?key=... — when complete returns { status: 'done' }.
- Settings
  - GET /api/settings
  - POST /api/settings → { playlistUrl, epgUrl, usePlaylistEpg, pastDays, futureDays, historyBackfill, historyRetentionDays }
- Sources
  - GET /api/sources
  - POST /api/sources → add/update { id?, url, enabled, priority }
  - DELETE /api/sources/:id
  - POST /api/sources/:id/rescan
  - GET /api/sources/:id/channels
- Mappings
  - GET /api/mappings
  - POST /api/mappings (single): { playlistId, sourceId?, epgChannelId?, offsetMinutes?, zoneId?, shiftMode? }
  - POST /api/mappings (bulk): { mappings: { [playlistId]: { sourceId?, epgChannelId?, offsetMinutes?, zoneId?, shiftMode? } } }
- Diagnostics
  - GET /api/_diag/headers?url=...
  - GET /api/_diag/epg-channels?epg=...
  - GET /api/_diag/epg-channels-simple?epg=...
  - GET /api/_diag/stream-head?epg=...


Data directories
epg-viewer/data/
- settings.json — saved defaults and mappings
- mirror/ — current mirror per source + timestamped snapshots
- cache/exports/ — prewarmed .xml.gz files
- cache/schedules/ — per‑channel JSON cache for UI
- source-cache/ — scanned channel lists per source

You can safely delete the cache/ folders to force a rebuild (mirrors/snapshots are retained).


Tips and troubleshooting
- “Non‑whitespace before first tag (0x1f)” usually means a gzipped feed was parsed as plain XML. The app auto‑detects gzip, but double‑check your EPG URL.
- If a .gz download is tiny or fails to expand, ensure prewarm completed; the exporter writes through a temp file and only promotes complete files.
- If times look off by hours, prefer Shift Wall Clock with the correct Zone (IANA) and set IPTV app time‑shift to 0 to avoid double‑shifting.
- First export with a large feed may take time; prewarm before sharing URLs with apps.


Development
- Dev server: npm run dev (nodemon) on port 3333
- Main files
  - Server: epg-viewer/server.js
  - Client: epg-viewer/public/index.html, public/app.js, public/styles.css
  - Parsers/storage: epg-viewer/src/*


License
No license specified. Use at your discretion.

