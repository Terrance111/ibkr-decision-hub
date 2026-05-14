import os
import re
import tempfile
import time
from typing import Optional

import pandas as pd
from datetime import datetime, timedelta
import config
from data.flex_report import fetch_flex_report_dataframe, flex_send_request_url

HISTORY_FILE = os.path.join(config.CACHE_DIR, "trade_history.csv")

# First fetch when there is no local cache (or force_full): ask Flex from (today - N days) through today.
# Default ~2 years (730). Override with IBKR_TRADE_HISTORY_LOOKBACK_DAYS in .env for longer history.
_DEFAULT_FIRST_LOOKBACK_DAYS = 730

# Incremental runs: always request Flex from (day after last cached trade) through **today** — every segment
# until now(), so multi-week/month gaps are filled in one run (not "latest day only").
# Optional overlap pulls a few extra days before that gap for amended/corrected Flex rows (deduped).
_DEFAULT_INCREMENTAL_OVERLAP_DAYS = 7

# Flex Activity SendRequest (fd/td): IBKR portal often caps each query to ~365 calendar days.
# We use multiple smaller segments in a loop until "today". Default 364 leaves headroom; override with
# IBKR_FLEX_TRADE_SEGMENT_DAYS (1–365, values above 365 are clamped).
_DEFAULT_FLEX_TRADE_SEGMENT_DAYS = 364


def _flex_trade_segment_days() -> int:
    """Max calendar span per trades SendRequest (fd .. td), clamped for IBKR ~365-day limits."""
    raw = os.getenv(
        "IBKR_FLEX_TRADE_SEGMENT_DAYS",
        str(_DEFAULT_FLEX_TRADE_SEGMENT_DAYS),
    )
    try:
        d = int(raw)
    except ValueError:
        d = _DEFAULT_FLEX_TRADE_SEGMENT_DAYS
    return max(1, min(d, 365))


def _incremental_overlap_days() -> int:
    raw = os.getenv(
        "IBKR_TRADE_INCREMENTAL_OVERLAP_DAYS",
        str(_DEFAULT_INCREMENTAL_OVERLAP_DAYS),
    )
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_INCREMENTAL_OVERLAP_DAYS


def _first_fetch_start_date() -> str:
    """
    Earliest date for a full (non-incremental) fetch.

    Priority:
    1. IBKR_TRADE_HISTORY_START_DATE  – explicit date string, e.g. "20240101"
    2. IBKR_TRADE_HISTORY_LOOKBACK_DAYS – rolling lookback from today (default 730)
    """
    explicit = os.getenv("IBKR_TRADE_HISTORY_START_DATE", "").strip()
    if explicit and re.match(r"^\d{8}$", explicit):
        return explicit
    raw = os.getenv("IBKR_TRADE_HISTORY_LOOKBACK_DAYS", str(_DEFAULT_FIRST_LOOKBACK_DAYS))
    try:
        days = int(raw)
    except ValueError:
        days = _DEFAULT_FIRST_LOOKBACK_DAYS
    return (datetime.now() - timedelta(days=max(1, days))).strftime("%Y%m%d")


def _norm_header(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lstrip("\ufeff").lower())


def _columns_by_norm(df: pd.DataFrame) -> dict[str, list[str]]:
    """Map normalized header -> list of original column names (Flex may repeat spelling)."""
    out: dict[str, list[str]] = {}
    for c in df.columns:
        raw = str(c).strip().lstrip("\ufeff")
        out.setdefault(_norm_header(raw), []).append(raw)
    return out


def _first_column(by_norm: dict[str, list[str]], norm_keys: list[str]) -> Optional[str]:
    for k in norm_keys:
        cols = by_norm.get(k)
        if cols:
            return sorted(cols)[0]
    return None


# Normalized Flex header -> priority (lower = preferred). Used to pick one trade timestamp column.
_DATE_HEADER_PRIORITY = {
    "tradedate": 0,
    "datetime": 1,
    "tradetime": 2,
    "executiontime": 3,
    "exectime": 4,
    "date": 10,
}


def _parse_flex_dt(series: pd.Series) -> pd.Series:
    """
    Robustly parse an IBKR Flex date/datetime column.

    Handles the following value formats found in Flex CSVs:
      - ISO strings  "2024-08-29"  or  "2024-08-29 07:37:23"
      - YYYYMMDD;HHMMSS  "20240829;073723"  (IBKR DateTime column)
      - Pure YYYYMMDD strings  "20240829"
      - Corrupt nanosecond artifacts  "1970-01-01 00:00:00.020240829"
        (produced when pd.to_datetime(20240829) is called on an integer)

    The last case is detected by checking year==1970 with a non-trivial nanosecond
    component that encodes a real YYYYMMDD date; we extract the 8-digit suffix.
    """
    # Step 1: standard parse (handles ISO + most string formats)
    result = pd.to_datetime(series, errors="coerce")

    # Step 2: for NaT entries, try stripping the ';HHMMSS' suffix (IBKR DateTime format)
    nat_mask = result.isna()
    if nat_mask.any():
        stripped = series[nat_mask].astype(str).str.split(";").str[0].str.strip()
        alt = pd.to_datetime(stripped, format="%Y%m%d", errors="coerce")
        result = result.copy()
        result[nat_mask] = alt

    # Step 3: fix nanosecond corruption artifacts: year==1970 but the sub-second field
    # encodes a YYYYMMDD integer stored as nanoseconds.
    # e.g.  Timestamp('1970-01-01 00:00:00.020240829')  ← pd.to_datetime(20240829 as int ns)
    # Recovery:  t.microsecond * 1000 + t.nanosecond  gives back the original 8-digit integer.
    epoch_mask = result.dt.year == 1970
    if epoch_mask.any():
        ep = result[epoch_mask]
        # Reconstruct the original YYYYMMDD integer from sub-second components
        ns_val = ep.dt.microsecond * 1000 + ep.dt.nanosecond
        candidate_dates = ns_val.astype(str).str.zfill(8)
        repaired = pd.to_datetime(candidate_dates, format="%Y%m%d", errors="coerce")
        # Only accept repair if it gives a sane year (>= 2000)
        good_mask = repaired.dt.year >= 2000
        if good_mask.any():
            result = result.copy()
            result[ep.index[good_mask]] = repaired[good_mask].values

    return result


def _flex_trade_date_column(df: pd.DataFrame) -> Optional[str]:
    best_col = None
    best_pri = 10**9
    for c in df.columns:
        raw = str(c).strip().lstrip("\ufeff")
        key = _norm_header(raw)
        pri = _DATE_HEADER_PRIORITY.get(key, 10**9)
        if pri < best_pri or (pri == best_pri and best_col is not None and raw < best_col):
            best_pri = pri
            best_col = raw
    return best_col if best_pri < 10**9 else None


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map IBKR Flex Trades CSV columns to the app's schema.

    Field names follow IBKR's Trades Flex Statement reference (e.g. Symbol, Trade Date,
    Quantity, Trade Price, Proceeds, Buy/Sell, IB Commission):
    https://www.ibkrguides.com/reportingreference/reportguide/tradesfq.htm

    Headers in real CSVs may differ in spacing/case or use short labels (e.g. T. Price);
    we match normalized names and, if needed, derive unit price from Proceeds / Quantity
    as in IBKR's definition of Proceeds.
    """
    if df.empty:
        return df
    out = df.copy()
    out.columns = [str(c).strip().lstrip("\ufeff") for c in out.columns]
    by_norm = _columns_by_norm(out)

    date_src = _flex_trade_date_column(out)
    if date_src is None:
        preview = list(out.columns)[:45]
        raise ValueError(
            "Flex trade CSV has no recognizable trade date column "
            "(e.g. 'Trade Date', 'Date/Time'). "
            f"Columns ({len(out.columns)}): {preview}"
            + ("…" if len(out.columns) > len(preview) else "")
        )

    sym_src = _first_column(by_norm, ["symbol", "underlyingsymbol"])
    act_src = _first_column(by_norm, ["buysell", "side"])
    qty_src = _first_column(by_norm, ["quantity", "qty"])
    # Official primary: Trade Price; common alternates in exports / third-party CSV maps.
    px_src = _first_column(
        by_norm,
        [
            "tradeprice",
            "tprice",
            "execprice",
            "executionprice",
            "fillprice",
            "avgprice",
            "averageprice",
            "price",
        ],
    )
    proc_src = _first_column(by_norm, ["proceeds"])
    comm_src = _first_column(by_norm, ["ibcommission", "commission"])
    basis_src = _first_column(by_norm, ["costbasis"])
    cur_src = _first_column(by_norm, ["currency"])

    missing_core = []
    if sym_src is None:
        missing_core.append("Symbol")
    if act_src is None:
        missing_core.append("Buy/Sell")
    if qty_src is None:
        missing_core.append("Quantity")
    if missing_core:
        preview = list(out.columns)[:45]
        raise ValueError(
            "Flex trade CSV is missing required execution column(s): "
            + ", ".join(missing_core)
            + ". Expected names per IBKR Trades Flex guide (Symbol, Buy/Sell, Quantity). "
            f"Columns ({len(out.columns)}): {preview}"
            + ("…" if len(out.columns) > len(preview) else "")
        )

    rename: dict[str, str] = {date_src: "date"}
    if sym_src:
        rename[sym_src] = "symbol"
    if act_src:
        rename[act_src] = "action"
    if qty_src:
        rename[qty_src] = "quantity"
    if px_src:
        rename[px_src] = "price"
    if proc_src:
        rename[proc_src] = "proceeds"
    if comm_src:
        rename[comm_src] = "commission"
    if basis_src:
        rename[basis_src] = "cost_basis"
    if cur_src:
        rename[cur_src] = "currency"

    out = out.rename(columns=rename)
    out["date"] = _parse_flex_dt(out["date"])

    if "price" not in out.columns:
        if "proceeds" in out.columns and "quantity" in out.columns:
            _q = pd.to_numeric(out["quantity"], errors="coerce").abs()
            _pr = pd.to_numeric(out["proceeds"], errors="coerce").abs()
            _den = _q.replace(0, float("nan"))
            out["price"] = pd.to_numeric(_pr / _den, errors="coerce")
        else:
            preview = list(out.columns)[:45]
            raise ValueError(
                "Flex trade CSV has no price column (IBKR field 'Trade Price') and "
                "could not derive price from Proceeds / Quantity. "
                "See Trades fields: "
                "https://www.ibkrguides.com/reportingreference/reportguide/tradesfq.htm — "
                f"Columns ({len(out.columns)}): {preview}"
                + ("…" if len(out.columns) > len(preview) else "")
            )

    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out.dropna(subset=["date", "symbol", "quantity", "price"])

    # Filter to EXECUTION rows only to prevent double-counting.
    # IBKR Flex exports both ORDER (aggregate) and EXECUTION (individual fill) rows for the same
    # trade, plus CLOSED_LOT entries that are internal lot-matching records — not actual trades.
    # Keeping only EXECUTION rows gives exactly one row per physical fill.
    lod_col = next(
        (c for c in out.columns if re.sub(r"[^a-z0-9]+", "", c.strip().lower()) == "levelofdetail"),
        None,
    )
    if lod_col is not None:
        levels = out[lod_col].astype(str).str.strip().str.upper()
        exec_mask = levels == "EXECUTION"
        order_mask = levels == "ORDER"
        if exec_mask.any():
            out = out[exec_mask].copy()
        elif order_mask.any():
            out = out[order_mask].copy()

    return out


def _dedupe_trades(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate executions Flex may repeat across overlapping windows."""
    if df.empty:
        return df
    sub = df.copy()
    sub["_q"] = pd.to_numeric(sub["quantity"], errors="coerce")
    sub["_p"] = pd.to_numeric(sub["price"], errors="coerce")
    key_cols = ["symbol", "date", "action", "_q", "_p"]
    if "proceeds" in sub.columns:
        sub["_pr"] = pd.to_numeric(sub["proceeds"], errors="coerce")
        key_cols.append("_pr")
    sub = sub.sort_values("date").drop_duplicates(subset=key_cols, keep="first")
    drop_cols = [c for c in ["_q", "_p", "_pr"] if c in sub.columns]
    return sub.drop(columns=drop_cols)


def _fetch_segments(start_date: str) -> list:
    # Forward segmentation with automatic td-boundary discovery.
    #
    # IBKR Flex token's query window ends at (token_generation_date - 1).  Any request
    # with td beyond that date returns 1003, even when fd is valid and trades exist in the
    # fd-to-window-end range.  We discover the effective boundary automatically by backing
    # off td one day at a time (_try_fetch), so no manual configuration is needed.
    #
    # Algorithm:
    #  1. Forward segments of SEG_SPAN days (120d, well under the 365-day cap).
    #  2. Each fetch uses _try_fetch: if 1003/empty, backtrack td by 1 day up to
    #     MAX_TD_BACKTRACK times, returning the first successful (df, actual_td).
    #  3. When a segment's _try_fetch is still empty (genuinely no trades in range),
    #     probe the same span in PROBE_SPAN-day sub-windows using _try_fetch.
    #  4. After EMPTY_LIMIT consecutive fully-empty probe groups, stop.
    if not config.IBKR_FLEX_TOKEN or not config.IBKR_FLEX_QUERY_ID:
        raise ValueError(
            "Missing IBKR_FLEX_TOKEN or IBKR_FLEX_QUERY_ID. Check your .env file."
        )

    SEG_SPAN = 120          # primary segment length (calendar days)
    PROBE_SPAN = 30         # sub-window when primary segment is empty
    MAX_TD_BACKTRACK = 7    # max days to backtrack td before giving up on a window
    EMPTY_LIMIT = 3         # consecutive empty probe-groups before stopping

    base_url = flex_send_request_url()
    all_dfs = []
    current_start = datetime.strptime(start_date, "%Y%m%d").replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    now = datetime.now()
    seg_sleep = float(os.getenv("IBKR_FLEX_SEGMENT_SLEEP", "1.1"))

    print(
        f"🚀 Forward trade fetch: {current_start.strftime('%Y%m%d')} → today "
        f"({SEG_SPAN}-day segments, {PROBE_SPAN}-day probes, "
        f"≤{MAX_TD_BACKTRACK}-day td-backtrack per window)..."
    )

    # IBKR Flex CSVs always include structural footer rows (EOS/EOA/EOF in the HEADER
    # column) even when a date range has zero real trades.  Strip these so callers can
    # use df.empty as a reliable "no-data" check.
    _FLEX_FOOTER_TAGS = {"EOS", "EOA", "EOF", "HEADER"}

    def _do_fetch(fd: datetime, td: datetime) -> pd.DataFrame:
        params = {
            "t": config.IBKR_FLEX_TOKEN,
            "q": config.IBKR_FLEX_QUERY_ID,
            "v": "3",
            "fd": fd.strftime("%Y%m%d"),
            "td": td.strftime("%Y%m%d"),
        }
        raw = fetch_flex_report_dataframe(
            params,
            config.IBKR_FLEX_TOKEN,
            send_request_url=base_url,
            timeout=40,
            parse_for_trades=True,
        )
        if raw.empty:
            return raw
        hdr_col = next((c for c in raw.columns if str(c).strip().upper() == "HEADER"), None)
        if hdr_col is not None:
            mask = raw[hdr_col].astype(str).str.strip().str.upper().isin(_FLEX_FOOTER_TAGS)
            raw = raw[~mask]
        return raw

    def _try_fetch(fd: datetime, td: datetime):
        """
        Fetch fd→td, auto-backtracking td by 1 day at a time when result is empty.
        IBKR token window ends at (generation_date - 1); any td past that returns 1003.
        Returns (df, actual_td_used).  df is empty if all backtrack attempts failed.
        """
        for days_back in range(MAX_TD_BACKTRACK + 1):
            t = td - timedelta(days=days_back)
            if t < fd:
                break
            if days_back > 0:
                # Brief pause between backtrack retries to respect rate limits
                if seg_sleep > 0:
                    time.sleep(seg_sleep)
            df = _do_fetch(fd, t)
            if not df.empty:
                if days_back > 0:
                    print(f"      td-backtrack {days_back}d → td={t.strftime('%Y%m%d')} hit")
                return df, t
        return pd.DataFrame(), td

    def _log_df(df: pd.DataFrame) -> None:
        date_col = next(
            (c for c in df.columns
             if re.sub(r"[^a-z0-9]", "", str(c).lower()) in
                ("tradedate", "datetime", "date", "tradetime")),
            None,
        )
        if date_col is not None:
            parsed = pd.to_datetime(df[date_col], errors="coerce")
            mn, mx = parsed.min(), parsed.max()
            print(
                f"      → {len(df)} row(s)  "
                f"actual range: {mn.date() if pd.notna(mn) else '?'} ~ "
                f"{mx.date() if pd.notna(mx) else '?'}"
            )
        else:
            print(f"      → {len(df)} row(s)  (no date column detected)")

    seg_idx = 0
    consecutive_empty = 0

    while current_start < now:
        seg_idx += 1
        segment_end = min(current_start + timedelta(days=SEG_SPAN), now)

        print(f"   Segment {seg_idx}: fd={current_start.strftime('%Y%m%d')} ~ td={segment_end.strftime('%Y%m%d')}")

        df, used_td = _try_fetch(current_start, segment_end)

        if not df.empty:
            _log_df(df)
            all_dfs.append(df)
            consecutive_empty = 0
            # Advance past the td we actually used (not necessarily segment_end)
            current_start = used_td + timedelta(days=1)
            if seg_sleep > 0:
                time.sleep(seg_sleep)
            continue

        # Primary segment empty (all td-backtrack attempts failed): probe in sub-windows
        print(f"      → empty after td-backtrack; probing in {PROBE_SPAN}-day sub-windows...")
        probe_start = current_start
        probe_consecutive_empty = 0
        probe_found = False

        while probe_start < segment_end:
            probe_end = min(probe_start + timedelta(days=PROBE_SPAN), segment_end)
            df_p, used_ptd = _try_fetch(probe_start, probe_end)
            if seg_sleep > 0:
                time.sleep(seg_sleep)

            if not df_p.empty:
                print(f"      Probe {probe_start.strftime('%Y%m%d')}~{probe_end.strftime('%Y%m%d')} hit:")
                _log_df(df_p)
                all_dfs.append(df_p)
                probe_consecutive_empty = 0
                probe_found = True
                probe_start = used_ptd + timedelta(days=1)
            else:
                probe_consecutive_empty += 1
                probe_start = probe_end + timedelta(days=1)
                if probe_consecutive_empty >= 2:
                    break  # 2 consecutive empty probes; no point scanning further in this segment

        if probe_found:
            consecutive_empty = 0
        else:
            consecutive_empty += 1
            if consecutive_empty >= EMPTY_LIMIT:
                print(
                    f"   ⚠️  {EMPTY_LIMIT} consecutive empty segment groups — "
                    "assuming end of available Flex data. Stopping.\n"
                    "   If you believe newer trades exist, the most common cause is an\n"
                    "   expired Flex Web Service Token window. Fix:\n"
                    "   1. IBKR Client Portal → Reports → Flex Queries →\n"
                    "      Flex Web Service Configuration → Generate New Token\n"
                    "   2. Update IBKR_FLEX_TOKEN in your .env with the new token\n"
                    "   3. Click Refresh All Data from IBKR in the sidebar"
                )
                break

        current_start = probe_start  # advance past all probed dates

    print(
        f"✅ Fetch complete: {seg_idx} segment(s), "
        f"{sum(len(d) for d in all_dfs)} total rows before dedup."
    )

    # Next Flex call in the same app load is often positions/cash — short token cooldown.
    tail = float(os.getenv("IBKR_FLEX_AFTER_SEGMENTS_SLEEP", "2.0"))
    if tail > 0:
        time.sleep(tail)

    return all_dfs


def fetch_ibkr_trades(
    incremental: bool = True,
    force_full: bool = False,
    start_date_override: Optional[str] = None,
) -> pd.DataFrame:
    """
    Segmented Flex fetch + persistent merge into cache/trade_history.csv.

    - incremental=True (default): if HISTORY_FILE exists, fetch from slightly before the gap
      (last cached trade date + 1 day, minus IBKR_TRADE_INCREMENTAL_OVERLAP_DAYS) through **today**.
      _fetch_segments() loops in chunks of IBKR_FLEX_TRADE_SEGMENT_DAYS (default 364, max 365) until now(), so returning after weeks/months still
      downloads the full missing range — not a single calendar day.
    - force_full=True or incremental=False: ignore incremental window; fetch from IBKR_TRADE_HISTORY_LOOKBACK_DAYS
      through today and replace the on-disk history with that result (full rebuild from Flex).
    - start_date_override (YYYYMMDD string): if provided, force a full rebuild starting from that
      exact date — bypasses both the cache last_date and IBKR_TRADE_HISTORY_LOOKBACK_DAYS.

    On HTTP/parsing failure, existing HISTORY_FILE is left unchanged when it already existed.
    """
    # UI / caller-supplied explicit start date → full rebuild from that date
    if start_date_override and re.match(r"^\d{8}$", str(start_date_override).strip()):
        start_date = str(start_date_override).strip()
        full_rebuild = True
        local_df = pd.DataFrame()
        print(f"📁 Full fetch from Flex (start_date_override = {start_date})")
        try:
            all_dfs = _fetch_segments(start_date)
        except Exception:
            if os.path.isfile(HISTORY_FILE):
                print("⚠️ Fetch failed; keeping existing local trade_history.csv")
                return pd.read_csv(HISTORY_FILE)
            raise
        if not all_dfs:
            if os.path.isfile(HISTORY_FILE):
                print("✅ No new rows in override window; returning existing cache.")
                return pd.read_csv(HISTORY_FILE)
            raise ValueError("No data retrieved from Flex. Check Token, Query ID, and date range.")
        new_raw = pd.concat(all_dfs, ignore_index=True)
        final_df = _dedupe_trades(_standardize_columns(new_raw))
        final_df = final_df.sort_values("date").reset_index(drop=True)
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix="trade_history_", dir=config.CACHE_DIR)
        os.close(tmp_fd)
        try:
            final_df.to_csv(tmp_path, index=False)
            os.replace(tmp_path, HISTORY_FILE)
        except Exception:
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
            raise
        print(
            f"💾 Saved {len(final_df):,} trades to {HISTORY_FILE} "
            f"({final_df['date'].min().date()} → {final_df['date'].max().date()})"
        )
        return final_df

    full_rebuild = force_full or (not incremental)

    # When a full rebuild is requested, delete the old cache first so the final saved
    # file contains only the freshly-fetched window — no stale rows survive.
    if full_rebuild and os.path.isfile(HISTORY_FILE):
        try:
            os.remove(HISTORY_FILE)
            print(f"🗑️  Deleted existing cache for clean rebuild: {HISTORY_FILE}")
        except Exception as _e:
            print(f"⚠️  Could not delete old cache before rebuild: {_e}")

    local_df = pd.DataFrame()
    if not full_rebuild and incremental and os.path.isfile(HISTORY_FILE):
        local_df = pd.read_csv(HISTORY_FILE)
        if "date" not in local_df.columns:
            raise ValueError(f"Invalid trade cache schema in {HISTORY_FILE}: missing 'date' column.")
        local_df["date"] = pd.to_datetime(local_df["date"], errors="coerce")
        local_df = local_df.dropna(subset=["date"])
        # Apply the same LevelOfDetail filter as _standardize_columns so stale cache rows
        # (ORDER + CLOSED_LOT) don't slip through when the cache predates this fix.
        _lod = next(
            (c for c in local_df.columns if re.sub(r"[^a-z0-9]+", "", c.strip().lower()) == "levelofdetail"),
            None,
        )
        if _lod is not None:
            _lv = local_df[_lod].astype(str).str.strip().str.upper()
            _exec = _lv == "EXECUTION"
            _ord = _lv == "ORDER"
            if _exec.any():
                local_df = local_df[_exec].copy()
            elif _ord.any():
                local_df = local_df[_ord].copy()
        # Detect corrupt epoch-artifact dates (e.g. "1970-01-01 00:00:00.020240829")
        # produced by an older code version.  Attempting in-place repair is fragile and
        # has been observed to drop rows.  Instead, delete the cache and trigger a full
        # rebuild from the configured start date — the user will see a one-time refetch.
        _epoch_rows = local_df["date"].dt.year == 1970
        if _epoch_rows.any():
            print(
                f"⚠️  Detected {_epoch_rows.sum()} rows with corrupted dates (year=1970) in "
                f"{HISTORY_FILE}. Deleting cache and triggering a fresh full fetch."
            )
            local_df = pd.DataFrame()
            try:
                os.remove(HISTORY_FILE)
            except Exception:
                pass
            full_rebuild = True

        last_date = local_df["date"].max()
        # Safety floor: if the cache contains bogus epoch-0 dates (parsed as 1970-01-01),
        # last_date would be ancient and trigger thousands of useless Flex segments.
        # Fall back to a full-rebuild start date whenever last_date is suspiciously old.
        _floor = pd.Timestamp("2010-01-01")
        if pd.isna(last_date) or last_date < _floor:
            start_date = _first_fetch_start_date()
            print(
                f"⚠️  Cache last_date={last_date} is before {_floor.date()}; "
                f"falling back to full-fetch window starting {start_date}. "
                "If your account history starts before 2010 set IBKR_TRADE_HISTORY_START_DATE in .env."
            )
        else:
            gap_start = last_date + timedelta(days=1)
            overlap = _incremental_overlap_days()
            fetch_start = gap_start - timedelta(days=overlap)
            start_date = fetch_start.strftime("%Y%m%d")
            today = datetime.now().date()
            span_days = (today - fetch_start.date()).days + 1
            print(
                f"📁 Local cache last trade {last_date.date()} ({len(local_df):,} rows); "
                f"incremental Flex window {start_date} → today (~{span_days} calendar days"
                + (f", incl. {overlap}d overlap" if overlap else "")
                + ")"
            )
    else:
        start_date = _first_fetch_start_date()
        print(
            "📁 Full fetch from Flex "
            + ("(force_full / incremental=False)" if full_rebuild else "")
            + f" starting {start_date}"
        )

    try:
        all_dfs = _fetch_segments(start_date)
    except Exception:
        if not local_df.empty and os.path.isfile(HISTORY_FILE):
            print("⚠️ Fetch failed; keeping existing local trade_history.csv")
            return local_df.sort_values("date").reset_index(drop=True)
        raise

    if not all_dfs:
        if not local_df.empty:
            print(
                "✅ No new Flex rows in requested window; cache unchanged on disk. "
                "See [Flex] lines above: IBKR often returns ErrorCode 1003 or header-only CSV when "
                "there are no executions in that fd/td range. Manual Portal export may still show "
                "Symbol and older trades if you chose a wider date range than incremental sync."
            )
            return local_df.sort_values("date").reset_index(drop=True)
        raise ValueError("No data retrieved from Flex. Check Token, Query ID, and date range.")

    new_raw = pd.concat(all_dfs, ignore_index=True)
    new_df = _standardize_columns(new_raw)

    if full_rebuild:
        final_df = _dedupe_trades(new_df)
    elif local_df.empty:
        final_df = _dedupe_trades(new_df)
    else:
        merged = pd.concat([local_df, new_df], ignore_index=True)
        final_df = _dedupe_trades(merged)

    final_df = final_df.sort_values("date").reset_index(drop=True)

    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".csv", prefix="trade_history_", dir=config.CACHE_DIR
    )
    os.close(tmp_fd)
    try:
        final_df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, HISTORY_FILE)
    except Exception:
        if os.path.isfile(tmp_path):
            os.remove(tmp_path)
        raise

    print(
        f"💾 Saved {len(final_df):,} trades to {HISTORY_FILE} "
        f"({final_df['date'].min().date()} → {final_df['date'].max().date()})"
    )

    return final_df
