"""
IBKR Flex Web Service: SendRequest often returns XML with <ReferenceCode>;
the CSV body is retrieved via GetStatement. Some setups return CSV directly from SendRequest.
"""

from __future__ import annotations

import csv
import os
import re
import time
import xml.etree.ElementTree as ET
from io import StringIO
from typing import Any, Dict, Optional

import pandas as pd
import requests

import config

# IBKR requires a User-Agent on all Flex Web Service requests (see Flex Web Service campus doc).
_DEFAULT_UA = "ibkr-tracker/1.0 (+https://www.interactivebrokers.com/campus/ibkr-api-page/flex-web-service/)"


def _flex_headers() -> Dict[str, str]:
    ua = (os.getenv("IBKR_FLEX_USER_AGENT") or "").strip() or _DEFAULT_UA
    return {"User-Agent": ua}


def flex_send_request_url() -> str:
    """Base SendRequest URL only; query params (t, q, v, fd, td) are passed separately."""
    return str(getattr(config, "IBKR_FLEX_SEND_REQUEST_URL", "") or "").strip()


def send_request_to_get_statement_url(send_request_url: str) -> str:
    if "SendRequest" not in send_request_url:
        raise ValueError("Expected 'SendRequest' in Flex URL")
    return send_request_url.replace("SendRequest", "GetStatement")


def _extract_reference_code(xml_text: str) -> Optional[str]:
    if not xml_text or not xml_text.strip():
        return None
    try:
        root = ET.fromstring(xml_text.strip())
        for el in root.iter():
            local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if local.lower() == "referencecode" and el.text and el.text.strip():
                return el.text.strip()
    except ET.ParseError:
        pass
    m = re.search(
        r"<ReferenceCode>\s*(?:<!\[CDATA\[)?\s*([^<\]]+?)\s*(?:\]\]>)?\s*</ReferenceCode>",
        xml_text,
        re.I | re.DOTALL,
    )
    if m:
        return m.group(1).strip()
    m2 = re.search(r'referenceCode\s*=\s*["\']([^"\']+)["\']', xml_text, re.I)
    return m2.group(1).strip() if m2 else None


def _extract_error_message(xml_text: str) -> Optional[str]:
    m = re.search(r"<ErrorMessage>\s*([^<]+?)\s*</ErrorMessage>", xml_text, re.I | re.DOTALL)
    return m.group(1).strip() if m else None


def _extract_error_code(xml_text: str) -> Optional[str]:
    m = re.search(r"<ErrorCode>\s*([^<]+?)\s*</ErrorCode>", xml_text, re.I | re.DOTALL)
    return m.group(1).strip() if m else None


def _looks_like_xml(text: str) -> bool:
    head = text.lstrip()[:400].lower()
    return head.startswith("<?xml") or head.startswith("<flex")


def _read_csv_flex_string(raw: str, skiprows: int, **kwargs: Any) -> pd.DataFrame:
    """Single pd.read_csv from string buffer (fresh StringIO each call)."""
    kw: Dict[str, Any] = {"skiprows": skiprows, **kwargs}
    if kw.get("engine") != "python":
        kw["low_memory"] = False
    return pd.read_csv(StringIO(raw), **kw)


def _df_looks_like_misread_as_tsv(df: pd.DataFrame) -> bool:
    """Comma-separated text wrongly read with sep='\\t' becomes one wide column."""
    if df.shape[1] != 1:
        return False
    name = str(df.columns[0])
    return "," in name


def _header_line_after_skiprows(raw: str, skiprows: int) -> Optional[str]:
    """The physical line pandas uses as column header when skiprows=int."""
    lines = raw.splitlines()
    if not lines or skiprows < 0 or skiprows >= len(lines):
        return None
    return lines[skiprows]


def _likely_tab_delimited_header(line: str) -> bool:
    """Only treat as TSV when tabs clearly separate fields (avoids _csv.Error on comma+quotes)."""
    if not line.strip():
        return False
    tabs = line.count("\t")
    commas = line.count(",")
    return tabs >= 2 and tabs > commas


# pandas ParserError does not always wrap the stdlib csv module errors from the python engine.
_CSV_READ_ERRORS = (pd.errors.ParserError, pd.errors.EmptyDataError, csv.Error)


def _read_flex_csv_with_skiprows(raw: str, skiprows: int) -> Optional[pd.DataFrame]:
    """
    Parse one Flex table for a given header offset.

    IBKR Flex CSV is usually comma-separated with preamble rows; some reports are
    tab-separated or contain commas inside unquoted fields, which breaks the C parser
    (ParserError: expected N fields, saw M). Try python engine, then skip bad lines.

    Tab-separated attempts run only when the header line after ``skiprows`` has more
    tabs than commas; otherwise parsing with a tab separator can raise ``csv.Error`` on
    valid comma-separated Flex text that uses quotes.
    """
    hdr = _header_line_after_skiprows(raw, skiprows) or ""
    use_tab = _likely_tab_delimited_header(hdr)

    if use_tab:
        variants: list[Dict[str, Any]] = [
            {"sep": "\t"},
            {"sep": "\t", "engine": "python"},
            {},
            {"engine": "python"},
        ]
    else:
        variants = [
            {},
            {"engine": "python"},
        ]

    last_err: Optional[Exception] = None
    for extra in variants:
        try:
            df = _read_csv_flex_string(raw, skiprows, **extra)
            if df.shape[1] > 0 and not _df_looks_like_misread_as_tsv(df):
                return df
        except _CSV_READ_ERRORS as exc:
            last_err = exc
            continue

    lenient_bases: list[Dict[str, Any]] = [{"engine": "python"}]
    if use_tab:
        lenient_bases.append({"engine": "python", "sep": "\t"})

    for base in lenient_bases:
        df = None
        kw = dict(base)
        try:
            df = _read_csv_flex_string(raw, skiprows, on_bad_lines="skip", **kw)
        except TypeError:
            try:
                df = _read_csv_flex_string(
                    raw,
                    skiprows,
                    error_bad_lines=False,
                    warn_bad_lines=False,
                    **kw,
                )
            except (TypeError, *_CSV_READ_ERRORS) as exc:
                last_err = exc
                continue
        except _CSV_READ_ERRORS as exc:
            last_err = exc
            continue
        if df is not None and df.shape[1] > 0 and not _df_looks_like_misread_as_tsv(df):
            return df

    if last_err is not None:
        raise last_err
    return None


def read_flex_csv_body(csv_text: str) -> pd.DataFrame:
    """Parse Flex CSV; honor IBKR_FLEX_CSV_SKIP_ROWS, else try common header row offsets."""
    raw = (csv_text or "").strip()
    if not raw:
        return pd.DataFrame()

    skip_env = os.getenv("IBKR_FLEX_CSV_SKIP_ROWS", "").strip()
    if skip_env:
        try:
            sk = int(skip_env)
            df = _read_flex_csv_with_skiprows(raw, sk)
            if df is not None:
                return df
        except (ValueError, *_CSV_READ_ERRORS):
            pass

    skips = [7, 0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12]
    last_err: Optional[Exception] = None
    for sk in skips:
        try:
            df = _read_flex_csv_with_skiprows(raw, sk)
            if df is not None:
                return df
        except _CSV_READ_ERRORS as exc:
            last_err = exc
            continue
    preview = raw[:500].replace("\n", "\\n")
    raise ValueError(
        "Could not parse Flex body as CSV (no columns after skiprows attempts). "
        "Set IBKR_FLEX_CSV_SKIP_ROWS to your report's header row count. "
        f"Tried {skips}. Preview: {preview!r}"
    ) from last_err


def _flex_column_norms(df: pd.DataFrame) -> set[str]:
    return {
        re.sub(r"[^a-z0-9]+", "", str(c).strip().lstrip("\ufeff").lower()) for c in df.columns
    }


def _df_looks_like_open_positions_table(df: pd.DataFrame) -> bool:
    """
    True if parsed headers look like IBKR Open Positions (not the Trades/Executions block).

    Activity Flex CSVs that include both Trades and Open Positions often parse as Trades first
    at the default skiprows=7; we scan other offsets to find the positions table.
    """
    cols = _flex_column_norms(df)
    pos_markers = {
        "markprice",
        "positionvalue",
        "fifounrealizedpnl",
        "costbasismoney",
        "costbasisprice",
        "percentofnav",
    }
    if cols & pos_markers:
        return True
    has_sym = "symbol" in cols
    has_qty = bool({"quantity", "position", "qty"} & cols)
    if not (has_sym and has_qty):
        return False
    has_dt = bool({"tradedate", "datetime", "trade date"} & cols)
    has_side = bool({"buysell", "tradeid", "trade id"} & cols)
    if has_dt and has_side:
        return False
    return True


def read_flex_csv_body_for_positions(csv_text: str) -> pd.DataFrame:
    """
    Parse Flex CSV preferring the **Open Positions** table when the file also contains Trades.

    Falls back to :func:`read_flex_csv_body` if no table matches the positions heuristic.
    """
    raw = (csv_text or "").strip()
    if not raw:
        return pd.DataFrame()

    skip_env = os.getenv("IBKR_FLEX_CSV_SKIP_ROWS", "").strip()
    skips_first: list[int] = []
    if skip_env:
        try:
            skips_first.append(int(skip_env))
        except ValueError:
            pass
    # Wider search than default: combined Activity reports push Open Positions below Trades.
    skips_rest = list(range(0, 25)) + [7, 25, 26, 27, 28, 29, 30, 35, 40, 45, 50]
    skips: list[int] = []
    for s in skips_first + skips_rest:
        if s not in skips:
            skips.append(s)

    candidates: list[tuple[int, pd.DataFrame]] = []
    last_err: Optional[Exception] = None
    for sk in skips:
        try:
            df = _read_flex_csv_with_skiprows(raw, sk)
            if df is None or df.shape[1] == 0 or len(df) == 0:
                continue
            if _df_looks_like_open_positions_table(df):
                candidates.append((sk, df))
        except _CSV_READ_ERRORS as exc:
            last_err = exc
            continue

    if candidates:
        best_sk, best_df = max(candidates, key=lambda t: (len(t[1]), -t[0]))
        print(
            f"[Flex] positions: chose skiprows={best_sk} (Open Positions–like header), "
            f"{len(best_df)} row(s), {len(best_df.columns)} column(s)."
        )
        return best_df

    print(
        "[Flex] positions: no Open Positions–like table across skiprows; "
        "using default Flex CSV parse (may be Trades-only if Open Positions is missing)."
    )
    return read_flex_csv_body(raw)


def _df_looks_like_flex_trades_table(df: pd.DataFrame) -> bool:
    """
    True if column names look like IBKR **Trades / Executions** (not a mis-parsed data row).

    When skiprows lands inside the CSV preamble or on a data row, pandas invents headers like
    ``DATA``, ``TRNT``, or ticker symbols — this rejects those parses.
    """
    if df is None or df.empty or df.shape[1] < 4:
        return False
    cols = _flex_column_norms(df)
    has_dt = bool(
        cols
        & {
            "tradedate",
            "datetime",
            "tradetime",
            "executiontime",
            "exectime",
            "date",
        }
    )
    has_sym = bool(cols & {"symbol", "underlyingsymbol"})
    has_side = bool(cols & {"buysell", "side"})
    has_qty = bool(cols & {"quantity", "qty"})
    return has_dt and has_sym and has_side and has_qty


def read_flex_csv_body_for_trades(csv_text: str) -> pd.DataFrame:
    """
    Parse Flex CSV preferring the **Trades** table header row (correct skiprows).

    Falls back to :func:`read_flex_csv_body` if no candidate matches.
    """
    raw = (csv_text or "").strip()
    if not raw:
        return pd.DataFrame()

    skip_env = os.getenv("IBKR_FLEX_CSV_SKIP_ROWS", "").strip()
    skips_first: list[int] = []
    if skip_env:
        try:
            skips_first.append(int(skip_env))
        except ValueError:
            pass
    skips_rest = list(range(0, 35)) + [7, 40, 45, 50, 55, 60]
    skips: list[int] = []
    for s in skips_first + skips_rest:
        if s not in skips:
            skips.append(s)

    candidates: list[tuple[int, pd.DataFrame]] = []
    last_err: Optional[Exception] = None
    for sk in skips:
        try:
            df = _read_flex_csv_with_skiprows(raw, sk)
            if df is None or df.shape[1] == 0 or len(df) == 0:
                continue
            if _df_looks_like_flex_trades_table(df):
                candidates.append((sk, df))
        except _CSV_READ_ERRORS as exc:
            last_err = exc
            continue

    if candidates:
        best_sk, best_df = max(candidates, key=lambda t: (len(t[1]), -t[0]))
        print(
            f"[Flex] trades: chose skiprows={best_sk} (Trades-like header), "
            f"{len(best_df)} row(s), {len(best_df.columns)} column(s)."
        )
        return best_df

    print(
        "[Flex] trades: no Trades-like table across skiprows; "
        "using default Flex CSV parse. Set IBKR_FLEX_CSV_SKIP_ROWS if headers are still wrong."
    )
    return read_flex_csv_body(raw)


def _is_generation_in_progress(xml_body: str) -> bool:
    """IBKR returns Warn + 1019 while the statement is still being built."""
    code = (_extract_error_code(xml_body) or "").strip()
    if code == "1019":
        return True
    low = xml_body.lower()
    if "generation in progress" in low or "please try again shortly" in low:
        return True
    return False


def _get_statement_body(
    get_url: str,
    token: str,
    v: str,
    reference_code: str,
    timeout: int,
) -> str:
    """
    Poll GetStatement until CSV is returned.

    Per IBKR Flex Web Service v3 documentation, GetStatement uses query param **q**
    for the **ReferenceCode** from SendRequest (not the Flex Query ID). CSV vs XML in
    Portal only affects the delivered file body once generation completes; 1019
    "generation in progress" is normal until the server finishes building that instance.

    Legacy fallbacks try ``ref`` / ``r`` if some environments differ.
    """
    ref_code = (reference_code or "").strip()
    if not ref_code:
        raise ValueError("Empty ReferenceCode for GetStatement")

    max_attempts = int(os.getenv("IBKR_FLEX_GETSTATEMENT_RETRIES", "35"))
    sleep_s = float(os.getenv("IBKR_FLEX_GETSTATEMENT_SLEEP", "1.5"))
    sleep_pending = float(
        os.getenv("IBKR_FLEX_GETSTATEMENT_SLEEP_INPROGRESS", "4.0")
    )
    rate_sleep = float(os.getenv("IBKR_FLEX_RATE_LIMIT_SLEEP", "15.0"))

    def _param_variants() -> list:
        # Official v3: https://www.interactivebrokers.com/campus/ibkr-api-page/flex-web-service/
        # GetStatement: t, q=ReferenceCode, v
        return [
            {"t": token, "v": v, "q": ref_code},
            {"t": token, "v": v, "ref": ref_code},
            {"t": token, "v": v, "r": ref_code},
        ]

    last_fail_snippet = ""
    hdrs = _flex_headers()
    for attempt in range(max(1, max_attempts)):
        got_fail = False
        pending = False
        for pvars in _param_variants():
            r2 = requests.get(get_url, params=pvars, headers=hdrs, timeout=timeout)
            r2.raise_for_status()
            body = (r2.text or "").strip()
            if not body:
                continue
            if not _looks_like_xml(body):
                return body

            last_fail_snippet = body[:900]

            if _is_generation_in_progress(body):
                got_fail = True
                pending = True
                break

            code = (_extract_error_code(body) or "").strip()
            if re.search(r"<Status>\s*Fail\s*</Status>", body, re.I):
                err = _extract_error_message(body) or ""
                if code in ("1015", "1017"):
                    raise ValueError(
                        f"GetStatement rejected (ErrorCode={code}): {err}. "
                        "Check Flex token and reference code."
                    )
                got_fail = True
                continue

            got_fail = True
            continue

        if got_fail and attempt < max_attempts - 1:
            code = (_extract_error_code(last_fail_snippet) or "").strip()
            if code == "1018":
                time.sleep(rate_sleep)
            elif pending:
                time.sleep(sleep_pending)
            else:
                time.sleep(sleep_s)
            continue
        if got_fail:
            code = _extract_error_code(last_fail_snippet) or "?"
            err = _extract_error_message(last_fail_snippet) or ""
            hint = ""
            if code == "1019" or "generation in progress" in (err + last_fail_snippet).lower():
                hint = (
                    " IBKR was still generating the statement; raise "
                    "IBKR_FLEX_GETSTATEMENT_RETRIES and/or IBKR_FLEX_GETSTATEMENT_SLEEP_INPROGRESS."
                )
            raise ValueError(
                f"GetStatement failed after {max_attempts} attempts (last ErrorCode={code}): {err}."
                " Confirm Flex Web Service is enabled, token matches SendRequest, and "
                "IBKR_FLEX_SEND_REQUEST_URL matches where you created the token."
                + hint
                + f" Snippet: {last_fail_snippet}"
            )

    raise ValueError(
        f"GetStatement did not return CSV after {max_attempts} attempts. Last: {last_fail_snippet[:600]}"
    )


def _flex_segment_label(params: dict) -> str:
    fd = params.get("fd")
    td = params.get("td")
    if fd is not None and td is not None:
        return f"fd={fd} td={td}"
    return "no fd/td (snapshot query)"


def _extract_ending_cash_usd(csv_text: str) -> float:
    """
    Scan a multi-section Flex CSV for an EndingCash value.

    Previous approach (skiprows 0..39) failed when the Cash Report section appears
    after a large Open Positions block (which can be hundreds of lines).

    This version does a direct line-by-line text scan:
    1. Find any line that contains "EndingCash" as a column header token.
    2. Parse the column index and the accompanying Currency column index.
    3. Walk the following data lines looking for a USD row with a non-zero value.

    To enable cash reporting: in IBKR Flex Query editor add the **Cash Report** section
    and include the **EndingCash** and **Currency** fields.
    """
    _n = lambda s: re.sub(r"[^a-z0-9]+", "", str(s).lower())

    lines = csv_text.splitlines()
    for hdr_idx, line in enumerate(lines):
        # Quick pre-check to avoid csv-parsing every line
        if "endingcash" not in line.lower() and "ending cash" not in line.lower():
            continue

        # Parse this line as CSV to find column positions
        import csv as _csv
        try:
            header_parts = next(_csv.reader([line]))
        except Exception:
            continue

        ec_idx = next(
            (j for j, h in enumerate(header_parts) if _n(h) in ("endingcash", "endingcash1")),
            None,
        )
        if ec_idx is None:
            continue

        cur_idx = next(
            (j for j, h in enumerate(header_parts) if _n(h) in ("currency", "currencyprimary")),
            None,
        )

        # Scan the following data lines (up to 30) for a matching USD row
        for data_line in lines[hdr_idx + 1 : hdr_idx + 31]:
            if not data_line.strip():
                continue
            try:
                parts = next(_csv.reader([data_line]))
            except Exception:
                continue
            if len(parts) <= ec_idx:
                break
            # Currency filter
            if cur_idx is not None and len(parts) > cur_idx:
                if parts[cur_idx].strip().upper() not in ("USD", "BASE", ""):
                    continue
            try:
                val = float(parts[ec_idx])
            except (ValueError, IndexError):
                continue
            if abs(val) > 0:
                print(f"[Flex] Cash Report section found: EndingCash (USD) = {val:,.2f}")
                return val

    return 0.0


def _inject_cash_row(df: pd.DataFrame, cash: float) -> pd.DataFrame:
    """Append a synthetic CASH row so _sum_cash_from_flex can detect it."""
    cash_row = pd.DataFrame([{"Symbol": "CASH", "Quantity": cash, "AssetClass": "CASH"}])
    return pd.concat([df, cash_row], ignore_index=True)


def fetch_flex_report_dataframe(
    params: dict,
    token: str,
    send_request_url: Optional[str] = None,
    timeout: int = 60,
    *,
    parse_for_open_positions: bool = False,
    parse_for_trades: bool = False,
) -> pd.DataFrame:
    """
    GET SendRequest with query params; if response is Flex XML with ReferenceCode,
    GET GetStatement for the CSV. Otherwise parse response as CSV.

    SendRequest may return Fail **1003** (statement not available) for a date segment
    with no activity — callers treat that as an empty segment and continue.

    **1018** (too many requests for this token) is retried with a longer pause; see
    https://www.interactivebrokers.com/campus/ibkr-api-page/flex-web-service/ (rate limits).

    When ``parse_for_open_positions`` is True, the CSV body is scanned for a table whose
    headers look like **Open Positions** (useful when the same Activity template includes Trades
    above Positions in one file).

    When ``parse_for_trades`` is True, the CSV body is scanned for a **Trades** header row
    (fixes wrong ``skiprows`` where the first data row becomes column names).
    """
    if parse_for_open_positions and parse_for_trades:
        raise ValueError("Use only one of parse_for_open_positions or parse_for_trades.")
    if parse_for_open_positions:
        csv_parser = read_flex_csv_body_for_positions
    elif parse_for_trades:
        csv_parser = read_flex_csv_body_for_trades
    else:
        csv_parser = read_flex_csv_body
    url = (send_request_url or flex_send_request_url()).strip()
    hdrs = _flex_headers()
    transient_send = {"1001", "1004", "1009", "1021"}
    rate_limit_send = {"1018"}
    max_send = max(1, int(os.getenv("IBKR_FLEX_SENDREQUEST_RETRIES", "8")))
    send_sleep = float(os.getenv("IBKR_FLEX_SENDREQUEST_SLEEP", "2.0"))
    rate_sleep = float(os.getenv("IBKR_FLEX_RATE_LIMIT_SLEEP", "15.0"))

    body = ""
    for attempt in range(max_send):
        resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
        resp.raise_for_status()
        body = (resp.text or "").strip()
        if not body:
            print(f"[Flex] {_flex_segment_label(params)}: empty HTTP body from SendRequest.")
            return pd.DataFrame()

        if not _looks_like_xml(body):
            df0 = csv_parser(body)
            kind = (
                "positions-aware"
                if parse_for_open_positions
                else ("trades-aware" if parse_for_trades else "default")
            )
            print(
                f"[Flex] {_flex_segment_label(params)}: direct CSV from SendRequest ({kind}), "
                f"{len(df0)} row(s), {len(df0.columns)} column(s)."
            )
            if parse_for_open_positions:
                cash = _extract_ending_cash_usd(body)
                if abs(cash) > 0:
                    df0 = _inject_cash_row(df0, cash)
            return df0

        err = _extract_error_message(body)
        code = (_extract_error_code(body) or "").strip()

        if re.search(r"<Status>\s*Fail\s*</Status>", body, re.I):
            if code == "1003":
                if params.get("fd") is not None and params.get("td") is not None:
                    print(
                        f"[Flex] {_flex_segment_label(params)}: SendRequest ErrorCode 1003 "
                        "(no statement / no activity in this date range per IBKR). "
                        "Not a missing Symbol column. Manual Portal export may use a wider fd/td range "
                        "than incremental trade sync."
                    )
                else:
                    print(
                        f"[Flex] {_flex_segment_label(params)}: SendRequest ErrorCode 1003 "
                        "(no statement for this snapshot request). Check the Flex template **Period**, "
                        "account selection, and that the query includes **Open Positions** if you expect rows."
                    )
                return pd.DataFrame()
            if code in rate_limit_send and attempt < max_send - 1:
                time.sleep(rate_sleep)
                continue
            if code in transient_send and attempt < max_send - 1:
                time.sleep(send_sleep)
                continue
            parts = []
            if code:
                parts.append(f"ErrorCode={code}")
            if err:
                parts.append(err)
            raise ValueError(
                "Flex SendRequest failed: "
                + ("; ".join(parts) if parts else body[:700])
            )

        ref = _extract_reference_code(body)
        if not ref:
            raise ValueError(
                "Flex returned XML without ReferenceCode. "
                + (f"ErrorMessage: {err}. " if err else "")
                + f"Snippet: {body[:900]}"
            )
        get_url = send_request_to_get_statement_url(url)
        v = str(params.get("v", "3"))
        body = _get_statement_body(
            get_url,
            token,
            v,
            ref,
            timeout=timeout + 30,
        )
        if not body:
            print(f"[Flex] {_flex_segment_label(params)}: GetStatement returned empty body.")
            return pd.DataFrame()

        df_out = csv_parser(body)
        kind = (
            "positions-aware"
            if parse_for_open_positions
            else ("trades-aware" if parse_for_trades else "default")
        )
        print(
            f"[Flex] {_flex_segment_label(params)}: GetStatement CSV parsed ({kind}) to "
            f"{len(df_out)} row(s), {len(df_out.columns)} column(s)."
        )
        if parse_for_open_positions:
            cash = _extract_ending_cash_usd(body)
            if abs(cash) > 0:
                df_out = _inject_cash_row(df_out, cash)
        if len(df_out.columns) > 0 and len(df_out) == 0:
            print(
                "[Flex] Hint: header-only CSV usually means zero executions in this fd/td window "
                "(same as 1003 from a reporting perspective)."
            )
        return df_out
