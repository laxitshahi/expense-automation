#!/usr/bin/env python3
import sqlite3
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

TORONTO_TZ = ZoneInfo("America/Toronto")

USAGE_TEXT = (
    "Usage:\n"
    "  tx.py read-all DB_PATH\n"
    "  tx.py recent DB_PATH [limit] [--pretty]\n"
    "  tx.py insert DB_PATH merchant amount location_dtz\n"
    "  tx.py delete DB_PATH id\n"
    "  tx.py prev-month-total DB_PATH anchor_datetime_with_offset\n"
)

SQL_SELECT_ALL = "SELECT * FROM transactions ORDER BY ts_utc DESC"
SQL_SELECT_RECENT = "SELECT * FROM transactions ORDER BY ts_utc DESC LIMIT ?"
SQL_INSERT_TX = """
        INSERT INTO transactions (merchant, amount, toronto_dt, ts_utc, location_dtz, note)
        VALUES (?, ?, ?, ?, ?, NULL)
        """
SQL_DELETE_BY_ID = "DELETE FROM transactions WHERE id = ?"
SQL_PREV_MONTH_START = """
        SELECT (strftime('%Y-%m', date(? || '-01', '-1 month')) || '-01T00:00:00')
        """
SQL_PREV_MONTH_TOTAL = """
        SELECT COALESCE(SUM(amount), 0)
        FROM transactions
        WHERE toronto_dt >= ?
          AND toronto_dt <  ?
        """


def connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def die(msg: str) -> None:
    print(msg, file=sys.stderr)
    sys.exit(2)


def iso_to_toronto_dt_and_ts_utc(location_dtz: str) -> tuple[str, int]:
    dt = datetime.fromisoformat(location_dtz)
    if dt.tzinfo is None:
        die("location_dtz must include timezone offset, e.g. 2026-02-01T00:30:00-08:00")

    ts_utc = int(dt.astimezone(timezone.utc).timestamp())
    toronto_dt = dt.astimezone(TORONTO_TZ).strftime("%Y-%m-%dT%H:%M:%S")
    return toronto_dt, ts_utc


def _unpack_row(row):
    (
        id_,
        merchant,
        amount,
        toronto_dt,
        ts_utc,
        location_dtz,
        note,
        created_at_utc,
        updated_at_utc,
    ) = row
    return (
        id_,
        merchant,
        amount,
        toronto_dt,
        ts_utc,
        location_dtz,
        note,
        created_at_utc,
        updated_at_utc,
    )


def row_to_csv_line(row) -> str:
    # Full row format (CSV-like, no escaping)
    (
        id_,
        merchant,
        amount,
        toronto_dt,
        ts_utc,
        location_dtz,
        note,
        created_at_utc,
        updated_at_utc,
    ) = _unpack_row(row)
    note_s = "" if note is None else str(note)
    updated_s = "" if updated_at_utc is None else str(updated_at_utc)

    return (
        f"{id_},"
        f"{merchant},"
        f"{float(amount):.2f},"
        f"{toronto_dt},"
        f"{ts_utc},"
        f"{location_dtz},"
        f"{note_s},"
        f"{created_at_utc},"
        f"{updated_s}"
    )


def row_to_pretty_csv_line(row) -> str:
    # Pretty format: id,amount,merchant,toronto_dt (no timezone)
    (
        id_,
        merchant,
        amount,
        toronto_dt,
        ts_utc,
        location_dtz,
        note,
        created_at_utc,
        updated_at_utc,
    ) = _unpack_row(row)
    toronto_pretty = toronto_dt.replace("T", " ")
    return f"{id_},{float(amount):.2f},{merchant},{toronto_pretty}"


def clean_amount_or_none(amount: str | None) -> float | None:
    if amount is None:
        return None
    s = amount.strip()
    if s == "":
        return None
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def validate_insert_inputs(
    merchant: str, amount: str, location_dtz: str
) -> tuple[str, float, str]:
    m = (merchant or "").strip()
    a = clean_amount_or_none(amount)
    iso = (location_dtz or "").strip()

    missing = []
    if m == "":
        missing.append("merchant")
    if a is None:
        missing.append("amount")
    if iso == "":
        missing.append("location_dtz")

    if missing:
        # amount: show if present raw, else blank (per your request)
        amt_display = (amount or "").strip()
        if amt_display == "":
            amt_display = ""
        return_error = (
            f"ERROR: Missing values, " f"merchant: {m}, " f"amount: {amt_display}"
        )
        # If you also want location_dtz echoed, uncomment:
        # return_error += f", location_dtz: {iso}"
        die(return_error)

    return m, a, iso


def read_all(db_path: str) -> None:
    con = connect(db_path)
    cur = con.cursor()
    cur.execute(SQL_SELECT_ALL)
    for row in cur.fetchall():
        print(row_to_csv_line(row))
    con.close()


def read_recent(db_path: str, limit: int, pretty: bool = False) -> None:
    if limit <= 0:
        return

    con = connect(db_path)
    cur = con.cursor()
    cur.execute(SQL_SELECT_RECENT, (limit,))
    rows = cur.fetchall()
    con.close()

    for row in rows:
        print(row_to_pretty_csv_line(row) if pretty else row_to_csv_line(row))


def insert_tx(db_path: str, merchant: str, amount: str, location_dtz: str) -> None:
    merchant, amount_f, location_dtz = validate_insert_inputs(
        merchant, amount, location_dtz
    )

    toronto_dt, ts_utc = iso_to_toronto_dt_and_ts_utc(location_dtz)

    con = connect(db_path)
    cur = con.cursor()
    cur.execute(SQL_INSERT_TX, (merchant, amount_f, toronto_dt, ts_utc, location_dtz))
    con.commit()
    new_id = cur.lastrowid
    con.close()
    print(f"OK id={new_id}")


def delete_by_id(db_path: str, tx_id: int) -> None:
    con = connect(db_path)
    cur = con.cursor()
    cur.execute(SQL_DELETE_BY_ID, (tx_id,))
    con.commit()
    deleted = cur.rowcount
    con.close()

    if deleted == 0:
        die(f"No row found with id={tx_id}")
    print(f"OK deleted id={tx_id}")


def calculate_previous_month_total(db_path: str, anchor_dt_with_offset: str) -> None:
    """
    Pass an anchor datetime with offset (ideally Toronto time at the start of a month).
    Prints the TOTAL spend for the previous Toronto month.
    """
    # Convert anchor -> Toronto wall time
    anchor_toronto_dt, _ = iso_to_toronto_dt_and_ts_utc(anchor_dt_with_offset)

    # Current month start boundary
    curr_month = anchor_toronto_dt[:7]  # YYYY-MM
    curr_start = f"{curr_month}-01T00:00:00"

    con = connect(db_path)
    cur = con.cursor()

    # Previous month start boundary (computed via SQLite month math)
    cur.execute(SQL_PREV_MONTH_START, (curr_month,))
    prev_start = cur.fetchone()[0]

    # Total for previous month
    cur.execute(SQL_PREV_MONTH_TOTAL, (prev_start, curr_start))
    total = cur.fetchone()[0]
    con.close()

    print(f"{float(total):.2f}")


def main(argv: list[str]) -> None:
    if len(argv) < 3:
        die(USAGE_TEXT)

    cmd = argv[1]
    db_path = argv[2]

    if cmd == "read-all":
        read_all(db_path)

    elif cmd == "recent":
        limit = 1
        pretty = False
        for a in argv[3:]:
            if a == "--pretty":
                pretty = True
            else:
                try:
                    limit = int(a)
                except ValueError:
                    die("Usage: tx.py recent DB_PATH [limit] [--pretty]")
        read_recent(db_path, limit, pretty)

    elif cmd == "insert":
        if len(argv) != 6:
            die("Usage: tx.py insert DB_PATH merchant amount location_dtz")
        insert_tx(db_path, argv[3], argv[4], argv[5])

    elif cmd == "delete":
        if len(argv) != 4:
            die("Usage: tx.py delete DB_PATH id")
        try:
            tx_id = int(argv[3])
        except ValueError:
            die("id must be an integer")
        delete_by_id(db_path, tx_id)

    elif cmd == "prev-month-total":
        if len(argv) != 4:
            die("Usage: tx.py prev-month-total DB_PATH anchor_datetime_with_offset")
        calculate_previous_month_total(db_path, argv[3])

    else:
        die(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main(sys.argv)
