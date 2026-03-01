import io
import sqlite3
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
TX_PATH = ROOT / "tx.py"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tx

SCHEMA_SQL = """
CREATE TABLE transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant TEXT NOT NULL,
    amount REAL NOT NULL,
    toronto_dt TEXT NOT NULL,
    ts_utc INTEGER NOT NULL,
    location_dtz TEXT NOT NULL,
    note TEXT,
    created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),
    updated_at_utc TEXT
)
"""


def init_test_db(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(SCHEMA_SQL)
        con.commit()
    finally:
        con.close()


def seed_transaction(
    db_path: Path,
    merchant: str,
    amount: float,
    toronto_dt: str,
    ts_utc: int,
    location_dtz: str,
) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO transactions (merchant, amount, toronto_dt, ts_utc, location_dtz, note)
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (merchant, amount, toronto_dt, ts_utc, location_dtz),
        )
        con.commit()
    finally:
        con.close()


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(TX_PATH), *args],
        capture_output=True,
        text=True,
        check=False,
    )


class TxPureFunctionTests(unittest.TestCase):
    def test_clean_amount_or_none(self) -> None:
        self.assertIsNone(tx.clean_amount_or_none(None))
        self.assertIsNone(tx.clean_amount_or_none("  "))
        self.assertEqual(tx.clean_amount_or_none("$1,234.50"), 1234.50)
        self.assertEqual(tx.clean_amount_or_none("  42 "), 42.0)
        self.assertIsNone(tx.clean_amount_or_none("not-a-number"))

    def test_iso_to_toronto_dt_and_ts_utc(self) -> None:
        toronto_dt, ts_utc = tx.iso_to_toronto_dt_and_ts_utc("2026-02-01T00:30:00-08:00")
        self.assertEqual(toronto_dt, "2026-02-01T03:30:00")
        self.assertEqual(ts_utc, 1769934600)

    def test_iso_to_toronto_dt_and_ts_utc_requires_timezone(self) -> None:
        err = io.StringIO()
        with redirect_stderr(err):
            with self.assertRaises(SystemExit) as cm:
                tx.iso_to_toronto_dt_and_ts_utc("2026-02-01T00:30:00")
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(
            err.getvalue(),
            "location_dtz must include timezone offset, e.g. 2026-02-01T00:30:00-08:00\n",
        )

    def test_row_formatters_exact_output(self) -> None:
        row = (
            1,
            "Coffee Shop",
            12.5,
            "2026-02-01T03:30:00",
            1769934600,
            "2026-02-01T00:30:00-08:00",
            None,
            "2026-02-01T08:30:00",
            None,
        )
        self.assertEqual(
            tx.row_to_csv_line(row),
            "1,Coffee Shop,12.50,2026-02-01T03:30:00,1769934600,2026-02-01T00:30:00-08:00,,2026-02-01T08:30:00,",
        )
        self.assertEqual(
            tx.row_to_pretty_csv_line(row), "1,12.50,Coffee Shop,2026-02-01 03:30:00"
        )

    def test_validate_insert_inputs_success_and_error(self) -> None:
        merchant, amount, location_dtz = tx.validate_insert_inputs(
            "  Coffee Shop  ", " $12.50 ", " 2026-02-01T00:30:00-08:00 "
        )
        self.assertEqual(merchant, "Coffee Shop")
        self.assertEqual(amount, 12.5)
        self.assertEqual(location_dtz, "2026-02-01T00:30:00-08:00")

        err = io.StringIO()
        with redirect_stderr(err):
            with self.assertRaises(SystemExit) as cm:
                tx.validate_insert_inputs("Coffee Shop", " ", "2026-02-01T00:30:00-08:00")
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(
            err.getvalue(), "ERROR: Missing values, merchant: Coffee Shop, amount: \n"
        )


class TxIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "test.db"
        init_test_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_insert_recent_pretty_and_delete(self) -> None:
        out = io.StringIO()
        with redirect_stdout(out):
            tx.insert_tx(
                str(self.db_path), "Coffee Shop", "$12.50", "2026-02-01T00:30:00-08:00"
            )
        self.assertEqual(out.getvalue(), "OK id=1\n")

        out = io.StringIO()
        with redirect_stdout(out):
            tx.read_recent(str(self.db_path), 1, pretty=True)
        self.assertEqual(out.getvalue(), "1,12.50,Coffee Shop,2026-02-01 03:30:00\n")

        out = io.StringIO()
        with redirect_stdout(out):
            tx.delete_by_id(str(self.db_path), 1)
        self.assertEqual(out.getvalue(), "OK deleted id=1\n")

        con = sqlite3.connect(self.db_path)
        try:
            remaining = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        finally:
            con.close()
        self.assertEqual(remaining, 0)

    def test_delete_missing_id_exits(self) -> None:
        err = io.StringIO()
        with redirect_stderr(err):
            with self.assertRaises(SystemExit) as cm:
                tx.delete_by_id(str(self.db_path), 999)
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(err.getvalue(), "No row found with id=999\n")

    def test_previous_month_total(self) -> None:
        seed_transaction(
            self.db_path,
            merchant="Rent",
            amount=10.0,
            toronto_dt="2026-02-01T00:00:00",
            ts_utc=100,
            location_dtz="2026-02-01T00:00:00-05:00",
        )
        seed_transaction(
            self.db_path,
            merchant="Groceries",
            amount=20.5,
            toronto_dt="2026-02-15T12:00:00",
            ts_utc=200,
            location_dtz="2026-02-15T12:00:00-05:00",
        )
        seed_transaction(
            self.db_path,
            merchant="Not Included",
            amount=99.0,
            toronto_dt="2026-03-01T00:00:00",
            ts_utc=300,
            location_dtz="2026-03-01T00:00:00-05:00",
        )

        out = io.StringIO()
        with redirect_stdout(out):
            tx.calculate_previous_month_total(
                str(self.db_path), "2026-03-01T00:00:00-05:00"
            )
        self.assertEqual(out.getvalue(), "30.50\n")

    def test_read_recent_non_positive_limit_prints_nothing(self) -> None:
        seed_transaction(
            self.db_path,
            merchant="Coffee Shop",
            amount=7.0,
            toronto_dt="2026-02-01T03:30:00",
            ts_utc=1769934600,
            location_dtz="2026-02-01T00:30:00-08:00",
        )
        out = io.StringIO()
        with redirect_stdout(out):
            tx.read_recent(str(self.db_path), 0, pretty=False)
        self.assertEqual(out.getvalue(), "")


class TxCliSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "test.db"
        init_test_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_unknown_command(self) -> None:
        result = run_cli("unknown-cmd", str(self.db_path))
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "Unknown command: unknown-cmd\n")

    def test_cli_insert_then_recent_pretty(self) -> None:
        insert_result = run_cli(
            "insert",
            str(self.db_path),
            "Coffee Shop",
            "$12.50",
            "2026-02-01T00:30:00-08:00",
        )
        self.assertEqual(insert_result.returncode, 0)
        self.assertEqual(insert_result.stdout, "OK id=1\n")
        self.assertEqual(insert_result.stderr, "")

        recent_result = run_cli("recent", str(self.db_path), "1", "--pretty")
        self.assertEqual(recent_result.returncode, 0)
        self.assertEqual(recent_result.stdout, "1,12.50,Coffee Shop,2026-02-01 03:30:00\n")
        self.assertEqual(recent_result.stderr, "")


if __name__ == "__main__":
    unittest.main()
