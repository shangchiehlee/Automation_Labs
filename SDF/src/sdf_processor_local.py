# -*- coding: utf-8 -*-
"""
Program Overview: Staff Development Fund (SDF) Reconciliation Engine
Author: Shane Lee

System Function:
    Executes financial reconciliation between transaction logs and staff reference data.
    Generates individual financial statements (.xlsx), email artifacts (.eml), and
    summary reports.

Architectural Pattern:
    Implements a hybrid memory and disk processing model using a temporary SQLite buffer
    for transaction storage and SQL queries for joins and aggregations.

    1. Ingestion: Streams transaction Excel data via OpenPyXL generators into SQLite.
       Reference data is loaded with pandas and written to SQLite.
    2. Processing: Executes joins and aggregations via SQL queries. Creates an index on
       transactions(TR_Code) and defines TR_Code as the reference primary key.
    3. Output: Encapsulates all generated artifacts and execution logs within a
       single compressed ZIP archive.
"""

import gc
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime
from email.generator import BytesGenerator
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Generator, Iterator, List, Optional, Tuple, Union

import openpyxl
import pandas as pd


# --- TYPES & CONSTANTS ---

ConfigDict = Any  # Type alias for parsed JSON configuration
PathLike = Union[str, Path]


# --- CONFIGURATION & LOGGING ---

def load_config(config_path: Path) -> ConfigDict:
    """Loads and returns the JSON configuration."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def setup_logging(log_file: Optional[Path] = None) -> None:
    """
    Configures logging. If log_file is provided, it enables file output.

    Args:
        log_file: Optional path to the execution log.
    """
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode='w', encoding='utf-8'))

    # Reset existing handlers if any and close them (important on Windows)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        try:
            handler.flush()
            handler.close()
        except Exception:
            pass

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


# --- UTILITIES ---

class DataQualityAuditor:
    """
    Provides structured logging for data quality anomalies.
    Ensures traceability of ingestion errors to specific source file coordinates.
    """
    
    @staticmethod
    def log_issue(batch_id: int, row_idx: int, column: str, value: Any, issue: str) -> None:
        """
        Logs a data anomaly with coordinate context.

        Args:
            batch_id: The ingestion chunk sequence number.
            row_idx: The row index within the current chunk.
            column: The column name exhibiting the issue.
            value: The problematic value.
            issue: Description of the validation failure.
        """
        logging.warning(
            f"DATA ISSUE [Batch {batch_id}, Row {row_idx}]: "
            f"Col '{column}' = '{value}'. Issue: {issue}"
        )


class DataHealthScorecard:
    """
    Accumulator for data retention metrics.
    Tracks valid vs. invalid rows to quantify data quality.
    """
    
    def __init__(self) -> None:
        self._input_rows: int = 0
        self._valid_rows: int = 0
    
    def add_input(self, count: int) -> None:
        """Increment total input row count."""
        self._input_rows += count
        
    def add_valid(self, count: int) -> None:
        """Increment valid (ingested) row count."""
        self._valid_rows += count
        
    def log_summary(self) -> None:
        """Emits a summary of data retention to the log."""
        retention = 0.0
        if self._input_rows > 0:
            retention = (self._valid_rows / self._input_rows) * 100.0
            
        logging.info("=" * 40)
        logging.info("--- DATA HEALTH SCORECARD ---")
        logging.info(f"Total Input Rows:    {self._input_rows:,}")
        logging.info(f"Total Valid Rows:    {self._valid_rows:,}")
        logging.info(f"Retention Rate:      {retention:.2f}%")
        logging.info("=" * 40)


scorecard = DataHealthScorecard()


# --- SQLITE BUFFER MANAGER ---

class SQLiteBuffer:
    """
    Manages a temporary disk-based relational store to extend processing capacity
    beyond available RAM.

    Implements the Context Manager pattern for resource management.
    """

    def __init__(self, db_path: PathLike) -> None:
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self) -> 'SQLiteBuffer':
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;") 
        self.cursor = self.conn.cursor()
        self._init_schema()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.conn:
            self.conn.close()

    def _init_schema(self) -> None:
        """Defines the SQL schema for Reference and Transaction data."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS reference (
                TR_Code TEXT PRIMARY KEY,
                Staff_ID TEXT,
                First_Name TEXT,
                Family_Name TEXT,
                UTS_Email TEXT
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                TR_Code TEXT,
                Account_Name TEXT,
                Debit REAL,
                Credit REAL,
                Net_Amount REAL,
                Year INTEGER,
                Month TEXT,
                Description TEXT,
                Batch_ID INTEGER
            )
        """)
        
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_tr_code ON transactions(TR_Code)")
        self.conn.commit()

    def _tabular_chunk_generator(self, file_path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
        """
        Yields DataFrame chunks from a supported tabular file.

        Supported input formats:
        - Excel (.xlsx, .xlsm) via OpenPyXL streaming
        - CSV (.csv) via pandas chunked reader
        """
        suffix = file_path.suffix.lower()

        if suffix in {'.xlsx', '.xlsm'}:
            wb = openpyxl.load_workbook(filename=file_path, read_only=True, data_only=True)
            try:
                ws = wb.active
                rows_iter = ws.iter_rows(values_only=True)

                try:
                    headers = next(rows_iter)
                except StopIteration:
                    return

                chunk = []
                for row in rows_iter:
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        yield pd.DataFrame(chunk, columns=headers)
                        chunk = []

                if chunk:
                    yield pd.DataFrame(chunk, columns=headers)
            finally:
                wb.close()
            return

        if suffix == '.csv':
            try:
                csv_iter = pd.read_csv(
                    file_path,
                    chunksize=chunk_size,
                    encoding='utf-8-sig',
                    low_memory=False
                )
            except UnicodeDecodeError:
                csv_iter = pd.read_csv(
                    file_path,
                    chunksize=chunk_size,
                    encoding='cp1252',
                    low_memory=False
                )

            for chunk_df in csv_iter:
                yield chunk_df
            return

        raise ValueError(f"Unsupported input file format: {file_path.suffix}")

    # Backward-compatible alias retained in case other methods still call the old name.
    def _excel_chunk_generator(self, file_path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
        yield from self._tabular_chunk_generator(file_path, chunk_size)

    def _prepare_reference_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes and validates reference data before SQL insertion.
        """
        df = df.copy()
        df.columns = [str(c).replace(' ', '_') for c in df.columns]

        if 'TR_Code' not in df.columns:
            logging.critical("Reference data missing TR_Code column.")
            raise ValueError("Reference data missing TR_Code column.")

        tr_series = df['TR_Code']
        if tr_series.isna().any():
            logging.critical("Reference data contains missing TR_Code values.")
            raise ValueError("Reference data contains missing TR_Code values.")

        tr_series = tr_series.astype(str).str.strip()
        invalid_mask = tr_series.eq('') | tr_series.str.lower().eq('nan')
        if invalid_mask.any():
            logging.critical("Reference data contains empty TR_Code values.")
            raise ValueError("Reference data contains empty TR_Code values.")

        df['TR_Code'] = tr_series.str.zfill(7)

        if df['TR_Code'].duplicated().any():
            logging.critical("Reference data contains duplicate TR_Code values.")
            raise ValueError("Reference data contains duplicate TR_Code values.")

        if 'Staff_ID' in df.columns:
            df['Staff_ID'] = df['Staff_ID'].astype(str).str.strip()

        target_cols = ['TR_Code', 'Staff_ID', 'First_Name', 'Family_Name', 'UTS_Email']
        for col in target_cols:
            if col not in df.columns:
                df[col] = None

        return df[target_cols]

    def ingest_reference(self, df: pd.DataFrame) -> None:
        """Loads reference data into SQL."""
        prepared = self._prepare_reference_frame(df)
        self.conn.execute("DELETE FROM reference")
        self.conn.commit()
        try:
            prepared.to_sql('reference', self.conn, if_exists='append', index=False)
        except sqlite3.IntegrityError as e:
            logging.critical("Reference data contains duplicate TR_Code values.")
            raise ValueError("Reference data contains duplicate TR_Code values.") from e
        logging.info(f"Reference Data Buffered: {len(prepared)} rows.")

    def ingest_reference_stream(self, file_path: Path, chunk_size: int) -> None:
        """Streams reference data into SQL."""
        logging.info(f"Ingesting reference data from {file_path.name}...")
        self.conn.execute("DELETE FROM reference")
        self.conn.commit()

        total_rows = 0
        for chunk in self._tabular_chunk_generator(file_path, chunk_size):
            prepared = self._prepare_reference_frame(chunk)
            try:
                prepared.to_sql('reference', self.conn, if_exists='append', index=False)
            except sqlite3.IntegrityError as e:
                logging.critical("Reference data contains duplicate TR_Code values.")
                raise ValueError("Reference data contains duplicate TR_Code values.") from e

            total_rows += len(prepared)

        logging.info(f"Reference Data Buffered: {total_rows} rows.")

    def ingest_transaction_stream(self, file_path: Path, chunk_size: int, dry_run_limit: int = 0) -> None:
        """
        Streams transaction data from source files into the SQLite buffer.
        """
        logging.info(f"Ingesting transactions from {file_path.name}...")
        
        total_rows = 0
        tr_pattern = re.compile(r"^0*(\d{7})")

        def extract_tr(val: Any) -> Optional[str]:
            s = str(val).strip()
            m = tr_pattern.match(s)
            return m.group(1) if m else None

        for batch_id, chunk in enumerate(self._tabular_chunk_generator(file_path, chunk_size)):
            if dry_run_limit > 0 and total_rows >= dry_run_limit:
                break

            scorecard.add_input(len(chunk))
            
            if 'Activity Level 3 Code and Name' in chunk.columns:
                chunk['TR_Code'] = chunk['Activity Level 3 Code and Name'].apply(extract_tr)
                chunk['TR_Code'] = chunk['TR_Code'].str.zfill(7)
            
            numeric_cols = ['debit', 'credit', 'net_amount']
            for col in numeric_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0.0)
                else:
                    chunk[col] = 0.0

            invalid_mask = chunk['TR_Code'].isna()
            if invalid_mask.any():
                bad_rows = chunk[invalid_mask]
                for idx, row in bad_rows.head(5).iterrows():
                    DataQualityAuditor.log_issue(
                        batch_id, idx, "TR_Code", 
                        row.get('Activity Level 3 Code and Name', 'N/A'), 
                        "Extraction Failed"
                    )
            
            valid_chunk = chunk[~invalid_mask].copy()
            
            column_map = {
                'Activity Level 3 Code and Name': 'Account_Name',
                'debit': 'Debit',
                'credit': 'Credit',
                'net_amount': 'Net_Amount',
                'journal_line_desc': 'Description'
            }
            valid_chunk = valid_chunk.rename(columns=column_map)
            
            target_schema = ['TR_Code', 'Account_Name', 'Debit', 'Credit', 'Net_Amount', 'Year', 'Month', 'Description']
            for c in target_schema:
                if c not in valid_chunk.columns: 
                    valid_chunk[c] = None
            
            valid_chunk['Batch_ID'] = batch_id
            
            valid_chunk[target_schema + ['Batch_ID']].to_sql(
                'transactions', 
                self.conn, 
                if_exists='append', 
                index=False,
                chunksize=chunk_size
            )
            
            count = len(valid_chunk)
            total_rows += count
            scorecard.add_valid(count)
            
            del chunk, valid_chunk
            gc.collect()

        self.conn.commit()
        logging.info(f"Ingestion Complete. Total Transactions: {total_rows}")

    def get_joined_data_iterator(self) -> Iterator[pd.DataFrame]:
        """Yields joined transaction data for individual staff members."""
        staff_query = """
            SELECT DISTINCT r.Staff_ID
            FROM reference r
            JOIN transactions t ON r.TR_Code = t.TR_Code
            ORDER BY r.Staff_ID
        """
        cursor = self.conn.cursor()
        cursor.execute(staff_query)
        for row in cursor:
            staff_id = row[0]
            txn_query = """
                SELECT t.*, r.First_Name, r.Family_Name, r.UTS_Email, r.Staff_ID
                FROM transactions t
                JOIN reference r ON t.TR_Code = r.TR_Code
                WHERE r.Staff_ID = ?
            """
            staff_txns = pd.read_sql(txn_query, self.conn, params=(staff_id,))
            yield staff_txns

    def export_transaction_ledger_csv(self, output_path: Path) -> None:
        """Streams the full transaction ledger to CSV."""
        import csv
        query = """
            SELECT 
                t.Batch_ID, t.TR_Code, t.Account_Name, t.Description, 
                t.Debit, t.Credit, t.Net_Amount, t.Year, t.Month,
                r.Staff_ID, r.First_Name, r.Family_Name, r.UTS_Email
            FROM transactions t
            JOIN reference r ON t.TR_Code = r.TR_Code
            WHERE r.Staff_ID IS NOT NULL 
              AND r.First_Name IS NOT NULL 
              AND r.Family_Name IS NOT NULL 
              AND r.UTS_Email IS NOT NULL
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([description[0] for description in cursor.description])
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                writer.writerows(rows)

    def get_spenders_summary(self) -> pd.DataFrame:
        """Aggregates spending by Staff Member."""
        query = """
            SELECT 
                r.Staff_ID, r.First_Name, r.Family_Name, r.UTS_Email,
                COUNT(t.Net_Amount) as Txn_Count,
                SUM(t.Debit) as Total_Debit,
                SUM(t.Credit) as Total_Credit,
                SUM(t.Net_Amount) as Total_Net_Spend
            FROM transactions t
            JOIN reference r ON t.TR_Code = r.TR_Code
            GROUP BY r.Staff_ID, r.First_Name, r.Family_Name, r.UTS_Email
            ORDER BY Total_Net_Spend DESC
        """
        return pd.read_sql(query, self.conn)

    def get_monthly_trends(self) -> pd.DataFrame:
        """Aggregates spending by Year and Month."""
        query = """
            SELECT Year, Month, COUNT(*) as Txn_Count, SUM(Net_Amount) as Total_Net_Spend
            FROM transactions
            GROUP BY Year, Month
            ORDER BY Year DESC, Month DESC
        """
        return pd.read_sql(query, self.conn)

    def get_account_summary(self) -> pd.DataFrame:
        """Aggregates spending by Account Name."""
        query = """
            SELECT Account_Name, SUM(Net_Amount) as Total_Net_Spend, COUNT(*) as Txn_Count
            FROM transactions
            GROUP BY Account_Name
            ORDER BY Total_Net_Spend DESC
        """
        return pd.read_sql(query, self.conn)


# --- REPORT GENERATOR ---

class ReportGenerator:
    """Executes the creation of reporting artifacts within a transient workspace."""
    
    EMAIL_BODY_TEMPLATE = """Hi {first_name},

Attached is a summary of your {tx_count} transaction(s).

Debit: ${debit:,.2f}
Credit: ${credit:,.2f}
Net Amount: ${net:,.2f}

This is an automated email based on transaction data.
Please review monthly for any discrepancies.
For general questions contact: nancy.chau@uts.edu.au
For financial queries contact: businessfacfinance@uts.edu.au

Kind regards,
Shane
"""

    def __init__(self, working_dir: Path, config: ConfigDict) -> None:
        self.working_dir = working_dir
        self.config = config

    def generate_csv_reports(self, buffer: SQLiteBuffer) -> None:
        """Generates the core CSV summary reports."""
        buffer.export_transaction_ledger_csv(self.working_dir / "SDF_Transaction_Ledger.csv")
        
        spenders_df = buffer.get_spenders_summary()
        spenders_df.to_csv(self.working_dir / "SDF_All_Spenders.csv", index=False)
        
        trends_df = buffer.get_monthly_trends()
        trends_df.to_csv(self.working_dir / "SDF_Monthly_Trends.csv", index=False)
        
        accounts_df = buffer.get_account_summary()
        accounts_df.to_csv(self.working_dir / "SDF_Spending_by_Account.csv", index=False)

    def generate_staff_statement(self, df: pd.DataFrame) -> None:
        """Creates Excel statement and EML file for a single staff member."""
        if df.empty:
            return

        first_name = df.iloc[0]['First_Name']
        family_name = df.iloc[0]['Family_Name']
        staff_id = df.iloc[0]['Staff_ID']
        email = df.iloc[0]['UTS_Email']

        tx_count = len(df)
        total_debit = df['Debit'].sum()
        total_credit = df['Credit'].sum()
        total_net = df['Net_Amount'].sum()

        safe_name = f"{str(first_name).strip()}_{str(family_name).strip()}"
        excel_filename = self.working_dir / f"SDF_Statement_{safe_name}_{staff_id}.xlsx"
        
        final_df = df.copy()
        final_df.loc[len(final_df)] = {
            'Description': 'TOTAL',
            'Debit': total_debit,
            'Credit': total_credit,
            'Net_Amount': total_net
        }
        final_df.to_excel(excel_filename, index=False)

        self._create_email_file(excel_filename, str(email), str(first_name).strip(), safe_name, tx_count, float(total_debit), float(total_credit), float(total_net))

    def _create_email_file(self, attachment_path: Path, recipient: str, first_name: str, safe_name: str, tx_count: int, debit: float, credit: float, net: float) -> None:
        """Generates a .eml file with the Excel statement attached."""
        msg = EmailMessage()
        msg['Subject'] = f"SDF Statement - {safe_name}"
        msg['From'] = self.config['settings']['email_sender']
        msg['To'] = recipient
        msg['Cc'] = self.config['settings']['email_cc']
        
        body = self.EMAIL_BODY_TEMPLATE.format(first_name=first_name, tx_count=tx_count, debit=debit, credit=credit, net=net)
        msg.set_content(body)

        with open(attachment_path, 'rb') as f:
            msg.add_attachment(f.read(), maintype='application', subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename=attachment_path.name)

        with open(self.working_dir / f"Email_{safe_name}.eml", 'wb') as f:
            BytesGenerator(f).flatten(msg)


class Archiver:
    """Compresses the workspace into a single ZIP deliverable."""
    
    @staticmethod
    def create_zip(source_dir: Path, output_path: Path) -> None:
        logging.info(f"Archiving workspace to {output_path.name}...")
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in source_dir.glob('*'):
                if file.is_file():
                    zipf.write(file, arcname=file.name)


# --- MAIN ORCHESTRATOR ---

def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    inputs_dir = base_dir / "inputs"
    outputs_dir = base_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    config = load_config(base_dir / "config.json")

    # Initial logging to stdout
    setup_logging()
    logging.info("--- SDF Processor (Streaming Edition) Started ---")

    def find_input_file(inputs_dir: Path, pattern: str) -> Path:
        supported_suffixes = ('.xlsx', '.xlsm', '.csv')
        candidates: List[Path] = []
        if inputs_dir.exists():
            for p in inputs_dir.iterdir():
                if not p.is_file():
                    continue
                if p.name.startswith('~$'):
                    continue
                if p.suffix.lower() not in supported_suffixes:
                    continue
                if pattern in p.name:
                    candidates.append(p)

        if not candidates:
            visible_files = []
            if inputs_dir.exists():
                visible_files = [
                    p.name for p in inputs_dir.iterdir()
                    if p.is_file() and not p.name.startswith('~$')
                ]
            raise FileNotFoundError(
                f"No supported input file found for pattern '{pattern}' in '{inputs_dir}'. "
                f"Supported types: {', '.join(supported_suffixes)}. "
                f"Visible files: {visible_files}"
            )

        # Prefer Excel if both Excel and CSV exist, then choose deterministically.
        ext_priority = {'.xlsx': 0, '.xlsm': 1, '.csv': 2}
        candidates.sort(key=lambda p: (ext_priority.get(p.suffix.lower(), 99), p.name.lower()))
        return candidates[0]

    try:
        ref_pattern = config['settings']['input_pattern_reference']
        tx_pattern = config['settings']['input_pattern_transaction']
        ref_file = find_input_file(inputs_dir, ref_pattern)
        tx_file = find_input_file(inputs_dir, tx_pattern)
        logging.info(f"Reference input selected: {ref_file.name}")
        logging.info(f"Transaction input selected: {tx_file.name}")
    except FileNotFoundError as e:
        logging.critical(str(e))
        sys.exit(1)

    exit_code = 0

    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)

        # Relocate execution log to temp workspace for inclusion in ZIP
        log_path = temp_dir / "sdf_processor_execution.log"
        setup_logging(log_path)

        # Create a temp DB path that is closed before sqlite opens it (Windows-safe)
        fd, db_name = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        db_path = Path(db_name)

        try:
            with SQLiteBuffer(db_path) as buffer:
                buffer.ingest_reference_stream(
                    ref_file,
                    config['performance_settings']['chunk_size']
                )
                gc.collect()

                buffer.ingest_transaction_stream(
                    tx_file,
                    config['performance_settings']['chunk_size'],
                    config['performance_settings']['dry_run_limit']
                )

                reporter = ReportGenerator(temp_dir, config)
                reporter.generate_csv_reports(buffer)

                # NOTE: Redundant Executive Summary generation removed as
                # SDF_Spending_by_Account.csv provides the same data in machine-readable format.

                for staff_df in buffer.get_joined_data_iterator():
                    reporter.generate_staff_statement(staff_df)
                    gc.collect()

                scorecard.log_summary()

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                zip_name = f"SDF_Output_Package_{timestamp}.zip"
                Archiver.create_zip(temp_dir, outputs_dir / zip_name)
                logging.info(f"Deliverable created: {zip_name}")

        except Exception as e:
            logging.exception(f"Processing Failed: {e}")
            exit_code = 1

        finally:
            # Ensure SQLite sidecar files are removed if WAL mode created them
            for p in [db_path, Path(str(db_path) + '-wal'), Path(str(db_path) + '-shm')]:
                try:
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass

            # Release temp_dir log file handle before TemporaryDirectory cleanup (Windows)
            logging.shutdown()

    if exit_code:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
