# -*- coding: utf-8 -*-
"""
Program Overview: Staff Development Fund (SDF) Output Validator
Author: Shane Lee

System Function:
    Validates SDF output packages as a standalone utility. Optionally executes
    the SDF pipeline and performs deterministic verification of generated
    artefacts.

Architectural Pattern:
    Implements a streaming validation workflow with optional pipeline execution.

    1. Execution: Optionally runs sdf_processor.py and collects new ZIP outputs.
    2. Extraction: Extracts each ZIP into a temporary directory for inspection.
    3. Verification: Streams CSV rows and statement worksheets to check ordering,
       TR_Code normalisation, and TOTAL row sums.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import openpyxl


ConfigDict = Dict[str, Any]

REQUIRED_CSVS: Tuple[str, ...] = (
    "SDF_Transaction_Ledger.csv",
    "SDF_All_Spenders.csv",
    "SDF_Monthly_Trends.csv",
    "SDF_Spending_by_Account.csv",
)

TR_CODE_PATTERN = re.compile(r"^\d{7}$")
MAX_ERRORS_PER_FILE = 5


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """
    Parses command line arguments.

    Inputs:
        argv: Optional sequence of arguments.
    Outputs:
        Parsed argparse namespace.
    Error conditions:
        argparse raises SystemExit on invalid arguments.
    Resource characteristics:
        In-memory parsing only.
    """
    parser = argparse.ArgumentParser(description="Validate SDF output packages.")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.json. Defaults to SDF/config.json.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory to validate. Defaults to SDF/outputs.",
    )
    parser.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Run the SDF pipeline before validation.",
    )
    return parser.parse_args(argv)


def load_config(path: Path) -> ConfigDict:
    """
    Loads a JSON configuration file.

    Inputs:
        path: Path to config.json.
    Outputs:
        Parsed configuration dictionary.
    Error conditions:
        Raises ValueError for invalid JSON or missing file.
    Resource characteristics:
        Entire file loaded into memory.
    """
    if not path.exists():
        raise ValueError(f"Config file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in config: {path}") from exc


def resolve_paths(args: argparse.Namespace) -> Tuple[Path, Path, Path]:
    """
    Resolves repository, SDF, and output directory paths.

    Inputs:
        args: Parsed command line arguments.
    Outputs:
        Tuple of repo_root, sdf_dir, output_dir paths.
    Error conditions:
        None.
    Resource characteristics:
        In-memory path resolution only.
    """
    script_path = Path(__file__).resolve()
    sdf_dir = script_path.parent.parent
    repo_root = sdf_dir.parent

    if args.run_pipeline and args.output_dir is None:
        output_dir = sdf_dir / "outputs" / "_sdf_validate"
    elif args.output_dir:
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = repo_root / output_dir
    else:
        output_dir = sdf_dir / "outputs"

    return repo_root, sdf_dir, output_dir


def find_input_file(inputs_dir: Path, pattern: str) -> Path:
    """
    Finds the first Excel input file matching a pattern.

    Inputs:
        inputs_dir: Directory containing input files.
        pattern: Substring pattern to match.
    Outputs:
        Path to the matching file.
    Error conditions:
        Raises ValueError if no match is found.
    Resource characteristics:
        Directory listing only.
    """
    matches = list(inputs_dir.glob(f"*{pattern}*.xlsx"))
    if not matches:
        raise ValueError(f"No input file matching pattern '{pattern}' in {inputs_dir}")
    return matches[0]


def preflight_inputs(sdf_dir: Path, config: ConfigDict) -> None:
    """
    Ensures reference and transaction inputs exist before pipeline execution.

    Inputs:
        sdf_dir: Base SDF directory.
        config: Parsed configuration.
    Outputs:
        None.
    Error conditions:
        Raises ValueError if required inputs are missing.
    Resource characteristics:
        Directory listing only.
    """
    inputs_dir = sdf_dir / "inputs"
    settings = config.get("settings", {})
    ref_pattern = settings.get("input_pattern_reference")
    tx_pattern = settings.get("input_pattern_transaction")
    if not ref_pattern or not tx_pattern:
        raise ValueError("Config missing input patterns for reference or transaction.")

    find_input_file(inputs_dir, str(ref_pattern))
    find_input_file(inputs_dir, str(tx_pattern))


def run_pipeline(repo_root: Path, sdf_dir: Path, config: ConfigDict, output_dir: Path) -> List[Path]:
    """
    Executes the SDF processor and copies newly generated ZIP outputs.

    Inputs:
        repo_root: Repository root path.
        sdf_dir: SDF module path.
        config: Parsed configuration.
        output_dir: Validation output directory for copied ZIPs.
    Outputs:
        List of ZIP paths copied into output_dir.
    Error conditions:
        Raises ValueError on execution failure or missing ZIPs.
    Resource characteristics:
        Runs an external Python process and copies ZIP files.
    """
    preflight_inputs(sdf_dir, config)
    processor_path = sdf_dir / "src" / "sdf_processor.py"
    outputs_dir = sdf_dir / "outputs"
    existing_zips = set(outputs_dir.glob("SDF_Output_Package_*.zip"))

    result = subprocess.run(
        [sys.executable, str(processor_path)],
        cwd=repo_root,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError("sdf_processor.py failed. Check its logs for details.")

    new_zips = sorted(set(outputs_dir.glob("SDF_Output_Package_*.zip")) - existing_zips)
    if not new_zips:
        raise ValueError("No new SDF_Output_Package_*.zip produced by the pipeline.")

    output_dir.mkdir(parents=True, exist_ok=True)
    copied: List[Path] = []
    for zip_path in new_zips:
        dest = output_dir / zip_path.name
        if dest.resolve() == zip_path.resolve():
            copied.append(zip_path)
            continue
        shutil.copy2(zip_path, dest)
        copied.append(dest)

    return copied


def record_error(
    errors: List[str],
    error_counts: Dict[str, int],
    key: str,
    message: str,
    limit: int = MAX_ERRORS_PER_FILE,
) -> None:
    """
    Records an error message with per key throttling.

    Inputs:
        errors: List of error messages.
        error_counts: Per key error counter.
        key: Throttling key, usually a filename.
        message: Error message to append.
        limit: Maximum messages per key.
    Outputs:
        None.
    Error conditions:
        None.
    Resource characteristics:
        In-memory list operations only.
    """
    count = error_counts.get(key, 0)
    if count < limit:
        errors.append(message)
    error_counts[key] = count + 1


def parse_decimal(value: Optional[str], file_key: str, row_num: int, column: str, errors: List[str], error_counts: Dict[str, int]) -> Optional[Decimal]:
    """
    Parses a string into a Decimal with error recording.

    Inputs:
        value: String value to parse.
        file_key: Filename for error context.
        row_num: CSV row number.
        column: Column name for error context.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        Decimal if parsing succeeds, otherwise None.
    Error conditions:
        InvalidOperation yields a recorded error and None result.
    Resource characteristics:
        In-memory parsing only.
    """
    if value is None:
        record_error(errors, error_counts, file_key, f"{file_key}: missing {column} at row {row_num}")
        return None

    raw = str(value).strip()
    if raw == "":
        record_error(errors, error_counts, file_key, f"{file_key}: missing {column} at row {row_num}")
        return None

    try:
        return Decimal(raw.replace(",", ""))
    except InvalidOperation:
        record_error(errors, error_counts, file_key, f"{file_key}: invalid {column} at row {row_num}: '{raw}'")
        return None


def validate_csv_file(csv_path: Path, errors: List[str], error_counts: Dict[str, int]) -> None:
    """
    Validates TR_Code normalisation and CSV sorting rules for a single file.

    Inputs:
        csv_path: Path to CSV file.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        None.
    Error conditions:
        Records issues for missing columns, invalid TR_Code, or sorting violations.
    Resource characteristics:
        Streams CSV rows without full materialisation.
    """
    file_key = csv_path.name
    sort_mode = None
    if csv_path.name == "SDF_All_Spenders.csv":
        sort_mode = "desc:Total_Net_Spend"
    elif csv_path.name == "SDF_Spending_by_Account.csv":
        sort_mode = "desc:Total_Net_Spend"
    elif csv_path.name == "SDF_Monthly_Trends.csv":
        sort_mode = "month"

    prev_value: Optional[Decimal] = None
    prev_year: Optional[int] = None
    prev_month: Optional[str] = None

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        if not headers:
            record_error(errors, error_counts, file_key, f"{file_key}: missing header row")
            return

        has_tr_code = "TR_Code" in headers

        if sort_mode == "desc:Total_Net_Spend" and "Total_Net_Spend" not in headers:
            record_error(errors, error_counts, file_key, f"{file_key}: missing Total_Net_Spend column")
            sort_mode = None
        elif sort_mode == "month":
            if "Year" not in headers or "Month" not in headers:
                record_error(errors, error_counts, file_key, f"{file_key}: missing Year or Month column")
                sort_mode = None

        for row_num, row in enumerate(reader, start=2):
            if has_tr_code:
                tr_value = str(row.get("TR_Code", "")).strip()
                if not TR_CODE_PATTERN.fullmatch(tr_value):
                    record_error(errors, error_counts, file_key, f"{file_key}: invalid TR_Code at row {row_num}: '{tr_value}'")

            if sort_mode == "desc:Total_Net_Spend":
                current = parse_decimal(row.get("Total_Net_Spend"), file_key, row_num, "Total_Net_Spend", errors, error_counts)
                if current is None:
                    continue
                if prev_value is not None and current > prev_value:
                    record_error(errors, error_counts, file_key, f"{file_key}: Total_Net_Spend not sorted DESC at row {row_num}")
                    break
                prev_value = current
            elif sort_mode == "month":
                year_val = parse_decimal(row.get("Year"), file_key, row_num, "Year", errors, error_counts)
                month_val = row.get("Month")
                if year_val is None:
                    continue
                if year_val != year_val.to_integral_value():
                    record_error(errors, error_counts, file_key, f"{file_key}: Year not an integer at row {row_num}")
                    continue
                if month_val is None or str(month_val).strip() == "":
                    record_error(errors, error_counts, file_key, f"{file_key}: missing Month at row {row_num}")
                    continue

                year_int = int(year_val)
                month_str = str(month_val).strip()

                if prev_year is not None:
                    if year_int > prev_year:
                        record_error(errors, error_counts, file_key, f"{file_key}: Year not sorted DESC at row {row_num}")
                        break
                    if year_int == prev_year and prev_month is not None and month_str > prev_month:
                        record_error(errors, error_counts, file_key, f"{file_key}: Month not sorted DESC at row {row_num}")
                        break

                prev_year = year_int
                prev_month = month_str


def to_decimal(value: Any, file_key: str, row_num: int, column: str, errors: List[str], error_counts: Dict[str, int]) -> Decimal:
    """
    Converts a value into Decimal with error recording.

    Inputs:
        value: Raw cell value.
        file_key: Filename for error context.
        row_num: Row number for error context.
        column: Column name for error context.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        Decimal value, defaulting to zero on errors.
    Error conditions:
        Records invalid numeric values and returns zero.
    Resource characteristics:
        In-memory parsing only.
    """
    if value is None or str(value).strip() == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except InvalidOperation:
        record_error(errors, error_counts, file_key, f"{file_key}: invalid {column} at row {row_num}")
        return Decimal("0")


def validate_statement_totals(statement_path: Path, errors: List[str], error_counts: Dict[str, int]) -> None:
    """
    Validates that the TOTAL row matches recomputed sums in a statement.

    Inputs:
        statement_path: Path to the statement workbook.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        None.
    Error conditions:
        Records missing columns, missing TOTAL row, or mismatched totals.
    Resource characteristics:
        Read-only streaming of worksheet rows.
    """
    file_key = statement_path.name
    try:
        wb = openpyxl.load_workbook(statement_path, data_only=True, read_only=True)
    except Exception as exc:
        record_error(errors, error_counts, file_key, f"{file_key}: failed to open statement: {exc}")
        return

    try:
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        try:
            headers = next(rows_iter)
        except StopIteration:
            record_error(errors, error_counts, file_key, f"{file_key}: empty worksheet")
            return

        header_map: Dict[str, int] = {}
        for idx, value in enumerate(headers):
            if value is None:
                continue
            header_map[str(value).strip()] = idx

        required_cols = ["Description", "Debit", "Credit", "Net_Amount"]
        missing_cols = [col for col in required_cols if col not in header_map]
        if missing_cols:
            record_error(errors, error_counts, file_key, f"{file_key}: missing columns {', '.join(missing_cols)}")
            return

        desc_idx = header_map["Description"]
        debit_idx = header_map["Debit"]
        credit_idx = header_map["Credit"]
        net_idx = header_map["Net_Amount"]

        sum_debit = Decimal("0")
        sum_credit = Decimal("0")
        sum_net = Decimal("0")
        total_row: Optional[Tuple[Any, ...]] = None

        for row_num, row in enumerate(rows_iter, start=2):
            if row is None or all(cell is None for cell in row):
                continue

            desc_val = row[desc_idx] if desc_idx < len(row) else None
            desc_str = str(desc_val).strip() if desc_val is not None else ""
            if desc_str == "TOTAL":
                if total_row is not None:
                    record_error(errors, error_counts, file_key, f"{file_key}: multiple TOTAL rows found")
                total_row = row
                continue

            sum_debit += to_decimal(row[debit_idx] if debit_idx < len(row) else None, file_key, row_num, "Debit", errors, error_counts)
            sum_credit += to_decimal(row[credit_idx] if credit_idx < len(row) else None, file_key, row_num, "Credit", errors, error_counts)
            sum_net += to_decimal(row[net_idx] if net_idx < len(row) else None, file_key, row_num, "Net_Amount", errors, error_counts)

        if total_row is None:
            record_error(errors, error_counts, file_key, f"{file_key}: TOTAL row not found")
            return

        total_debit = to_decimal(total_row[debit_idx] if debit_idx < len(total_row) else None, file_key, 0, "Debit", errors, error_counts)
        total_credit = to_decimal(total_row[credit_idx] if credit_idx < len(total_row) else None, file_key, 0, "Credit", errors, error_counts)
        total_net = to_decimal(total_row[net_idx] if net_idx < len(total_row) else None, file_key, 0, "Net_Amount", errors, error_counts)

        tolerance = Decimal("0.01")
        if abs(total_debit - sum_debit) > tolerance:
            record_error(errors, error_counts, file_key, f"{file_key}: TOTAL Debit does not match recomputed sum")
        if abs(total_credit - sum_credit) > tolerance:
            record_error(errors, error_counts, file_key, f"{file_key}: TOTAL Credit does not match recomputed sum")
        if abs(total_net - sum_net) > tolerance:
            record_error(errors, error_counts, file_key, f"{file_key}: TOTAL Net_Amount does not match recomputed sum")
    finally:
        wb.close()


def validate_zip_package(zip_path: Path, errors: List[str], error_counts: Dict[str, int]) -> None:
    """
    Validates output artefacts within a ZIP package.

    Inputs:
        zip_path: Path to the ZIP file.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        None.
    Error conditions:
        Records missing required files and validation failures.
    Resource characteristics:
        Extracts ZIP to a temporary directory.
    """
    if not zip_path.exists():
        record_error(errors, error_counts, zip_path.name, f"{zip_path.name}: ZIP file not found")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        try:
            with zipfile.ZipFile(zip_path, "r") as zipf:
                zipf.extractall(temp_path)
        except zipfile.BadZipFile:
            record_error(errors, error_counts, zip_path.name, f"{zip_path.name}: invalid ZIP file")
            return

        for csv_name in REQUIRED_CSVS:
            if not (temp_path / csv_name).exists():
                record_error(errors, error_counts, zip_path.name, f"{zip_path.name}: missing {csv_name}")

        csv_files = sorted(temp_path.glob("*.csv"))
        for csv_path in csv_files:
            validate_csv_file(csv_path, errors, error_counts)

        statement_files = sorted(temp_path.glob("SDF_Statement_*.xlsx"))
        for statement_path in statement_files:
            validate_statement_totals(statement_path, errors, error_counts)


def validate_zip_paths(zip_paths: List[Path], errors: List[str], error_counts: Dict[str, int]) -> int:
    """
    Validates a list of ZIP paths.

    Inputs:
        zip_paths: List of ZIP files to validate.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        Count of ZIPs processed.
    Error conditions:
        Records issues per ZIP file.
    Resource characteristics:
        Processes each ZIP independently.
    """
    if not zip_paths:
        record_error(errors, error_counts, "outputs", "No SDF_Output_Package_*.zip found for validation.")
        return 0

    for zip_path in zip_paths:
        validate_zip_package(zip_path, errors, error_counts)
    return len(zip_paths)


def validate_output_dir(output_dir: Path, errors: List[str], error_counts: Dict[str, int]) -> int:
    """
    Validates all SDF output ZIPs within a directory.

    Inputs:
        output_dir: Directory containing ZIP files.
        errors: Collected error messages.
        error_counts: Per file error counters.
    Outputs:
        Count of ZIPs processed.
    Error conditions:
        Records missing ZIP files or validation failures.
    Resource characteristics:
        Directory listing and per ZIP processing.
    """
    zip_paths = sorted(output_dir.glob("SDF_Output_Package_*.zip"))
    return validate_zip_paths(zip_paths, errors, error_counts)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Entry point for validation.

    Inputs:
        argv: Optional argument list.
    Outputs:
        Exit code 0 on success, non-zero on failure.
    Error conditions:
        Reports validation failures and subprocess execution errors.
    Resource characteristics:
        Executes validation using streaming checks.
    """
    args = parse_args(argv)
    repo_root, sdf_dir, output_dir = resolve_paths(args)

    default_config_path = sdf_dir / "config.json"
    config_path = Path(args.config) if args.config else default_config_path
    if not config_path.is_absolute():
        config_path = repo_root / config_path

    if args.run_pipeline and args.config:
        if config_path.resolve() != default_config_path.resolve():
            print("FAIL: --run-pipeline uses SDF/config.json. Custom config paths are not supported.")
            return 1

    errors: List[str] = []
    error_counts: Dict[str, int] = {}
    zip_paths: List[Path] = []

    if args.run_pipeline:
        try:
            config = load_config(config_path)
            zip_paths = run_pipeline(repo_root, sdf_dir, config, output_dir)
        except ValueError as exc:
            print(f"FAIL: {exc}")
            return 1
    else:
        if args.config:
            try:
                load_config(config_path)
            except ValueError as exc:
                print(f"FAIL: {exc}")
                return 1

    if zip_paths:
        processed = validate_zip_paths(zip_paths, errors, error_counts)
    else:
        processed = validate_output_dir(output_dir, errors, error_counts)

    if errors:
        print(f"FAIL: {len(errors)} issue(s) found across {processed} package(s).")
        for message in errors:
            print(f"- {message}")
        return 1

    print(f"PASS: {processed} package(s) validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
