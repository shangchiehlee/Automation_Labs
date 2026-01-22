# -*- coding: utf-8 -*-
"""
Program Overview: Staff Development Fund (SDF) Data Anonymizer
Author: Shane Lee

System Function:
    Executes deterministic PII sanitisation on tabular datasets. The program constructs a
    redaction dictionary from reference data and applies regex-based substitution to
    transaction logs.

Architectural Pattern:
    Implements a streaming pipeline for Excel inputs.
    
    1. Ingestion: Uses a generator to stream Excel data in chunks (implemented via
       OpenPyXL read_only iteration).
    2. Processing: Builds a compiled regular expression from the PII map and replaces
       matched tokens via a lookup map.
    3. Output: Appends processed records to CSV output.
"""

import gc
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, List, Optional, Set, Tuple, Union

import openpyxl
import pandas as pd


# --- TYPES & CONSTANTS ---

ConfigDict = Any
PathLike = Union[str, Path]


# --- CONFIGURATION & UTILITIES ---

def load_config(config_path: Path) -> ConfigDict:
    """Loads JSON configuration."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def setup_logging(output_dir: Path) -> None:
    """Configures execution logging."""
    log_file = output_dir / "sdf_anonymizer.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def find_excel_file(search_dir: Path, partial_name: str) -> Optional[Path]:
    """Locates the first Excel file matching the partial name pattern."""
    matches = list(search_dir.glob(f"*{partial_name}*.xlsx"))
    if not matches:
        logging.error(f"No file found matching '{partial_name}' in {search_dir}")
        return None
    return matches[0]


def _excel_chunk_generator(file_path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
    """
    Generator function yielding DataFrame chunks from an Excel file.
    Utilises OpenPyXL in read_only mode to maintain O(1) memory usage.
    """
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


# --- ANONYMISATION ENGINE ---

def build_pii_map(reference_path: Path) -> Dict[str, str]:
    """
    Constructs the redaction dictionary from the Reference file.
    
    Strategy:
        Iterates through reference data to collect names, IDs, and email components.
        Constructs a deterministic mapping to obfuscated values.
    """
    logging.info("Building PII Map from Reference Data...")
    
    pii_words: Set[str] = set()
    staff_ids: Set[str] = set()
    
    generator = _excel_chunk_generator(reference_path, chunk_size=10000)

    for chunk in generator:
        for _, row in chunk.iterrows():
            if pd.notna(row.get('First_Name')):
                pii_words.add(str(row['First_Name']).strip())
            if pd.notna(row.get('Family_Name')):
                pii_words.add(str(row['Family_Name']).strip())
            if pd.notna(row.get('Staff_ID')):
                staff_ids.add(str(row['Staff_ID']).strip())
            
            email = str(row.get('UTS_Email', ''))
            if '@' in email:
                prefix = email.split('@')[0]
                parts = re.split(r'[._-]', prefix)
                for part in parts:
                    if len(part) > 2:
                        pii_words.add(part)
        
        del chunk
        gc.collect()

    sorted_names = sorted(list(pii_words), key=len, reverse=True)
    name_map = {name: f'RedactedName_{i+1}' for i, name in enumerate(sorted_names)}
    
    sorted_ids = sorted(list(staff_ids))
    id_map = {sid: f'9{str(i+1).zfill(6)}' for i, sid in enumerate(sorted_ids)}
    
    combined_map = {**name_map, **id_map}
    logging.info(f"PII Map Built: {len(combined_map)} entries.")
    return combined_map


class AnonymizerEngine:
    """
    Encapsulates regex compilation and replacement logic.
    Optimizes for O(N) replacement using a single compiled regex pattern.
    """
    
    def __init__(self, pii_map: Dict[str, str]):
        self.pii_map = pii_map
        self.lookup_map = {k.lower(): v for k, v in pii_map.items()}
        
        sorted_keys = sorted(pii_map.keys(), key=len, reverse=True)
        if sorted_keys:
            pattern_str = '|'.join(map(re.escape, sorted_keys))
            self.regex = re.compile(f'\\b({pattern_str})\\b', re.IGNORECASE)
        else:
            self.regex = None

    def anonymize_text(self, text: Any) -> Any:
        """Replaces PII tokens in the input text using the compiled regex."""
        if not self.regex or not isinstance(text, str):
            return text
        
        def replace_match(match):
            key = match.group(0).lower()
            return self.lookup_map.get(key, match.group(0))

        return self.regex.sub(replace_match, text)


# --- MAIN STREAMING LOGIC ---

def process_stream(input_path: Path, output_path: Path, engine: AnonymizerEngine, 
                  chunk_size: int, dry_run: int) -> None:
    """
    Streams input Excel data, applies anonymization, and appends results to CSV.
    """
    logging.info(f"Anonymising: {input_path.name} -> {output_path.name}")

    if output_path.exists():
        output_path.unlink()

    total_rows = 0
    
    try:
        reader = _excel_chunk_generator(input_path, chunk_size)
        
        for i, chunk in enumerate(reader):
            if dry_run > 0 and total_rows >= dry_run:
                break

            obj_cols = chunk.select_dtypes(include='object').columns
            for col in obj_cols:
                chunk[col] = chunk[col].apply(engine.anonymize_text)
            
            chunk.to_csv(output_path, mode='a', index=False, header=(i == 0))
            
            total_rows += len(chunk)
            del chunk
            gc.collect()

        logging.info(f"Processing Complete. Total Rows: {total_rows}")

    except Exception as e:
        logging.critical(f"Streaming failed: {e}")
        sys.exit(1)


def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    inputs_dir = base_dir / "inputs"
    outputs_dir = base_dir / "outputs"
    config = load_config(base_dir / "config.json")
    setup_logging(outputs_dir)
    
    ref_file = find_excel_file(inputs_dir, config['settings']['input_pattern_reference'])
    data_file = find_excel_file(inputs_dir, config['settings']['input_pattern_transaction'])
    
    if not ref_file or not data_file:
        sys.exit(1)
        
    pii_map = build_pii_map(ref_file)
    engine = AnonymizerEngine(pii_map)
    
    process_stream(ref_file, outputs_dir / "Anonymized_Reference.csv", engine, config['performance_settings']['chunk_size'], config['performance_settings']['dry_run_limit'])
    process_stream(data_file, outputs_dir / "Anonymized_Data_Source.csv", engine, config['performance_settings']['chunk_size'], config['performance_settings']['dry_run_limit'])


if __name__ == "__main__":
    main()
