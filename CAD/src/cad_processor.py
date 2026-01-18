# -*- coding: utf-8 -*-
"""
Program Overview: Casual Academic Database (CAD) Processor
Author: Shane Lee

System Function:
    Calculates financial efficiency metrics ('Cost Per Student') from raw contract datasets.
    Generates consolidated financial reports and statistical trend heatmaps.

Architectural Pattern:
    Implements a Streaming Map-Reduce architecture to process datasets exceeding
    physical memory limits (N > 10^7 rows).

    1. Ingestion: Utilises OpenPyXL generators in read-only mode to stream Excel data
       with O(1) memory complexity.
    2. Map Phase: Performs partial aggregation on data chunks to reduce high-volume
       transactional data into lightweight summary statistics.
    3. Reduce Phase: Combines partial aggregates into global totals, ensuring memory
       usage is proportional to result cardinality rather than input volume.
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.formatting.rule import CellIsRule, ColorScaleRule
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

# --- TYPES & CONSTANTS ---

ConfigDict = Dict[str, Any]


class Config:
    """Immutable configuration and constant definitions."""
    # Column Definitions
    COL_SCHOOL = "School"
    COL_SUBJECT_NO = "Subject No."
    COL_SUBJECT = "Subject"
    COL_SESSION = "Teaching Session"
    
    # Metric Definitions
    METRIC_ONCOSTS = "Incl Oncosts"
    METRIC_STUDENTS = "Student Count"
    METRIC_RATIO = "Incl Oncosts Per Student"
    
    # Aggregation Keys
    META_COLS = [COL_SCHOOL, COL_SUBJECT_NO, COL_SUBJECT]

    # Visual Styles (Hex Codes)
    COLOR_GREY = "E0E0E0"
    COLOR_GREEN = "63BE7B"
    COLOR_YELLOW = "FFEB84"
    COLOR_RED = "F8696B"
    COLOR_WHITE = "FFFFFF"

    # Compiled Regex Patterns
    REGEX_YEAR = re.compile(r'(20\d{2})')
    REGEX_NUMERIC_CLEAN = re.compile(r'[^\d.-]')
    REGEX_TOTAL_FILTER = re.compile(r'Total|Sum|Result', re.IGNORECASE)


# --- INFRASTRUCTURE ---

def load_config(config_path: Path) -> ConfigDict:
    """Loads JSON configuration."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def setup_logging(output_dir: Path) -> None:
    """Configures forensic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(output_dir / "cad_processor.log", mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


class CasualContractProcessor:
    """
    Executes the Map-Reduce pipeline for financial contract data.
    Enforces O(1) memory usage during ingestion via streaming generators.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.config_data = load_config(base_dir / "config.json")
        self.input_dir = base_dir / "inputs"
        self.output_dir = base_dir / "outputs"
        
        setup_logging(self.output_dir)
        
        # Performance Settings
        self.chunk_size = self.config_data['performance_settings']['chunk_size']
        self.dry_run = self.config_data['performance_settings']['dry_run_limit']
        self.target_years = self.config_data['settings']['target_years']

    def _find_header_row(self, ws: Worksheet) -> int:
        """
        Scans the first 20 rows to dynamically locate the header index.
        Complexity: O(1) - Scans fixed number of rows.
        """
        target = Config.COL_SCHOOL.lower()
        for idx, row in enumerate(ws.iter_rows(min_row=1, max_row=20, values_only=True)):
            if any(target in str(cell).lower() for cell in row if cell is not None):
                return idx
        return 0

    def _excel_chunk_generator(self, ws: Worksheet, header_row: int, chunk_size: int) -> Iterator[pd.DataFrame]:
        """
        Yields DataFrame chunks from a worksheet using OpenPyXL in read_only mode.
        
        Complexity:
            Space: O(1) - Only holds 'chunk_size' rows in memory.
            Time: O(N) - Single pass linear scan.
        """
        rows_iter = ws.iter_rows(values_only=True)
        
        # Skip to header
        for _ in range(header_row):
            next(rows_iter, None)
        
        try:
            headers = next(rows_iter)
        except StopIteration:
            return 
        
        chunk: List[Tuple[Any, ...]] = []
        for row in rows_iter:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield pd.DataFrame(chunk, columns=headers)
                chunk = []
        
        if chunk:
            yield pd.DataFrame(chunk, columns=headers)

    def _clean_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies vectorised cleaning operations to a data chunk in memory.
        """
        if Config.COL_SCHOOL not in df.columns:
            return pd.DataFrame()
            
        # 1. Row Filtering
        df = df.dropna(subset=[Config.COL_SCHOOL])
        df = df[~df[Config.COL_SCHOOL].astype(str).str.contains(Config.REGEX_TOTAL_FILTER, na=False)]
        
        # 2. Type Conversion
        if Config.METRIC_STUDENTS in df.columns:
            df[Config.METRIC_STUDENTS] = pd.to_numeric(df[Config.METRIC_STUDENTS], errors='coerce').fillna(0)
            
        if Config.METRIC_ONCOSTS in df.columns:
            # Remove currency symbols ($ ,) before conversion
            df[Config.METRIC_ONCOSTS] = (
                df[Config.METRIC_ONCOSTS]
                .astype(str)
                .str.replace(Config.REGEX_NUMERIC_CLEAN, '', regex=True)
            )
            df[Config.METRIC_ONCOSTS] = pd.to_numeric(df[Config.METRIC_ONCOSTS], errors='coerce').fillna(0)

        # 3. Temporal Extraction
        if Config.COL_SESSION in df.columns:
            df['Year_Extracted'] = (
                df[Config.COL_SESSION]
                .astype(str)
                .str.extract(Config.REGEX_YEAR)
                .fillna("Unknown")
            )
            
        if self.target_years and 'Year_Extracted' in df.columns:
            df = df[df['Year_Extracted'].isin(self.target_years)]

        return df

    def execute_map_reduce(self, input_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Executes the Map-Reduce pipeline.
        
        Phase 1 (Map): Reads chunks, cleans, and computes partial sums.
        Phase 2 (Reduce): Aggregates partial sums into global totals.
        """
        wb = load_workbook(filename=input_path, read_only=True, data_only=True)
        try:
            ws = wb.active
            header_row = self._find_header_row(ws)
            logging.info(f"Detected Header at Row {header_row}. Starting Stream...")

            running_aggregate: Optional[pd.DataFrame] = None
            total_rows = 0

            reader = self._excel_chunk_generator(ws, header_row, self.chunk_size)
            
            for i, chunk in enumerate(reader):
                if self.dry_run > 0 and total_rows >= self.dry_run:
                    break

                clean_chunk = self._clean_chunk(chunk)
                
                if not clean_chunk.empty:
                    try:
                        # MAP PHASE: Partial Aggregation
                        grouped_chunk = clean_chunk.groupby(
                            Config.META_COLS + ['Year_Extracted']
                        )[[Config.METRIC_ONCOSTS, Config.METRIC_STUDENTS]].sum().reset_index()
                        
                        if running_aggregate is None:
                            running_aggregate = grouped_chunk
                        else:
                            running_aggregate = (
                                pd.concat([running_aggregate, grouped_chunk], ignore_index=True)
                                .groupby(Config.META_COLS + ['Year_Extracted'])[[Config.METRIC_ONCOSTS, Config.METRIC_STUDENTS]]
                                .sum()
                                .reset_index()
                            )
                    except KeyError as e:
                        logging.warning(f"Skipping batch {i+1} due to missing columns: {e}")
                
                total_rows += len(chunk)
                logging.info(f"Processed Batch {i+1}: {len(chunk)} rows.")
                    
            logging.info(f"Streaming Complete. Total Input Rows: {total_rows}")

            if running_aggregate is None:
                return pd.DataFrame(), pd.DataFrame()
                
            # REDUCE PHASE: Global Aggregation
            # Memory safe: Retains only aggregated keys across chunks.
            final_pivot = running_aggregate
        finally:
            wb.close()

        # Final Metric Derivation
        final_pivot[Config.METRIC_RATIO] = (
            final_pivot[Config.METRIC_ONCOSTS] / final_pivot[Config.METRIC_STUDENTS]
        ).replace([np.inf, -np.inf], 0).fillna(0)

        return self._transform_reports(final_pivot)

    def _transform_reports(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Transforms the aggregated data into Detailed and Trend views."""
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # 1. Trend View (School Level)
        school_totals = df.groupby([Config.COL_SCHOOL, 'Year_Extracted'])[[Config.METRIC_ONCOSTS, Config.METRIC_STUDENTS]].sum().reset_index()
        school_totals[Config.METRIC_RATIO] = (
            school_totals[Config.METRIC_ONCOSTS] / school_totals[Config.METRIC_STUDENTS]
        )
        
        trend_df = school_totals.pivot(
            index=Config.COL_SCHOOL, 
            columns='Year_Extracted', 
            values=Config.METRIC_RATIO
        ).reset_index()
        
        # 2. Detailed Report
        detailed_df = df.pivot_table(
            index=Config.META_COLS,
            columns='Year_Extracted',
            values=[Config.METRIC_ONCOSTS, Config.METRIC_STUDENTS, Config.METRIC_RATIO],
            fill_value=0
        )
        
        # Flatten MultiIndex
        detailed_df.columns = [f"{year} {metric}" for metric, year in detailed_df.columns]
        detailed_df = detailed_df.reset_index()
        
        return detailed_df, trend_df

    def save_and_format(self, detailed_df: pd.DataFrame, trend_df: pd.DataFrame) -> None:
        """Saves reports to Excel and applies conditional formatting."""
        output_path = self.output_dir / self.config_data['settings']['output_filename']
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            trend_df.to_excel(writer, sheet_name="Trend Analysis", index=False)
            detailed_df.to_excel(writer, sheet_name="Report", index=False)
            
        wb = load_workbook(output_path)
        if "Trend Analysis" in wb.sheetnames:
            self._format_trend_sheet(wb["Trend Analysis"])
        wb.save(output_path)
        logging.info(f"Report Saved: {output_path}")

    def _format_trend_sheet(self, ws: Worksheet) -> None:
        """Applies Heatmap formatting and Methodology Commentary."""
        logging.info("Applying Statistical Formatting...")
        
        # Styles
        header_font = Font(bold=True)
        grey_fill = PatternFill(start_color=Config.COLOR_GREY, end_color=Config.COLOR_GREY, fill_type="solid")
        
        # Header Formatting
        for cell in ws[1]:
            cell.font = header_font
            cell.fill = grey_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')

        # Statistics Calculation
        values: List[float] = []
        zero_count = 0
        
        for row in ws.iter_rows(min_row=2, min_col=2, values_only=True):
            for val in row:
                if isinstance(val, (int, float)):
                    if val > 0: 
                        values.append(val)
                    else: 
                        zero_count += 1

        if not values: 
            return

        min_val, median_val, max_val = min(values), float(np.median(values)), max(values)

        # Heatmap Rules
        if ws.max_column >= 2:
            rng = f"B2:{get_column_letter(ws.max_column)}{ws.max_row}"
            
            ws.conditional_formatting.add(rng, CellIsRule(
                operator='equal', formula=['0'], stopIfTrue=True, 
                fill=PatternFill(start_color=Config.COLOR_WHITE, end_color=Config.COLOR_WHITE, fill_type="solid")
            ))
            
            ws.conditional_formatting.add(rng, ColorScaleRule(
                start_type='num', start_value=min_val, start_color=Config.COLOR_GREEN,
                mid_type='num', mid_value=median_val, mid_color=Config.COLOR_YELLOW,
                end_type='num', end_value=max_val, end_color=Config.COLOR_RED
            ))

        self._write_methodology(ws, zero_count, min_val, median_val, max_val)

    def _write_methodology(self, ws: Worksheet, zero_count: int, min_val: float, median_val: float, max_val: float) -> None:
        """Writes the explanatory legend."""
        col_idx = ws.max_column + 2
        ws.column_dimensions[get_column_letter(col_idx)].width = 60
        
        title = ws.cell(row=1, column=col_idx, value="Methodology & Insights")
        title.font = Font(bold=True)
        title.fill = PatternFill(start_color=Config.COLOR_GREY, end_color=Config.COLOR_GREY, fill_type="solid")
        
        texts = [
            f"1. Handling Outliers (Zeros): {zero_count} instances of $0.00 detected (No Activity).",
            f"2. Median Anchor: ${median_val:,.2f} (50th percentile) used to normalize scale.",
            f"3. Colour Scale: Green (${min_val:,.2f}) -> Yellow (${median_val:,.2f}) -> Red (${max_val:,.2f})"
        ]

        for i, text in enumerate(texts):
            cell = ws.cell(row=2 + (i*2), column=col_idx, value=text)
            cell.alignment = Alignment(wrap_text=True, indent=1)

    def run(self) -> None:
        """Main execution entry point."""
        input_file = self.input_dir / self.config_data['settings']['input_filename']
        if not input_file.exists():
            logging.error(f"Input file not found: {input_file}")
            return

        detailed, trend = self.execute_map_reduce(input_file)
        
        if not detailed.empty:
            self.save_and_format(detailed, trend)
        else:
            logging.warning("No data processed.")


if __name__ == "__main__":
    base_path = Path(__file__).resolve().parent.parent
    processor = CasualContractProcessor(base_path)
    processor.run()
