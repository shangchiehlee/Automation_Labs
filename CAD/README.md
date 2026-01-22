# Casual Academic Database (CAD) Processor

**Author:** Shane Lee
**Licence:** MIT

---

## 1. System Overview

**Functional Scope:**
The CAD processor reads an Excel workbook, detects a header row containing required columns, and aggregates data by `School`, `Subject No.`, `Subject`, and year extracted from `Teaching Session`. It computes total oncosts, total students, and cost per student where student count is greater than zero. It writes an output workbook with `Processing Summary`, `Trend Analysis`, `Report`, and `Fuzzy Bands` worksheets. The default input is `inputs/CAD_Contract.xlsx` and the default output is `outputs/Processed_CAD_Contract.xlsx`.

**Architectural Summary:**
The module implements a deterministic, single pass ingestion and aggregation pipeline. It streams rows via `openpyxl` read only iteration and aggregates into in memory dictionaries keyed by subject year and school year. Memory growth is proportional to the number of unique aggregation keys and the size of the aggregated tables, not the number of input rows.

---

## 2. Technical Architecture

### 2.1. Ingestion and Aggregation
The processor locates the input table by scanning the first 30 rows for required columns. It streams rows from the detected sheet in read only mode, discards summary rows (`Total`, `Sum`, `Result`), and drops rows missing key fields or a detectable year. Aggregation maintains running totals per subject year and school year, then derives cost per student and status values for each group.

### 2.2. Fuzzy Banding
Per year anchors are derived from finite, positive cost per student values. Triangular membership functions assign Low, Medium, and High membership scores, with deterministic tie breaking that prioritises Medium, then Low, then High.

### 2.3. Workbook Output
The output workbook includes:
*   `Processing Summary` with input SHA-256, detected sheet and header row, row drop counts, group counts, and per year fuzzy anchors.
*   `Trend Analysis` with a school by year cost per student matrix and conditional formatting anchored to per year minimum, median, and maximum finite positive values.
*   `Report` with subject level totals and cost per student values arranged by year.
*   `Fuzzy Bands` with per school year band assignments, membership scores, and a summary table.

---

## 3. Usage

### 3.1. Execution
Execute the processor to run the pipeline and generate the Excel report.
```bash
python src/cad_processor.py
```

### 3.2. Options
```
--base-dir
--input-filename
--output-filename
--verbose
--no-processing-summary
```

---

## 4. Inputs, Outputs, and Errors

**Inputs:**
*   `inputs/CAD_Contract.xlsx` by default, unless overridden with `--input-filename`.

**Outputs:**
*   `outputs/Processed_CAD_Contract.xlsx`
*   `outputs/cad_processor.log`
*   `outputs/cad_processor_execution.log`

**Error Conditions:**
*   Missing input file results in `FileNotFoundError`.
*   Missing required columns results in `ValueError`.
