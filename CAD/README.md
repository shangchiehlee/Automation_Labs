# Casual Academic Database (CAD) Processor

**Author:** Shane Lee
**Licence:** MIT

---

## 1. System Overview

**Functional Scope:**
The CAD Processor reads the configured Excel input file, cleans and aggregates by `School`, `Subject No.`, `Subject`, and `Year_Extracted` derived from `Teaching Session`, and computes `Incl Oncosts Per Student` as `Incl Oncosts` divided by `Student Count`. It writes an Excel output with `Trend Analysis` and `Report` worksheets.

**Architectural Summary:**
The module implements a chunked Map-Reduce style pipeline with stream-based Excel ingestion and incremental aggregation. Memory usage is bounded by the number of unique aggregation keys held in the running aggregation state and the configured chunk size, rather than input row count.

---

## 2. Technical Architecture

### 2.1. Incremental Map-Reduce Engine
The `CasualContractProcessor` class executes a two-phase processing pipeline:

1.  **Map Phase (Ingestion & Partial Aggregation):**
    *   Streams raw Excel data via `openpyxl` iterators in read-only mode.
    *   Applies cleaning and per-chunk aggregation using pandas operations.
2.  **Reduce Phase (Global Aggregation):**
    *   Updates a running aggregation keyed by `School`, `Subject No.`, `Subject`, and `Year_Extracted` by combining each chunk aggregate.
    *   **Memory Impact:** Memory usage is bounded by the number of unique aggregation keys in the running aggregation state and the configured chunk size, rather than input row count.

### 2.2. Visualization Logic
*   **Statistical Formatting:** Applies a three-colour scale heatmap using minimum, median, and maximum positive values as scale points, and formats zero values as white.
*   **Data Integrity:** Detects the header row by scanning the first 20 rows for a cell containing `School`.

---

## 3. Usage

### 3.1. Execution
Execute the processor to run the pipeline and generate the Excel report.
```bash
python src/cad_processor.py
```
