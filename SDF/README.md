# Staff Development Fund (SDF) Processor

**Author:** Shane Lee
**Licence:** MIT

---

## 1. System Overview

**Functional Scope:**
The SDF Processor performs financial reconciliation between transaction logs and staff reference data to generate staff-specific statements and summary reports. It incorporates a deterministic anonymisation utility for PII sanitisation.

**Architectural Summary:**
The system uses a hybrid memory and disk pipeline. Transaction data is streamed into a temporary SQLite database for joins and aggregations, and the anonymiser applies compiled regex replacement while streaming Excel inputs to CSV outputs.

---

## 2. Technical Architecture

### 2.1. Hybrid SQL Pipeline (Temporary SQLite Buffer)
The processor stores transaction data in a temporary SQLite database for joins and aggregations:
*   **Ingestion:** Streams transaction and reference Excel data into a temporary SQLite buffer using `openpyxl` iterators and the configured chunk size.
*   **Processing:** Executes joins and aggregations via SQL queries. Creates an index on `transactions(TR_Code)` and defines `TR_Code` as the primary key in the reference table.
*   **Output:** Generates reports within a transient workspace and encapsulates all artifacts, including execution logs and CSV summaries, into a single ZIP deliverable.

### 2.2. Regex Anonymisation
The `AnonymizerEngine` executes a deterministic sanitisation pipeline:
*   **Streaming I/O:** Streams Excel data in chunks using `openpyxl` and appends anonymised rows to CSV output.
*   **Regex Replacement:** Builds a compiled regular expression from the PII map and replaces matched tokens via a lookup map.

---

## 3. Usage

### 3.1. Data Anonymisation (Optional)
Run this utility if data must be shared externally.
```bash
python src/sdf_anonymizer.py
```

### 3.2. Reconciliation Processing
Run the main processor to generate the comprehensive ZIP deliverable.
```bash
python src/sdf_processor.py
```

### 3.3. Validation Outputs
Running `python SDF/scripts/sdf_validate_outputs.py --run-pipeline` creates `SDF/outputs/_sdf_validate/`. This directory is a transient validation artefact and may be deleted at any time.

---

## 4. Inputs, Outputs, and Errors

**Inputs:**
*   `config.json` for `settings.input_pattern_reference`, `settings.input_pattern_transaction`, and `performance_settings`.
*   `inputs/*Reference*.xlsx` for reference data, where `Reference` is the configured pattern.
*   `inputs/*Data_Source*.xlsx` for transaction data, where `Data_Source` is the configured pattern.

**Outputs:**
*   `outputs/SDF_Output_Package_<timestamp>.zip` containing:
    *   `SDF_Transaction_Ledger.csv`
    *   `SDF_All_Spenders.csv`
    *   `SDF_Monthly_Trends.csv`
    *   `SDF_Spending_by_Account.csv`
    *   `SDF_Statement_<First>_<Family>_<Staff_ID>.xlsx`
    *   `Email_<First>_<Family>.eml`
    *   `sdf_processor_execution.log`
*   `outputs/Anonymized_Reference.csv` and `outputs/Anonymized_Data_Source.csv` when running `src/sdf_anonymizer.py`.
*   `outputs/sdf_anonymizer.log` when running `src/sdf_anonymizer.py`.

**Error Conditions:**
*   Missing input files result in early termination with a non zero exit code.
*   Invalid or duplicate `TR_Code` values in the reference data raise `ValueError` during ingestion and terminate processing.
