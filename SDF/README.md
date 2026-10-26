# Staff Development Fund (SDF) Processor

**Author:** Shane Lee
**Licence:** MIT

---

## 1. Overview

The SDF processor prepares Staff Development Fund reporting data. It links each transaction to the staff member it belongs to, calculates each person's debit, credit and net totals, and produces:

- a spreadsheet statement for each staff member
- a matching email file (`.eml`) with that statement attached, ready to review and send
- summary CSV files for later analysis and querying
- an execution log that records how the data was processed and any problems found

All outputs are packaged in one ZIP file.

The processor automates repeatable preparation. Interpreting the figures, reconciling them with the wider administrative context and explaining them to staff remain the responsibility of the person running the workflow. The processor does not send emails.

The module also includes an anonymisation utility, which replaces names and staff IDs in selected files before they are used elsewhere, and a validation script that checks the generated packages.

### Research context

This software is part of the MSc (Research) thesis *Enterprise Generative AI Workability within a University: Sociotechnical Friction, Workaround Formation and Less Visible Work* (Shane Lee, University of Technology Sydney). Chapter 4 of the thesis describes how the processor was developed and how it fits into the wider SDF reporting workflow. The research website explains this arrangement visually: <https://mres.shangchiehlee.com/settings/sdf-workflow/>.

---

## 2. How it works

1. **Load.** The processor reads the reference file (staff records) and the transaction file in chunks, so large files do not need to fit in memory.
2. **Link.** It extracts a seven-digit `TR_Code` from each transaction's account field and uses it to link the transaction to a staff record. A temporary SQLite database holds both tables for the joins and totals, and is deleted when the run ends.
3. **Check.** Rows whose `TR_Code` cannot be extracted are logged and excluded. Missing, empty or duplicate `TR_Code` values in the reference file stop the run. The log ends with the number of input rows, the number of valid rows and the share retained.
4. **Produce.** For each staff member with transactions, the processor creates a statement with a `TOTAL` row and a matching `.eml` file. It also writes the summary CSV files.
5. **Package.** It places the statements, email files, CSV files and execution log in one timestamped ZIP file.

`TR_Code` stays in the outputs, so a figure in a statement or summary can be traced back to its transactions and staff record.

---

## 3. Usage

### 3.1. Setup

From the repository root, create the shared virtual environment and install the dependencies, as described in the root `README.md`. The module needs Python 3, `pandas` and `openpyxl`.

### 3.2. Prepare the processing run

1. Place the input files in `SDF/inputs/` (see Section 4).
2. Set the email details and check the other settings in `SDF/config.json` (see Section 5).
3. Run the processor:

```bash
python SDF/src/sdf_processor.py
```

### 3.3. Variants for more input types

`sdf_processor_local.py` and `sdf_anonymizer_local.py` process data in the same way as the standard scripts, with three differences:

- they also accept `.xlsm` and `.csv` input files
- they ignore temporary Excel lock files (names starting with `~$`)
- when several files match, they prefer Excel files and log which file was selected

```bash
python SDF/src/sdf_processor_local.py
```

### 3.4. Anonymisation (optional)

Use the anonymisation utility before using selected files outside the processing environment, for example for querying.

```bash
python SDF/src/sdf_anonymizer.py
```

It builds a replacement list from the names, staff IDs and email addresses in the reference file. It then replaces matching whole words, ignoring case, in every text value of both input files, including descriptions:

- first and family names become `RedactedName_1`, `RedactedName_2` and so on
- staff IDs become seven-digit codes starting with `9`
- name parts of email addresses (for example the first and last name in `first.last@…`) are replaced where they are longer than two characters

The same original value always receives the same replacement, so the relationships between records, including `TR_Code`, are kept.

Some details are not changed:

- values stored as numbers, including staff IDs held in a numeric `Staff_ID` column
- names of people who are not in the reference file
- amounts, account codes, dates and email domains

Review the anonymised files before sharing them, because remaining details can still identify people.

### 3.5. Validation

```bash
python SDF/scripts/sdf_validate_outputs.py               # check existing packages in SDF/outputs/
python SDF/scripts/sdf_validate_outputs.py --run-pipeline  # run the processor first, then check
```

The script checks that each ZIP package contains the four summary CSV files, that every `TR_Code` has seven digits, that the summaries are sorted as expected and that each statement's `TOTAL` row equals the sum of its rows. It reports `PASS` or lists the problems found. With `--run-pipeline`, it writes to `SDF/outputs/_sdf_validate/`, which can be deleted at any time.

These checks confirm that the processing ran as designed. They do not establish that every figure is correct in its administrative context.

---

## 4. Inputs

The processor finds its inputs in `SDF/inputs/` by name:

- **Reference file:** an `.xlsx` file whose name contains `Reference`, with the columns `TR_Code`, `Staff_ID`, `First_Name`, `Family_Name` and `UTS_Email`.
- **Transaction file:** an `.xlsx` file whose name contains `Data_Source`, with the columns `Activity Level 3 Code and Name`, `debit`, `credit`, `net_amount`, `Year`, `Month` and `journal_line_desc`.

The account field (`Activity Level 3 Code and Name`) must start with the seven-digit `TR_Code`. Leading zeros before the seven digits are ignored. Missing or non-numeric amounts are treated as zero.

---

## 5. Configuration

`SDF/config.json` holds:

- `settings.email_sender`: the sender address written into each `.eml` file
- `settings.email_cc`: an optional copy address (leave empty for none)
- `settings.email_general_contact` and `settings.email_finance_contact`: the contacts named in the email text
- `settings.email_sign_off`: the name that signs the email
- `settings.input_pattern_reference` and `settings.input_pattern_transaction`: the text that identifies each input file (default `Reference` and `Data_Source`)
- `performance_settings.chunk_size`: the number of rows read at a time (default 50,000)
- `performance_settings.dry_run_limit`: a row limit for test runs (`0` processes all rows)

The email settings in the repository are placeholders. Replace them with your own details before running the processor. The rest of the email text is set in `EMAIL_BODY_TEMPLATE` in the processor scripts.

---

## 6. Outputs

The processor writes `SDF/outputs/SDF_Output_Package_<timestamp>.zip`, which contains:

- `SDF_Statement_<First>_<Family>_<Staff_ID>.xlsx`: each staff member's transactions, with a `TOTAL` row
- `Email_<First>_<Family>.eml`: an email to that staff member with the statement attached and the totals in the message
- `SDF_Transaction_Ledger.csv`: every linked transaction with its staff record
- `SDF_All_Spenders.csv`: totals for each staff member, highest net spend first
- `SDF_Monthly_Trends.csv`: transaction counts and net spend by year and month
- `SDF_Spending_by_Account.csv`: net spend and transaction counts by account
- `sdf_processor_execution.log`: the processing log, including data issues and row counts

The anonymisation utility writes `Anonymized_Reference.csv`, `Anonymized_Data_Source.csv` and `sdf_anonymizer.log` to `SDF/outputs/`.

---

## 7. When a run stops

- **Missing input file:** the processor logs the problem and exits with a non-zero code.
- **Invalid reference data:** missing, empty or duplicate `TR_Code` values stop processing with an error.
- **Other processing errors:** the error is logged, no ZIP file is created and the processor exits with a non-zero code.

Transaction rows without a usable `TR_Code` do not stop the run. They are logged as data issues and excluded, and the row counts at the end of the log show how many were retained.

---

## 8. Citation

Lee, S. C. (2026). *Staff Development Fund (SDF) processor* (Version 1.0.0) [Computer software]. GitHub. https://github.com/shangchiehlee/data_automation/tree/main/SDF
