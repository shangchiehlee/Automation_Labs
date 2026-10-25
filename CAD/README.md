# Casual Academic Database (CAD) Processor

**Author:** Shane Lee
**Licence:** MIT

---

## 1. Overview

The CAD processor turns a CAD contract export workbook into a summary workbook. For each subject and each school, it adds up contract costs (including on-costs) and student numbers by year, and calculates the cost per student. It then writes a workbook with four sheets:

- **Processing Summary:** what was read, what was excluded and why, and the values used for banding
- **Trend Analysis:** cost per student for each school by year, with colour scales
- **Report:** totals and cost per student for each subject by year
- **Fuzzy Bands:** a Low, Medium or High band for each school and year, with membership scores

The processor is designed to give the same results each time it runs on the same input file. The Processing Summary records a SHA-256 fingerprint of the input file, so a result can be matched to the exact file used.

---

## 2. How it works

1. **Find the table.** The processor scans the first 30 rows of each sheet for a header row containing the required columns (see Section 4).
2. **Read the rows.** It reads the table row by row without loading the whole workbook into memory. It excludes summary rows (`Total`, `Sum`, `Result`), rows missing the school or subject details, rows where no year can be found in `Teaching Session`, and rows with negative student counts. Missing costs and student counts are treated as zero.
3. **Add up.** It totals cost and students for each subject and year, and for each school and year, and calculates cost per student. A group with costs but no students is marked "Undefined (0 students)"; a group with neither is marked "No activity".
4. **Band.** For each year, it takes the lowest, median and highest cost per student across schools (positive values only) as anchors. Triangular membership functions then give each school a Low, Medium and High score, and the highest score sets its band. Ties are resolved in the order Medium, Low, High. The bands support comparison within a year; they are not probabilities.
5. **Write.** It writes the output workbook and two log files.

---

## 3. Usage

Place the input workbook in `CAD/inputs/`, then run from the repository root:

```bash
python CAD/src/cad_processor.py
```

Options:

- `--input-filename`: the input workbook name in `inputs/` (default `CAD_Contract.xlsx`)
- `--output-filename`: the output workbook name in `outputs/` (default `Processed_CAD_Contract.xlsx`)
- `--base-dir`: the CAD folder, if the processor cannot find it automatically
- `--no-processing-summary`: leave out the Processing Summary sheet
- `--verbose`: show more detail in the log

The processor finds the CAD folder by looking for `config.json` next to an `inputs/` folder. It takes its file names from the options above.

---

## 4. Inputs, outputs and errors

**Input:** `CAD/inputs/CAD_Contract.xlsx` by default. The table needs these columns: `School`, `Subject No.`, `Subject`, `Teaching Session`, `Incl Oncosts` and `Student Count`. Common variants, such as `Subject Number` and `Incl. Oncosts`, are recognised.

**Outputs:**

- `CAD/outputs/Processed_CAD_Contract.xlsx`
- `CAD/outputs/cad_processor.log`: the processing log
- `CAD/outputs/cad_processor_execution.log`: one line per successful run, with the input file name, output file name and input fingerprint

**Errors:**

- A missing input file stops the run with a message that says where to place it.
- If no sheet contains all the required columns, the run stops and lists the columns it looked for.
