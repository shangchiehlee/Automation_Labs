# data_automation

**Author:** Shane Lee
**Licence:** MIT

---

## 1. Overview

This repository holds two Python modules that prepare administrative spreadsheet data for reporting. Each module has its own `README.md` with full details.

---

## 2. Modules

- **[SDF](./SDF):** prepares Staff Development Fund (SDF) reporting data. It links transactions to staff records, calculates each person's totals, and produces a statement and a matching email file for each staff member, summary CSV files and a processing log, packaged in one ZIP file. It also includes an optional anonymisation utility and a validation script. This module is part of the MSc (Research) thesis described in `SDF/README.md`.
- **[CAD](./CAD):** processes a Casual Academic Database (CAD) contract export workbook. It calculates cost per student by subject and school for each year, and writes a workbook with a processing summary, a trend view, a subject report and Low, Medium and High bands for each year.

---

## 3. Setup

Both modules share one virtual environment at the repository root. Run these commands from the root folder:

```bash
# 1. Create the virtual environment
python -m venv .venv

# 2. Activate it (Windows)
.venv\Scripts\activate
# 2. Activate it (macOS or Linux)
source .venv/bin/activate

# 3. Install the dependencies
pip install -r requirements.txt
```

Input and output data are kept out of the repository. Place input files in each module's `inputs/` folder; results are written to its `outputs/` folder.
