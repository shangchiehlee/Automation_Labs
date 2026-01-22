# Automation_Labs

**Author:** Shane Lee
**Licence:** MIT

---

## 1. Overview

Automation_Labs contains two modules, CAD and SDF. Each module includes its own `README.md` with module-specific details.

---

## 2. Modules

- **[CAD](./CAD):** CAD contract workbook processor that produces `Processing Summary`, `Trend Analysis`, `Report`, and `Fuzzy Bands` worksheets. See `CAD/README.md`.
- **[SDF](./SDF):** Staff Development Fund (SDF) reconciliation module that produces a ZIP package containing CSV summaries, staff statements, and email artefacts. The module also includes an optional anonymiser that outputs redacted CSV files. See `SDF/README.md`.

---

## 3. Unified Setup Guide

### 3.1. Environment Initialisation
The monorepo operates within a single virtual environment anchored at the root.

```bash
# 1. Create virtual environment
python -m venv .venv

# 2. Activate (Windows)
.venv\Scripts\activate
# 2. Activate (Linux/MacOS)
source .venv/activate

# 3. Install dependencies
pip install -r requirements.txt
```
