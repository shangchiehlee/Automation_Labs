# Automation_Labs

**Author:** Shane Lee
**Licence:** MIT

---

## 1. Overview

Automation_Labs contains two modules, CAD and SDF. Each module includes its own `README.md` with module-specific details.

---

## 2. Modules

- **[CAD](./CAD):** Casual Academic Database (CAD) module. See `CAD/README.md`.
- **[SDF](./SDF):** Staff Development Fund (SDF) module. See `SDF/README.md`.

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
