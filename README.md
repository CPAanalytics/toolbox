

## 1 · Install Python from the Microsoft Store

1. Open **Microsoft Store** → search for **“Python 3.11”** (or the newest 3.x edition).
2. Click **Install**.

---

## 2 · Open a *regular* PowerShell and verify Python

```powershell
python --version
# Example output: Python 3.11.4
```

If that prints a 3.9 + version you’re ready to continue.

---

## 3 · Upgrade pip (still no admin rights)

```powershell
python -m pip install --upgrade --user pip
```

The `--user` switch forces the install into:

```
%APPDATA%\Python\Python311\site-packages\
```

---

## 4 · Install **pipx** (per-user)

```powershell
python -m pip install --user pipx
python -m pipx ensurepath        # adds the pipx "Scripts" dir to your PATH
```

> **Close & reopen** PowerShell (or run `refreshenv`) so the PATH change takes effect.  
> You can always run pipx without PATH via  
> `python -m pipx` … if your IT policy blocks PATH changes.

Verify:

```powershell
pipx --version
```

---

## 5 · Install the **toolbox** CLI with pipx

### a. From the team’s Git repository

```powershell
pipx install "toolbox @ git+https://github.com/CPAanalytics/toolbox"
```

## 6 · Smoke test

```powershell
toolbox --help
toolbox dedup Example.csv --amount Amount --out Cleaned.csv
```

You should find `Cleaned.csv` sitting next to the original file with cancelling “in-and-out” rows removed.

---

## 7 · Upgrading and removing

| Task | Command |
|------|---------|
| **Upgrade to the newest release** | `pipx upgrade toolbox` |
| **Uninstall completely** | `pipx uninstall toolbox` |

Each pipx-managed app lives in its own virtual-environment directory under  
`%USERPROFILE%\.local\pipx\venvs` and can be removed without affecting anything else.

---

### Cheat-sheet recap

```powershell
# one-time setup
python -m pip install --user pipx
python -m pipx ensurepath
# reopen shell

# install or upgrade toolbox
pipx install  "toolbox @ git+https://github.com/your-org/toolbox.git@main"
pipx upgrade  toolbox

# run it
toolbox dedup my.csv --amount Amount --out my_clean.csv
```
