#!/usr/bin/env python3
"""
install_modules.py
Checks for required modules and installs any that are missing.
Run with:  python install_modules.py
"""

import importlib.util
import subprocess
import sys

REQUIRED = [
    "pandas",
    "numpy",
    "xlsxwriter",
    "teradatasql",
]

def is_installed(pkg: str) -> bool:
    return importlib.util.find_spec(pkg) is not None

def pip_install(pkg: str) -> int:
    print(f"Installing {pkg} ...")
    # Use -q to reduce noise; remove -q if you want verbose logs
    return subprocess.call([sys.executable, "-m", "pip", "install", pkg])

def main():
    print("Checking required packages...")
    missing = [p for p in REQUIRED if not is_installed(p)]
    if not missing:
        print("All required packages already installed.")
        return

    print("Missing:", ", ".join(missing))
    failures = []
    for pkg in missing:
        rc = pip_install(pkg)
        if rc != 0:
            failures.append(pkg)

    if failures:
        print("\nThe following packages failed to install:", ", ".join(failures))
        print("You can try installing them manually, e.g.:")
        print(f"  {sys.executable} -m pip install " + " ".join(failures))
        sys.exit(1)

    # Final import check
    print("\nVerifying imports...")
    for pkg in REQUIRED:
        try:
            __import__(pkg)
            print(f"OK: {pkg}")
        except Exception as e:
            print(f"FAILED: {pkg} -> {e}")
            sys.exit(2)

    print("\nAll set!")
    print("Now you can run your script, e.g.:")
    print(f"  {sys.executable} c86_client360_pa.py")

if __name__ == "__main__":
    main()
