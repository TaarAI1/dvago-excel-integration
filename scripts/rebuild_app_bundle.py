#!/usr/bin/env python3
"""
Regenerate backend/app_bundle.tar.gz from backend/app/.

Run this script whenever backend/app/ source files change before committing:
    py scripts/rebuild_app_bundle.py

The tarball is committed to git so Railway's build context (which includes
files at depth-2 like backend/app_bundle.tar.gz but not nested directories
like backend/app/) can still get the full source via COPY + tar extract.
"""
import tarfile
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(REPO_ROOT, "backend", "app")
OUTPUT = os.path.join(REPO_ROOT, "backend", "app_bundle.tar.gz")

if not os.path.isdir(SOURCE_DIR):
    print(f"ERROR: source directory not found: {SOURCE_DIR}", file=sys.stderr)
    sys.exit(1)

with tarfile.open(OUTPUT, "w:gz") as tar:
    for root, dirs, files in os.walk(SOURCE_DIR):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for file in files:
            if file.endswith((".pyc", ".pyo", ".pyd")):
                continue
            full = os.path.join(root, file)
            arcname = os.path.relpath(full, os.path.dirname(SOURCE_DIR)).replace(os.sep, "/")
            tar.add(full, arcname=arcname)

size = os.path.getsize(OUTPUT)
print(f"Created {OUTPUT}  ({size:,} bytes)")
