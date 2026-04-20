#!/usr/bin/env python3
"""
Regenerate app_bundle.tar.gz (repo root) and backend/app_bundle.tar.gz
from backend/app/ source files.

Run whenever backend/app/ source changes, before committing:
    py scripts/rebuild_app_bundle.py

Why two files?
  Railway partitions monorepo build contexts per service root.
  The root-service context only contains repo-root files, so the
  root Dockerfile needs app_bundle.tar.gz at the repo root.
  backend/app_bundle.tar.gz is kept as a fallback if the backend
  service is ever configured with Root Directory = /backend.
"""
import tarfile
import os
import shutil
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(REPO_ROOT, "backend", "app")

if not os.path.isdir(SOURCE_DIR):
    print(f"ERROR: source directory not found: {SOURCE_DIR}", file=sys.stderr)
    sys.exit(1)

OUTPUTS = [
    os.path.join(REPO_ROOT, "app_bundle.tar.gz"),
    os.path.join(REPO_ROOT, "backend", "app_bundle.tar.gz"),
]

for output in OUTPUTS:
    with tarfile.open(output, "w:gz") as tar:
        for root, dirs, files in os.walk(SOURCE_DIR):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for file in files:
                if file.endswith((".pyc", ".pyo", ".pyd")):
                    continue
                full = os.path.join(root, file)
                arcname = os.path.relpath(full, os.path.dirname(SOURCE_DIR)).replace(os.sep, "/")
                tar.add(full, arcname=arcname)
    size = os.path.getsize(output)
    print(f"Created {output}  ({size:,} bytes)")
