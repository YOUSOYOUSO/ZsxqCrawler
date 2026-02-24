#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Centralized filesystem path helpers for the project."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
VAR_DIR = PROJECT_ROOT / "var"
LOG_DIR = VAR_DIR / "logs"
OUTPUT_DIR = PROJECT_ROOT / "output"


def ensure_dirs() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_config_path(name: str) -> Path:
    ensure_dirs()
    return CONFIG_DIR / name


def get_log_path(name: str) -> Path:
    ensure_dirs()
    return LOG_DIR / name


def get_output_dir() -> Path:
    ensure_dirs()
    return OUTPUT_DIR

