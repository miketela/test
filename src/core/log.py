#!/usr/bin/env python3
"""
Logging utilities for SBP Atoms Pipeline.
Provides console and structured audit logging capabilities.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional


class StructuredLogger:
    """Structured logger for audit trails."""
    
    def __init__(self, log_dir: Path, run_id: str):
        """Initialize structured logger."""
        self.log_dir = log_dir
        self.run_id = run_id
        self.events_file = log_dir / "events.jsonl"
        
        # Ensure log directory exists
        log_dir.mkdir(parents=True, exist_ok=True)
    
    def log_event(self, event_type: str, data: Dict[str, Any]):
        """Log a structured event."""
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": self.run_id,
            "event_type": event_type,
            "data": data
        }
        
        with open(self.events_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    
    def save_run_summary(self, summary: Dict[str, Any]):
        """Save run summary to run.json."""
        run_file = self.log_dir / "run.json"
        summary["run_id"] = self.run_id
        summary["timestamp"] = datetime.utcnow().isoformat()
        
        with open(run_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
    
    def save_manifest(self, manifest: Dict[str, Any]):
        """Save file manifest to manifest.json."""
        manifest_file = self.log_dir / "manifest.json"
        manifest["run_id"] = self.run_id
        manifest["timestamp"] = datetime.utcnow().isoformat()
        
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """Colored console formatter."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        """Format log record with colors."""
        log_color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(level: str = "INFO", verbose: bool = False):
    """Setup console logging."""
    # Clear any existing handlers
    logging.getLogger().handlers.clear()
    
    # Set level
    log_level = getattr(logging, level.upper(), logging.INFO)
    if verbose:
        log_level = logging.DEBUG
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Create formatter
    if sys.stdout.isatty():  # Use colors if terminal supports it
        formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)