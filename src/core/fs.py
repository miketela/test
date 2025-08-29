#!/usr/bin/env python3
"""
Filesystem utilities for SBP Atoms Pipeline.
Handles file operations, checksums, and versioning.
"""

import hashlib
import shutil
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA256 hash of a file.
    
    Args:
        file_path: Path to the file
    
    Returns:
        SHA256 hash as hexadecimal string
    """
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        # Read file in chunks to handle large files
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def get_file_info(file_path: Path) -> dict:
    """Get comprehensive file information.
    
    Args:
        file_path: Path to the file
    
    Returns:
        Dictionary with file information
    """
    stat = file_path.stat()
    
    return {
        "path": str(file_path),
        "name": file_path.name,
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "sha256": calculate_sha256(file_path)
    }


def copy_with_versioning(source_path: Path, dest_path: Path, run_id: str) -> Tuple[Path, bool]:
    """Copy file with versioning if destination exists with different content.
    
    Args:
        source_path: Source file path
        dest_path: Destination file path
        run_id: Current run ID for versioning
    
    Returns:
        Tuple of (actual_dest_path, was_versioned)
    """
    # Ensure destination directory exists
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    # If destination doesn't exist, simple copy
    if not dest_path.exists():
        shutil.copy2(source_path, dest_path)
        return dest_path, False
    
    # Check if files are different
    source_hash = calculate_sha256(source_path)
    dest_hash = calculate_sha256(dest_path)
    
    if source_hash == dest_hash:
        # Files are identical, no need to copy
        return dest_path, False
    
    # Files are different, create versioned copy
    stem = dest_path.stem
    suffix = dest_path.suffix
    versioned_path = dest_path.parent / f"{stem}__run-{run_id}{suffix}"
    
    shutil.copy2(source_path, versioned_path)
    return versioned_path, True


def normalize_filename(filename: str) -> str:
    """Normalize filename to uppercase for validation.
    
    Args:
        filename: Original filename
    
    Returns:
        Normalized filename in uppercase
    """
    return filename.upper()


def parse_filename_components(filename: str) -> Optional[dict]:
    """Parse filename components according to pattern [SUBTYPE]_[YYYYMMDD].CSV.
    
    Args:
        filename: Filename to parse
    
    Returns:
        Dictionary with parsed components or None if invalid
    """
    import re
    
    # Normalize to uppercase
    normalized = normalize_filename(filename)
    
    # Pattern: SUBTYPE_YYYYMMDD.CSV
    pattern = r'^([A-Z_]+)_(\d{8})\.CSV$'
    match = re.match(pattern, normalized)
    
    if not match:
        return None
    
    subtype = match.group(1)
    date_str = match.group(2)
    
    # Validate date
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return None
    
    return {
        "subtype": subtype,
        "date_str": date_str,
        "date": date_obj,
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def find_files_by_pattern(directory: Path, pattern: str = "*.csv") -> list[Path]:
    """Find files in directory matching pattern.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern to match
    
    Returns:
        List of matching file paths
    """
    if not directory.exists():
        return []
    
    return list(directory.glob(pattern))


def ensure_directory(path: Path) -> Path:
    """Ensure directory exists, create if necessary.
    
    Args:
        path: Directory path
    
    Returns:
        The directory path
    """
    path.mkdir(parents=True, exist_ok=True)
    return path