#!/usr/bin/env python3
"""
Time utilities for SBP Atoms Pipeline.
Handles period resolution, date parsing, and validation.
"""

import re
from datetime import datetime, timedelta
from typing import Tuple, Optional


MONTH_NAMES = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}


def resolve_period(year: Optional[int] = None, month: Optional[str] = None) -> Tuple[int, int]:
    """Resolve the processing period.
    
    Args:
        year: Year to process (optional)
        month: Month to process - can be number or name (optional)
    
    Returns:
        Tuple of (year, month) as integers
    
    If no year/month provided, defaults to previous month.
    """
    now = datetime.now()
    
    # Default to previous month
    if year is None or month is None:
        # Calculate previous month
        first_day_current = now.replace(day=1)
        last_day_previous = first_day_current - timedelta(days=1)
        default_year = last_day_previous.year
        default_month = last_day_previous.month
        
        year = year or default_year
        month = month or str(default_month)
    
    # Parse month
    if isinstance(month, str):
        # Try to parse as number first
        try:
            month_num = int(month)
            if 1 <= month_num <= 12:
                return year, month_num
        except ValueError:
            pass
        
        # Try to parse as month name
        month_lower = month.lower()
        if month_lower in MONTH_NAMES:
            return year, MONTH_NAMES[month_lower]
        
        raise ValueError(f"Invalid month: {month}")
    
    return year, month


def parse_date_from_filename(filename: str, date_fmt: str = "%Y%m%d") -> Optional[datetime]:
    """Parse date from filename using the specified format.
    
    Args:
        filename: Filename to parse
        date_fmt: Date format string
    
    Returns:
        Parsed datetime or None if invalid
    """
    # Extract date pattern from filename
    # Assuming format like BASE_AT12_20250131.CSV
    pattern = r'_(\d{8})\.'
    match = re.search(pattern, filename.upper())
    
    if not match:
        return None
    
    date_str = match.group(1)
    
    try:
        return datetime.strptime(date_str, date_fmt)
    except ValueError:
        return None


def validate_period_coherence(filename: str, expected_year: int, expected_month: int) -> bool:
    """Validate that the date in filename matches the expected period.
    
    Args:
        filename: Filename to validate
        expected_year: Expected year
        expected_month: Expected month
    
    Returns:
        True if coherent, False otherwise
    """
    parsed_date = parse_date_from_filename(filename)
    
    if parsed_date is None:
        return False
    
    return (parsed_date.year == expected_year and 
            parsed_date.month == expected_month)


def format_period(year: int, month: int) -> str:
    """Format period as YYYY-MM string."""
    return f"{year:04d}-{month:02d}"


def format_period_compact(year: int, month: int) -> str:
    """Format period as YYYYMM string."""
    return f"{year:04d}{month:02d}"


def generate_run_id() -> str:
    """Generate a unique run ID based on current timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")