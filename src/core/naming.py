#!/usr/bin/env python3
"""
Naming utilities for SBP Atoms Pipeline.
Handles filename normalization and parsing.
"""

import re
import unicodedata
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class ParsedFilename:
    """Parsed filename components."""
    original_name: str
    normalized_name: str
    subtype: str
    date_str: str
    date: datetime
    year: int
    month: int
    day: int
    extension: str
    is_valid: bool
    errors: List[str]


class FilenameParser:
    """Parser for AT12 filename patterns."""
    
    DEFAULT_ALIASES = {
        'GARANTIAS_AUTOS_AT12': 'GARANTIA_AUTOS_AT12',
    }

    def __init__(self, expected_subtypes: List[str], aliases: Optional[Dict[str, str]] = None):
        """Initialize filename parser.

        Args:
            expected_subtypes: List of expected subtype names
        """
        base_subtypes = [subtype.upper() for subtype in expected_subtypes]
        alias_map = aliases or {}
        alias_map = {alias.upper(): target.upper() for alias, target in alias_map.items()}
        # merge with defaults (explicit aliases override defaults if provided)
        for alias, target in self.DEFAULT_ALIASES.items():
            alias_map.setdefault(alias.upper(), target.upper())

        self.alias_map = alias_map

        # include alias keys in expected list to make regex matching possible
        self.expected_subtypes = base_subtypes.copy()
        for alias in self.alias_map.keys():
            if alias not in self.expected_subtypes:
                self.expected_subtypes.append(alias)

    def normalize_filename(self, filename: str) -> str:
        """Normalize filename to uppercase.
        
        Args:
            filename: Original filename
        
        Returns:
            Normalized filename in uppercase
        """
        return filename.upper()
    
    def parse_filename(self, filename: str) -> ParsedFilename:
        """Parse filename according to pattern [SUBTYPE]_[YYYYMMDD].CSV.
        
        Args:
            filename: Filename to parse
        
        Returns:
            ParsedFilename object with parsing results
        """
        errors = []
        original_name = filename
        normalized_name = self.normalize_filename(filename)
        
        # Try to match against known subtypes first
        subtype = ""
        date_str = ""
        extension = ""
        
        # Pattern: SUBTYPE_YYYYMMDD.CSV or SUBTYPE_YYYYMMDD__RUN-RUNID.CSV where SUBTYPE is from expected list
        for expected_subtype in self.expected_subtypes:
            # First try pattern with __RUN- suffix
            pattern_with_run = f'^{re.escape(expected_subtype)}_(\\d{{8}})__RUN-[^.]+\\.(CSV|TXT)$'
            match = re.match(pattern_with_run, normalized_name)
            if match:
                subtype = expected_subtype
                date_str = match.group(1)
                extension = match.group(2)
                break
            
            # Then try basic pattern without suffix
            pattern_basic = f'^{re.escape(expected_subtype)}_(\\d{{8}})\\.(CSV|TXT)$'
            match = re.match(pattern_basic, normalized_name)
            if match:
                subtype = expected_subtype
                date_str = match.group(1)
                extension = match.group(2)
                break
        
        if not subtype:
            errors.append(f"Filename does not match any expected subtype pattern: {normalized_name}")
        
        # Validate date
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            errors.append(f"Invalid date format: {date_str}")
            date_obj = datetime.min
        
        canonical_subtype = self.alias_map.get(subtype, subtype)

        return ParsedFilename(
            original_name=original_name,
            normalized_name=normalized_name,
            subtype=canonical_subtype,
            date_str=date_str,
            date=date_obj,
            year=date_obj.year if date_obj != datetime.min else 0,
            month=date_obj.month if date_obj != datetime.min else 0,
            day=date_obj.day if date_obj != datetime.min else 0,
            extension=extension,
            is_valid=len(errors) == 0,
            errors=errors
        )
    
    def validate_period_coherence(self, parsed_filename: ParsedFilename, 
                                expected_year: int, expected_month: int) -> bool:
        """Validate that filename date matches expected period.
        
        Args:
            parsed_filename: Parsed filename object
            expected_year: Expected year
            expected_month: Expected month
        
        Returns:
            True if coherent, False otherwise
        """
        if not parsed_filename.is_valid:
            return False
        
        return (parsed_filename.year == expected_year and 
                parsed_filename.month == expected_month)
    
    def find_most_recent_duplicate(self, parsed_files: List[ParsedFilename]) -> Dict[str, ParsedFilename]:
        """Find most recent file for each subtype (handles duplicates).
        
        Args:
            parsed_files: List of parsed filename objects
        
        Returns:
            Dictionary mapping subtype to most recent ParsedFilename
        """
        subtype_files = {}
        
        for parsed_file in parsed_files:
            if not parsed_file.is_valid:
                continue
            
            subtype = parsed_file.subtype
            
            if subtype not in subtype_files:
                subtype_files[subtype] = parsed_file
            else:
                # Compare dates and keep the most recent
                current_file = subtype_files[subtype]
                if parsed_file.date > current_file.date:
                    subtype_files[subtype] = parsed_file
                # If dates are equal, keep the first one (current_file) and ignore duplicates
                # This handles cases where multiple identical files exist
        
        return subtype_files
    
    def generate_output_filename(self, atom: str, year: int, month: int, 
                               run_id: str, extension: str = "txt") -> str:
        """Generate output filename for consolidated data.
        
        Args:
            atom: Atom name (e.g., 'AT12')
            year: Year
            month: Month
            run_id: Run ID
            extension: File extension
        
        Returns:
            Generated filename
        """
        if atom == "AT12":
            # Special case for AT12 -> AT12_Cobis.txt
            base_name = f"{atom}_Cobis_{year:04d}{month:02d}__run-{run_id}"
        else:
            base_name = f"{atom}_CONSOLIDATED_{year:04d}{month:02d}__run-{run_id}"
        
        return f"{base_name}.{extension.upper()}"
    
    def generate_report_filename(self, atom: str, process_type: str, 
                               year: int, month: int, run_id: str) -> str:
        """Generate report filename.
        
        Args:
            atom: Atom name
            process_type: 'exploration' or 'transformation'
            year: Year
            month: Month
            run_id: Run ID
        
        Returns:
            Generated report filename
        """
        return f"{process_type}_{atom}_{year:04d}-{month:02d}__run-{run_id}.pdf"


class HeaderNormalizer:
    """Normalizer for CSV headers."""
    
    @staticmethod
    def remove_accents(text: str) -> str:
        """Remove accents and tildes from text.
        
        Args:
            text: Text with potential accents/tildes
        
        Returns:
            Text without accents/tildes
        """
        # Normalize to NFD (decomposed form) and filter out combining characters
        nfd = unicodedata.normalize('NFD', text)
        without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
        return without_accents
    
    @staticmethod
    def clean_header_text(text: str) -> str:
        """Clean header text by removing parenthetical numbers and extra spaces.
        
        Args:
            text: Original header text
        
        Returns:
            Cleaned header text
        """
        # Remove BOM if present
        if text and text[0] == '\ufeff':  # ZERO WIDTH NO-BREAK SPACE (BOM)
            text = text.lstrip('\ufeff')
        # Remove parenthetical numbers like (0), (1), (2), etc.
        cleaned = re.sub(r'\(\d+\)', '', text)
        
        # Remove extra whitespace (leading, trailing, and multiple spaces)
        cleaned = re.sub(r'\s+', ' ', cleaned.strip())
        
        return cleaned
    
    @staticmethod
    def normalize_headers(headers: List[str]) -> List[str]:
        """Normalize CSV headers.
        
        Args:
            headers: Original headers
        
        Returns:
            Normalized headers
        """
        normalized = []
        
        for header in headers:
            # First clean the header text (remove parenthetical numbers and extra spaces)
            cleaned_header = HeaderNormalizer.clean_header_text(header)
            
            # Strip whitespace and normalize case
            normalized_header = cleaned_header.strip()
            
            # Remove accents and tildes
            normalized_header = HeaderNormalizer.remove_accents(normalized_header)
            
            # Replace spaces with underscores and clean special characters
            normalized_header = normalized_header.replace(' ', '_')
            normalized_header = re.sub(r'[^A-Za-z0-9_]', '_', normalized_header)
            normalized_header = re.sub(r'_+', '_', normalized_header)  # Multiple underscores to single
            normalized_header = normalized_header.strip('_')  # Remove leading/trailing underscores
            
            normalized.append(normalized_header)
        
        return normalized
    
    @staticmethod
    def validate_headers_against_schema(headers: List[str], 
                                      expected_headers: List[str], 
                                      order_strict: bool = True) -> Dict[str, Any]:
        """Validate headers against expected schema.
        
        Args:
            headers: Actual headers from file
            expected_headers: Expected headers from schema
            order_strict: Whether order must match exactly
        
        Returns:
            Dictionary with validation results
        """
        result = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'missing_headers': [],
            'extra_headers': [],
            'order_issues': []
        }
        
        # Check for missing headers
        missing = set(expected_headers) - set(headers)
        if missing:
            result['missing_headers'] = list(missing)
            result['errors'].append(f"Missing required headers: {', '.join(missing)}")
            result['is_valid'] = False
        
        # Check for extra headers
        extra = set(headers) - set(expected_headers)
        if extra:
            result['extra_headers'] = list(extra)
            result['warnings'].append(f"Extra headers found: {', '.join(extra)}")
        
        # Check order if strict
        if order_strict and not missing:
            # Only check order for headers that exist in both lists
            common_headers = [h for h in expected_headers if h in headers]
            actual_order = [h for h in headers if h in expected_headers]
            
            if common_headers != actual_order:
                result['order_issues'] = {
                    'expected_order': common_headers,
                    'actual_order': actual_order
                }
                result['warnings'].append("Header order does not match expected schema")
        
        return result
