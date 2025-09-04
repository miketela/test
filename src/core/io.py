#!/usr/bin/env python3
"""
I/O utilities for SBP Atoms Pipeline.
Handles strict CSV and XLSX reading/writing with chunking support.
"""

import csv
import pandas as pd
from pathlib import Path
from typing import Iterator, List, Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass
from abc import ABC, abstractmethod
import chardet


def detect_file_encoding(file_path: Path, sample_size: int = 8192) -> str:
    """Detect file encoding using multiple methods.
    
    Args:
        file_path: Path to the file
        sample_size: Number of bytes to sample for detection
    
    Returns:
        Detected encoding string
    """
    # Common encodings to try in order of preference
    common_encodings = [
        'utf-8',
        'latin-1',
        'iso-8859-1', 
        'cp1252',
        'utf-16',
        'ascii'
    ]
    
    # First try chardet detection
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            if raw_data:
                detected = chardet.detect(raw_data)
                if detected and detected['encoding'] and detected['confidence'] > 0.7:
                    return detected['encoding']
    except Exception:
        pass
    
    # Fallback: try common encodings
    for encoding in common_encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(sample_size)
            return encoding
        except (UnicodeDecodeError, UnicodeError, FileNotFoundError, OSError):
            # If file doesn't exist or can't be read with this encoding, try next
            continue
    
    # Last resort: return utf-8 with error handling
    return 'utf-8'


@dataclass
class FileValidationResult:
    """Result of file validation (CSV or XLSX)."""
    is_valid: bool
    row_count: int
    column_count: int
    headers: List[str]
    warnings: List[str]
    errors: List[str]
    file_format: str  # 'csv' or 'xlsx'
    sheet_names: Optional[List[str]] = None  # For XLSX files


# Keep backward compatibility
CSVValidationResult = FileValidationResult


class BaseFileReader(ABC):
    """Abstract base class for file readers."""
    
    def __init__(self, chunk_size: int = 1000000):
        self.chunk_size = chunk_size
    
    @abstractmethod
    def validate_file(self, file_path: Path) -> FileValidationResult:
        """Validate file structure."""
        pass
    
    @abstractmethod
    def read_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """Read entire file into DataFrame."""
        pass
    
    @abstractmethod
    def read_chunks(self, file_path: Path, **kwargs) -> Iterator[pd.DataFrame]:
        """Read file in chunks."""
        pass
    
    @abstractmethod
    def read_sample(self, file_path: Path, sample_size: int = 100, **kwargs) -> pd.DataFrame:
        """Read a sample of rows from file."""
        pass
    
    @abstractmethod
    def count_records(self, file_path: Path, **kwargs) -> int:
        """Count total number of records in file."""
        pass


class StrictCSVReader(BaseFileReader):
    """Strict CSV reader with validation and chunking support."""
    
    def __init__(self, 
                 delimiter: str = ",",
                 encoding: Optional[str] = 'utf-8',
                 quotechar: str = '"',
                 chunk_size: int = 1000000,
                 auto_detect_encoding: bool = True,
                 auto_detect_delimiter: bool = True,
                 delimiter_candidates: Optional[List[str]] = None):
        """Initialize CSV reader.
        
        Args:
            delimiter: CSV delimiter
            encoding: File encoding (if None, will auto-detect)
            quotechar: Quote character
            chunk_size: Number of rows per chunk
            auto_detect_encoding: Whether to auto-detect encoding when encoding is None
        """
        super().__init__(chunk_size)
        self.delimiter = delimiter
        self.encoding = encoding
        self.quotechar = quotechar
        self.auto_detect_encoding = auto_detect_encoding
        self.auto_detect_delimiter = auto_detect_delimiter
        # Include common delimiters: comma, semicolon, pipe, tab, and space
        self.delimiter_candidates = delimiter_candidates or [',', ';', '|', '\t', ' ']
    
    def _get_file_encoding(self, file_path: Path) -> str:
        """Get the appropriate encoding for a file.
        
        Args:
            file_path: Path to the file
        
        Returns:
            Encoding string to use
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.debug(f"Getting encoding for {file_path.name}: encoding={self.encoding}, auto_detect={self.auto_detect_encoding}")
        
        if self.encoding is not None:
            logger.debug(f"Using specified encoding: {self.encoding}")
            return self.encoding
        
        if self.auto_detect_encoding:
            detected = detect_file_encoding(file_path)
            logger.debug(f"Auto-detected encoding: {detected}")
            return detected
        
        logger.debug("Using default fallback: utf-8")
        return 'utf-8'  # Default fallback
    
    def validate_file(self, file_path: Path) -> FileValidationResult:
        """Validate CSV file structure.
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            Validation result
        """
        warnings = []
        errors = []
        headers = []
        row_count = 0
        column_count = 0
        
        # Get the appropriate encoding for this file
        file_encoding = self._get_file_encoding(file_path)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        # Resolve delimiter for this file if enabled
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        try:
            # Log which delimiter will be used for this file
            mode = "auto" if getattr(self, 'auto_detect_delimiter', False) else "configured"
            msg = f"Detected CSV delimiter ({mode}) for {file_path.name}: '{delim}'"
            logger = logging.getLogger(__name__)
            # Only use INFO when auto-detected delimiter differs from configured; else DEBUG
            if getattr(self, 'auto_detect_delimiter', False) and delim != getattr(self, 'delimiter', ','):
                logger.info(msg)
            else:
                logger.debug(msg)
        except Exception:
            pass
        
        try:
            with open(file_path, 'r', encoding=file_encoding, newline='') as f:
                reader = csv.reader(f, delimiter=delim, quotechar=self.quotechar)
                
                # Read header
                try:
                    headers = next(reader)
                    column_count = len(headers)
                except StopIteration:
                    errors.append("File is empty")
                    return FileValidationResult(
                        is_valid=False,
                        row_count=0,
                        column_count=0,
                        headers=[],
                        warnings=warnings,
                        errors=errors,
                        file_format='csv'
                    )
                
                # Validate data rows
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                    row_count += 1
                    
                    if len(row) != column_count:
                        warnings.append(f"Row {row_num}: width mismatch (expected {column_count}, got {len(row)})")
                
        except UnicodeDecodeError as e:
            # If auto-detection is enabled and we get encoding error, try fallback encodings
            if self.auto_detect_encoding and self.encoding is None:
                fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
                for fallback_encoding in fallback_encodings:
                    if fallback_encoding != file_encoding:
                        try:
                            with open(file_path, 'r', encoding=fallback_encoding, newline='') as f:
                                reader = csv.reader(f, delimiter=delim, quotechar=self.quotechar)
                                
                                # Read header
                                try:
                                    headers = next(reader)
                                    column_count = len(headers)
                                    warnings.append(f"Used fallback encoding: {fallback_encoding} (original: {file_encoding})")
                                except StopIteration:
                                    continue  # Try next encoding
                                
                                # Validate data rows
                                for row_num, row in enumerate(reader, start=2):
                                    row_count += 1
                                    
                                    if len(row) != column_count:
                                        warnings.append(f"Row {row_num}: width mismatch (expected {column_count}, got {len(row)})")
                                
                                # If we get here, the fallback worked
                                break
                        except (UnicodeDecodeError, Exception):
                            continue  # Try next encoding
                else:
                    # All fallbacks failed
                    errors.append(f"Encoding error with {file_encoding}: {e}. All fallback encodings failed.")
            else:
                errors.append(f"Encoding error with {file_encoding}: {e}")
        except Exception as e:
            errors.append(f"Unexpected error: {e}")
        
        return FileValidationResult(
            is_valid=len(errors) == 0,
            row_count=row_count,
            column_count=column_count,
            headers=headers,
            warnings=warnings,
            errors=errors,
            file_format='csv'
        )
    
    # Keep backward compatibility
    def validate_csv(self, file_path: Path) -> FileValidationResult:
        """Validate CSV file structure (backward compatibility)."""
        return self.validate_file(file_path)
    
    def read_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """Read entire CSV file into DataFrame.
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            DataFrame with CSV data
        """
        file_encoding = self._get_file_encoding(file_path)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        
        try:
            return pd.read_csv(
                file_path,
                delimiter=delim,
                encoding=file_encoding,
                quotechar=self.quotechar,
                dtype=str,  # Read all as strings initially
                keep_default_na=False  # Don't convert to NaN
            )
        except pd.errors.ParserError:
            # Retry with python engine and tolerant bad line handling
            return pd.read_csv(
                file_path,
                delimiter=delim,
                encoding=file_encoding,
                quotechar=self.quotechar,
                dtype=str,
                keep_default_na=False,
                engine='python',
                on_bad_lines='warn'
            )
        except UnicodeDecodeError:
            # Try fallback encodings if auto-detection is enabled
            if self.auto_detect_encoding and self.encoding is None:
                fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
                for fallback_encoding in fallback_encodings:
                    if fallback_encoding != file_encoding:
                        try:
                            return pd.read_csv(
                                file_path,
                                delimiter=delim,
                                encoding=fallback_encoding,
                                quotechar=self.quotechar,
                                dtype=str,
                                keep_default_na=False
                            )
                        except UnicodeDecodeError:
                            continue
            # Re-raise the original error if all fallbacks fail
            raise
    
    def read_chunks(self, file_path: Path, **kwargs) -> Iterator[pd.DataFrame]:
        """Read CSV file in chunks.
        
        Args:
            file_path: Path to CSV file
        
        Yields:
            DataFrame chunks
        """
        file_encoding = self._get_file_encoding(file_path)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        
        try:
            chunk_reader = pd.read_csv(
                file_path,
                delimiter=delim,
                encoding=file_encoding,
                quotechar=self.quotechar,
                dtype=str,
                keep_default_na=False,
                chunksize=self.chunk_size
            )
            
            for chunk in chunk_reader:
                yield chunk
                
        except UnicodeDecodeError:
            # Try fallback encodings if auto-detection is enabled
            if self.auto_detect_encoding and self.encoding is None:
                fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
                for fallback_encoding in fallback_encodings:
                    if fallback_encoding != file_encoding:
                        try:
                            chunk_reader = pd.read_csv(
                                file_path,
                                delimiter=delim,
                                encoding=fallback_encoding,
                                quotechar=self.quotechar,
                                dtype=str,
                                keep_default_na=False,
                                chunksize=self.chunk_size
                            )
                            
                            for chunk in chunk_reader:
                                yield chunk
                            return  # Success, exit the function
                        except UnicodeDecodeError:
                            continue
            # Re-raise the original error if all fallbacks fail
            raise
    
    def read_sample(self, file_path: Path, sample_size: int = 100, **kwargs) -> pd.DataFrame:
        """Read a sample of rows from CSV file.
        
        Args:
            file_path: Path to CSV file
            sample_size: Number of rows to sample
        
        Returns:
            DataFrame with sample data
        """
        file_encoding = self._get_file_encoding(file_path)
        delim = self._resolve_csv_delimiter(file_path, file_encoding)
        
        try:
            return pd.read_csv(
                file_path,
                delimiter=delim,
                encoding=file_encoding,
                quotechar=self.quotechar,
                dtype=str,
                keep_default_na=False,
                nrows=sample_size
            )
        except UnicodeDecodeError:
            # Try fallback encodings if auto-detection is enabled
            if self.auto_detect_encoding and self.encoding is None:
                fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
                for fallback_encoding in fallback_encodings:
                    if fallback_encoding != file_encoding:
                        try:
                            return pd.read_csv(
                                file_path,
                                delimiter=delim,
                                encoding=fallback_encoding,
                                quotechar=self.quotechar,
                                dtype=str,
                                keep_default_na=False,
                                nrows=sample_size
                            )
                        except UnicodeDecodeError:
                            continue
            # Re-raise the original error if all fallbacks fail
            raise
    
    def count_records(self, file_path: Path, **kwargs) -> int:
        """Count total number of records in CSV file (excluding header).
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            Number of data records
        """
        file_encoding = self._get_file_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=file_encoding, newline='') as f:
                reader = csv.reader(f, delimiter=delim, quotechar=self.quotechar)
                # Skip header
                next(reader, None)
                # Count remaining rows
                return sum(1 for _ in reader)
        except UnicodeDecodeError:
            # Try fallback encodings if auto-detection is enabled
            if self.auto_detect_encoding and self.encoding is None:
                fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
                for fallback_encoding in fallback_encodings:
                    if fallback_encoding != file_encoding:
                        try:
                            with open(file_path, 'r', encoding=fallback_encoding, newline='') as f:
                                reader = csv.reader(f, delimiter=delim, quotechar=self.quotechar)
                                # Skip header
                                next(reader, None)
                                # Count remaining rows
                                return sum(1 for _ in reader)
                        except UnicodeDecodeError:
                            continue
            # Fallback: simple line count minus header
            try:
                with open(file_path, 'r', encoding=file_encoding, errors='ignore') as f:
                    total = sum(1 for _ in f)
                return max(total - 1, 0)
            except Exception:
                return 0
        except Exception:
            # Fallback: simple line count minus header on generic errors
            try:
                with open(file_path, 'r', encoding=file_encoding, errors='ignore') as f:
                    total = sum(1 for _ in f)
                return max(total - 1, 0)
            except Exception:
                return 0
    
    # Keep backward compatibility methods
    def read_csv(self, file_path: Path) -> pd.DataFrame:
        """Read entire CSV file into DataFrame (backward compatibility)."""
        return self.read_file(file_path)
    
    def read_csv_chunks(self, file_path: Path) -> Iterator[pd.DataFrame]:
         """Read CSV file in chunks (backward compatibility)."""
         return self.read_chunks(file_path)

    def _resolve_csv_delimiter(self, file_path: Path, file_encoding: Optional[str] = None) -> str:
        """Detect delimiter for CSV if enabled; fallback to configured delimiter."""
        if not getattr(self, 'auto_detect_delimiter', False):
            return self.delimiter
        try:
            # Read a few non-empty lines
            text = ''
            with open(file_path, 'r', encoding=file_encoding or self._get_file_encoding(file_path), newline='') as f:
                lines = []
                for _ in range(10):
                    line = f.readline()
                    if not line:
                        break
                    if line.strip():
                        lines.append(line)
                text = ''.join(lines)
            if text:
                # Try Sniffer
                try:
                    sniff = csv.Sniffer().sniff(text, delimiters=''.join(getattr(self, 'delimiter_candidates', [',',';','|','\t'])))
                    if sniff and getattr(sniff, 'delimiter', None) in getattr(self, 'delimiter_candidates', [',',';','|','\t']):
                        return sniff.delimiter
                except Exception:
                    pass
                # Heuristic: count candidates in header
                header = text.splitlines()[0] if text else ''
                candidates = getattr(self, 'delimiter_candidates', [',',';','|','\t'])
                counts = {d: header.count(d) for d in candidates}
                best = max(counts.items(), key=lambda kv: kv[1])
                if best[1] > 0:
                    return best[0]
        except Exception:
            pass
        return self.delimiter


class StrictXLSXReader(BaseFileReader):
    """Strict XLSX reader with validation and chunking support."""
    
    def __init__(self, chunk_size: int = 1000000, sheet_name: Union[str, int] = 0):
        super().__init__(chunk_size)
        self.sheet_name = sheet_name
    
    def validate_file(self, file_path: Path) -> FileValidationResult:
        """Validate XLSX file structure."""
        warnings = []
        errors = []
        headers = []
        row_count = 0
        column_count = 0
        sheet_names = []
        
        try:
            # Get sheet names
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            if not sheet_names:
                errors.append("No sheets found in Excel file")
                return FileValidationResult(
                    is_valid=False,
                    row_count=0,
                    column_count=0,
                    headers=[],
                    warnings=warnings,
                    errors=errors,
                    file_format='xlsx',
                    sheet_names=sheet_names
                )
            
            # Determine which sheet to validate
            target_sheet = self.sheet_name
            if isinstance(target_sheet, int):
                if target_sheet >= len(sheet_names):
                    errors.append(f"Sheet index {target_sheet} out of range (0-{len(sheet_names)-1})")
                    target_sheet = 0
            elif isinstance(target_sheet, str):
                if target_sheet not in sheet_names:
                    warnings.append(f"Sheet '{target_sheet}' not found, using first sheet '{sheet_names[0]}'")
                    target_sheet = 0
            
            # Read and validate the sheet
            df = pd.read_excel(file_path, sheet_name=target_sheet, dtype=str, keep_default_na=False)
            
            if df.empty:
                errors.append("Sheet is empty")
            else:
                headers = list(df.columns)
                column_count = len(headers)
                row_count = len(df)
                
                # Check for unnamed columns
                unnamed_cols = [col for col in headers if str(col).startswith('Unnamed:')]
                if unnamed_cols:
                    warnings.append(f"Found {len(unnamed_cols)} unnamed columns: {unnamed_cols}")
            
        except Exception as e:
            errors.append(f"Error reading Excel file: {e}")
        
        return FileValidationResult(
            is_valid=len(errors) == 0,
            row_count=row_count,
            column_count=column_count,
            headers=headers,
            warnings=warnings,
            errors=errors,
            file_format='xlsx',
            sheet_names=sheet_names
        )
    
    def read_file(self, file_path: Path, sheet_name: Union[str, int] = None, **kwargs) -> pd.DataFrame:
        """Read entire XLSX file into DataFrame."""
        target_sheet = sheet_name if sheet_name is not None else self.sheet_name
        return pd.read_excel(
            file_path,
            sheet_name=target_sheet,
            dtype=str,
            keep_default_na=False
        )
    
    def read_chunks(self, file_path: Path, sheet_name: Union[str, int] = None, **kwargs) -> Iterator[pd.DataFrame]:
        """Read XLSX file in chunks."""
        target_sheet = sheet_name if sheet_name is not None else self.sheet_name
        
        # For XLSX, we need to read the entire file first, then chunk it
        df = pd.read_excel(
            file_path,
            sheet_name=target_sheet,
            dtype=str,
            keep_default_na=False
        )
        
        # Yield chunks
        for i in range(0, len(df), self.chunk_size):
            yield df.iloc[i:i + self.chunk_size]
    
    def read_sample(self, file_path: Path, sample_size: int = 100, sheet_name: Union[str, int] = None, **kwargs) -> pd.DataFrame:
        """Read a sample of rows from XLSX file."""
        target_sheet = sheet_name if sheet_name is not None else self.sheet_name
        return pd.read_excel(
            file_path,
            sheet_name=target_sheet,
            dtype=str,
            keep_default_na=False,
            nrows=sample_size
        )
    
    def count_records(self, file_path: Path, sheet_name: Union[str, int] = None, **kwargs) -> int:
        """Count total number of records in XLSX file."""
        try:
            target_sheet = sheet_name if sheet_name is not None else self.sheet_name
            df = pd.read_excel(file_path, sheet_name=target_sheet, dtype=str, keep_default_na=False)
            return len(df)
        except Exception:
            return 0


class UniversalFileReader:
    """Universal file reader that automatically detects CSV or XLSX format."""
    
    def __init__(self, 
                 csv_delimiter: str = ",",
                 csv_encoding: Optional[str] = None,
                 csv_quotechar: str = '"',
                 xlsx_sheet_name: Union[str, int] = 0,
                 chunk_size: int = 1000000):
        
        self.csv_reader = StrictCSVReader(
            delimiter=csv_delimiter,
            encoding=csv_encoding,
            quotechar=csv_quotechar,
            chunk_size=chunk_size,
            auto_detect_encoding=True if csv_encoding is None else False
        )
        
        self.xlsx_reader = StrictXLSXReader(
            chunk_size=chunk_size,
            sheet_name=xlsx_sheet_name
        )
    
    def detect_format(self, file_path: Path) -> str:
        """Detect file format based on extension."""
        suffix = file_path.suffix.lower()
        if suffix in ['.csv', '.txt']:
            return 'csv'
        elif suffix in ['.xlsx', '.xls']:
            return 'xlsx'
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
    
    def get_reader(self, file_path: Path) -> BaseFileReader:
        """Get appropriate reader for file format."""
        format_type = self.detect_format(file_path)
        if format_type == 'csv':
            return self.csv_reader
        elif format_type == 'xlsx':
            return self.xlsx_reader
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def validate_file(self, file_path: Path) -> FileValidationResult:
        """Validate file regardless of format."""
        reader = self.get_reader(file_path)
        return reader.validate_file(file_path)
    
    def read_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """Read file regardless of format."""
        reader = self.get_reader(file_path)
        return reader.read_file(file_path, **kwargs)
    
    def read_chunks(self, file_path: Path, **kwargs) -> Iterator[pd.DataFrame]:
        """Read file in chunks regardless of format."""
        reader = self.get_reader(file_path)
        return reader.read_chunks(file_path, **kwargs)
    
    def read_sample(self, file_path: Path, sample_size: int = 100, **kwargs) -> pd.DataFrame:
        """Read sample regardless of format."""
        reader = self.get_reader(file_path)
        return reader.read_sample(file_path, sample_size, **kwargs)
    
    def count_records(self, file_path: Path, **kwargs) -> int:
        """Count records regardless of format."""
        reader = self.get_reader(file_path)
        return reader.count_records(file_path, **kwargs)


class StrictCSVWriter:
    """Strict CSV writer with configurable output format."""
    
    def __init__(self, 
                 delimiter: str = "|",
                 encoding: str = "utf-8",
                 quotechar: str = '"',
                 trailing_delimiter: bool = False):
        """Initialize CSV writer.
        
        Args:
            delimiter: Output delimiter
            encoding: File encoding
            quotechar: Quote character
            trailing_delimiter: Whether to add delimiter at end of each row
        """
        self.delimiter = delimiter
        self.encoding = encoding
        self.quotechar = quotechar
        self.trailing_delimiter = trailing_delimiter
    
    def write_csv(self, data: pd.DataFrame, file_path: Path, include_header: bool = True):
        """Write DataFrame to CSV file.
        
        Args:
            data: DataFrame to write
            file_path: Output file path
            include_header: Whether to include header row
        """
        # Ensure output directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.trailing_delimiter:
            # Custom writing with trailing delimiter
            self._write_with_trailing_delimiter(data, file_path, include_header)
        else:
            # Standard pandas to_csv
            data.to_csv(
                file_path,
                sep=self.delimiter,
                encoding=self.encoding,
                quotechar=self.quotechar,
                index=False,
                header=include_header,
                quoting=csv.QUOTE_MINIMAL
            )
    
    def _write_with_trailing_delimiter(self, data: pd.DataFrame, file_path: Path, include_header: bool):
        """Write CSV with trailing delimiter on each row."""
        with open(file_path, 'w', encoding=self.encoding, newline='') as f:
            writer = csv.writer(f, delimiter=self.delimiter, quotechar=self.quotechar, quoting=csv.QUOTE_MINIMAL)
            
            # Write header if requested
            if include_header:
                header_row = list(data.columns) + ['']
                f.write(self.delimiter.join(str(col) for col in data.columns) + self.delimiter + '\n')
            
            # Write data rows
            for _, row in data.iterrows():
                row_str = self.delimiter.join(str(val) for val in row.values) + self.delimiter
                f.write(row_str + '\n')


def infer_data_types(df: pd.DataFrame) -> Dict[str, str]:
    """Infer data types for DataFrame columns.
    
    Note: For regulatory data, we treat all columns as strings to avoid
    automatic type inference issues, especially with date fields in YYYYMMDD format
    which are technically integers but have specific semantic meaning.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        Dictionary mapping column names to inferred types (all as 'string')
    """
    type_mapping = {}
    
    for col in df.columns:
        # Treat all columns as strings to avoid type inference warnings
        # and maintain data integrity for regulatory formats
        type_mapping[col] = 'string'
    
    return type_mapping
