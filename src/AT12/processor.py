#!/usr/bin/env python3
"""
AT12 Processor for SBP Atoms Pipeline.
Handles exploration and transformation of AT12 regulatory atoms.
"""

import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict

from ..core.log import get_logger
from ..core.config import Config
from ..core.fs import copy_with_versioning, get_file_info, find_files_by_pattern
from ..core.io import StrictCSVReader, StrictCSVWriter, UniversalFileReader
from ..core.metrics import MetricsCalculator, FileMetrics
from ..core.naming import FilenameParser, HeaderNormalizer, ParsedFilename
from ..core.header_mapping import HeaderMapper
from ..core.time_utils import format_period


@dataclass
class ProcessingResult:
    """Result of processing operation."""
    success: bool
    message: str
    files_processed: int
    total_records: int
    errors: List[str]
    warnings: List[str]
    metrics: Optional[Dict[str, Any]] = None
    output_files: List[str] = None

    def __post_init__(self):
        if self.output_files is None:
            self.output_files = []


class AT12Processor:
    """Processor for AT12 regulatory atoms."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize AT12 processor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = get_logger(__name__)
        self.atom_name = "AT12"
        
        # Load schemas
        self._load_schemas()
        
        # Initialize file reader/writer
        self.file_reader = UniversalFileReader(
            csv_delimiter=config.get('csv_delimiter', ','),
            csv_encoding=config.get('encoding'),
            chunk_size=config.get('chunk_size', 10000)
        )
        
        # Keep CSV reader for backward compatibility
        self.csv_reader = self.file_reader.csv_reader
        
        self.csv_writer = StrictCSVWriter(
            delimiter=config.get('output_delimiter', '|'),
            trailing_delimiter=config.get('trailing_delimiter', False)
        )
        
        # Initialize filename parser
        expected_subtypes = list(self.expected_files['subtypes'].keys())
        self.filename_parser = FilenameParser(expected_subtypes)
        
        # Initialize metrics calculator
        self.metrics_calculator = MetricsCalculator(self.file_reader)
    
    def _load_schemas(self):
        """Load schema files."""
        # Prefer explicit schemas_dir from config, resolve relative to base_dir if needed
        schemas_root_cfg = self.config.get('schemas_dir')
        base_dir_cfg = Path(self.config.get('base_dir', os.getcwd()))
        if schemas_root_cfg:
            root_path = Path(schemas_root_cfg)
            if not root_path.is_absolute():
                root_path = base_dir_cfg / root_path
        else:
            root_path = base_dir_cfg / 'schemas'
        schema_dir = root_path / self.atom_name
        
        # Load expected files (fallback to project schemas if not present in custom schemas_dir)
        expected_files_path = schema_dir / 'expected_files.json'
        try:
            with open(expected_files_path, 'r', encoding='utf-8') as f:
                self.expected_files = json.load(f)
        except FileNotFoundError:
            fallback_expected = Path(self.config.get('base_dir', os.getcwd())) / 'schemas' / self.atom_name / 'expected_files.json'
            if fallback_expected.exists():
                self.logger.warning(f"Expected files not found in custom schemas; using fallback: {fallback_expected}")
                with open(fallback_expected, 'r', encoding='utf-8') as f:
                    self.expected_files = json.load(f)
            else:
                self.logger.error(f"Expected files schema not found: {expected_files_path}")
                raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in expected files schema: {e}")
            raise
        
        # Load schema headers
        schema_headers_path = schema_dir / 'schema_headers.json'
        try:
            with open(schema_headers_path, 'r', encoding='utf-8') as f:
                self.schema_headers = json.load(f)
        except FileNotFoundError:
            # Fallback to project schemas
            fallback_headers = Path(self.config.get('base_dir', os.getcwd())) / 'schemas' / self.atom_name / 'schema_headers.json'
            if fallback_headers.exists():
                self.logger.warning(f"Schema headers not found in custom schemas; using fallback: {fallback_headers}")
                with open(fallback_headers, 'r', encoding='utf-8') as f:
                    self.schema_headers = json.load(f)
            else:
                self.logger.warning(f"Schema headers not found: {schema_headers_path}")
                self.schema_headers = {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in schema headers: {e}")
            raise
    
    def explore(self, year: int, month: int, run_id: str) -> ProcessingResult:
        """Execute exploration phase for AT12.
        
        Args:
            year: Processing year
            month: Processing month
            run_id: Unique run identifier
        
        Returns:
            ProcessingResult with exploration results
        """
        self.logger.info(f"Starting AT12 exploration for {format_period(year, month)}")
        
        try:
            # Phase 1: Discovery
            discovery_result = self._discover_files(year, month)
            if not discovery_result.success:
                return discovery_result
            
            # Phase 2: Validation
            validation_result = self._validate_files(discovery_result.output_files, year, month)
            if not validation_result.success:
                return validation_result
            
            # Phase 3: Copy with versioning
            versioning_result = self._copy_with_versioning(validation_result.output_files, run_id)
            if not versioning_result.success:
                return versioning_result
            
            # Phase 4: Analysis and metrics
            analysis_result = self._analyze_files(versioning_result.output_files, year, month, run_id)
            
            # Combine results
            total_records = sum([r.total_records for r in [discovery_result, validation_result, versioning_result, analysis_result]])
            all_errors = []
            all_warnings = []
            
            for result in [discovery_result, validation_result, versioning_result, analysis_result]:
                all_errors.extend(result.errors)
                all_warnings.extend(result.warnings)
            
            final_result = ProcessingResult(
                success=True,
                message=f"AT12 exploration completed successfully for {format_period(year, month)}",
                files_processed=len(versioning_result.output_files),
                total_records=total_records,
                errors=all_errors,
                warnings=all_warnings,
                metrics=analysis_result.metrics,
                output_files=versioning_result.output_files
            )
            
            self.logger.info(f"AT12 exploration completed: {final_result.files_processed} files, {final_result.total_records} records")
            return final_result
            
        except Exception as e:
            self.logger.error(f"AT12 exploration failed: {str(e)}")
            return ProcessingResult(
                success=False,
                message=f"AT12 exploration failed: {str(e)}",
                files_processed=0,
                total_records=0,
                errors=[str(e)],
                warnings=[]
            )
    
    def _discover_files(self, year: int, month: int) -> ProcessingResult:
        """Discover AT12 files in source directory.
        
        Args:
            year: Processing year
            month: Processing month
        
        Returns:
            ProcessingResult with discovered files
        """
        self.logger.info("Starting file discovery")
        
        source_dir = Path(self.config['source_dir'])
        if not source_dir.exists():
            return ProcessingResult(
                success=False,
                message=f"Source directory not found: {source_dir}",
                files_processed=0,
                total_records=0,
                errors=[f"Source directory not found: {source_dir}"],
                warnings=[]
            )
        
        # Find all CSV/TXT files (case-insensitive)
        csv_files = []
        for patt in ("*.csv", "*.CSV", "*.txt", "*.TXT"):
            csv_files.extend(find_files_by_pattern(source_dir, patt))
        
        # Remove duplicates that may occur on case-insensitive filesystems
        unique_files = []
        seen_paths = set()
        for file_path in csv_files:
            # Use resolved path to handle case-insensitive duplicates
            resolved_path = file_path.resolve()
            if resolved_path not in seen_paths:
                seen_paths.add(resolved_path)
                unique_files.append(file_path)
        
        csv_files = unique_files
        
        if not csv_files:
            return ProcessingResult(
                success=False,
                message="No source files found in source directory",
                files_processed=0,
                total_records=0,
                errors=["No source files found in source directory"],
                warnings=[]
            )
        
        # Parse filenames
        parsed_files = []
        errors = []
        warnings = []
        
        for file_path in csv_files:
            filename = file_path.name
            parsed = self.filename_parser.parse_filename(filename)
            
            if not parsed.is_valid:
                errors.extend([f"{filename}: {error}" for error in parsed.errors])
                continue
            
            # Check period coherence
            if not self.filename_parser.validate_period_coherence(parsed, year, month):
                warnings.append(f"{filename}: Date does not match expected period {year}-{month:02d}")
                continue
            
            parsed_files.append((file_path, parsed))
        
        if not parsed_files:
            return ProcessingResult(
                success=False,
                message="No valid AT12 files found for the specified period",
                files_processed=0,
                total_records=0,
                errors=errors,
                warnings=warnings
            )
        
        # Handle duplicates - keep most recent
        self.logger.info(f"Before deduplication: {len(parsed_files)} files")
        for i, (file_path, parsed) in enumerate(parsed_files, 1):
            self.logger.info(f"  {i}. {file_path.name} -> subtype: {parsed.subtype}, date: {parsed.date_str}")
        
        subtype_files = self.filename_parser.find_most_recent_duplicate([p[1] for p in parsed_files])
        
        self.logger.info(f"After deduplication: {len(subtype_files)} unique subtypes")
        for subtype, parsed in subtype_files.items():
            self.logger.info(f"  Selected {subtype}: {parsed.original_name} (date: {parsed.date_str})")
        
        # Map back to file paths
        final_files = []
        for file_path, parsed in parsed_files:
            if subtype_files.get(parsed.subtype) == parsed:
                final_files.append(file_path)
        
        self.logger.info(f"Final files after mapping: {len(final_files)} files")
        
        # Log all discovered files
        self.logger.info(f"Found {len(csv_files)} source files in source directory:")
        for i, file_path in enumerate(csv_files, 1):
            self.logger.info(f"  {i}. {file_path.name}")
        
        # Log final selected files
        self.logger.info(f"Selected {len(final_files)} valid AT12 files for processing:")
        for i, file_path in enumerate(final_files, 1):
            self.logger.info(f"  {i}. {file_path.name}")
        
        return ProcessingResult(
            success=True,
            message=f"Discovered {len(final_files)} valid AT12 files",
            files_processed=len(final_files),
            total_records=0,
            errors=errors,
            warnings=warnings,
            output_files=[str(f) for f in final_files]
        )
    
    def _validate_files(self, file_paths: List[str], year: int, month: int) -> ProcessingResult:
        """Validate AT12 files structure and headers.
        
        Args:
            file_paths: List of file paths to validate
            year: Processing year
            month: Processing month
        
        Returns:
            ProcessingResult with validation results
        """
        self.logger.info("Starting file validation")
        
        errors = []
        warnings = []
        valid_files = []
        failed_files = []
        total_records = 0
        file_record_counts = {}
        
        for file_path in file_paths:
            try:
                file_path_obj = Path(file_path)
                filename = file_path_obj.name
                parsed = self.filename_parser.parse_filename(filename)
                
                if not parsed.is_valid:
                    self.logger.error(f"✗ {filename} failed filename parsing:")
                    for error in parsed.errors:
                        self.logger.error(f"    - {error}")
                    errors.append(f"{filename}: Invalid filename format")
                    failed_files.append(filename)
                    continue
                
                # Validate file can be read
                try:
                    # Use universal reader to support CSV and XLSX
                    df_sample = self.file_reader.read_sample(file_path_obj, sample_size=100)
                    if df_sample.empty:
                        self.logger.error(f"✗ {filename} failed validation: File appears to be empty")
                        warnings.append(f"{filename}: File appears to be empty")
                        failed_files.append(filename)
                        continue
                    
                    # Count total records
                    record_count = self.file_reader.count_records(file_path_obj)
                    file_record_counts[filename] = record_count
                    total_records += record_count
                    
                    # Validate headers if schema available
                    expected_headers: Optional[List[str]] = None
                    # Prefer subtype-specific schema; fallback to global 'required_headers' (test format)
                    if isinstance(self.schema_headers, dict) and parsed.subtype in self.schema_headers:
                        expected_headers = list(self.schema_headers[parsed.subtype].keys())
                    elif isinstance(self.schema_headers, dict) and 'required_headers' in self.schema_headers:
                        expected_headers = self.schema_headers['required_headers']
                    
                    if expected_headers is not None:
                        actual_headers = list(df_sample.columns)
                        # Build standardization plan against schema for this subtype
                        try:
                            # Prepare synonym map for subtype when available
                            syn_map = HeaderMapper.TDC_AT12_MAPPING if parsed.subtype == 'TDC_AT12' else {}
                            selectors, std_report, extras = HeaderMapper.build_schema_standardization(
                                actual_headers, expected_headers, parsed.subtype, synonym_map=syn_map
                            )
                            # Log concise summary
                            kept = sum(1 for r in std_report if r.get('action') == 'kept')
                            added = sum(1 for r in std_report if r.get('action') == 'added')
                            dropped = sum(1 for r in std_report if r.get('action') == 'dropped')
                            self.logger.info(
                                f"Header standardization for {parsed.subtype}: kept={kept}, added={added}, dropped={dropped}"
                            )
                            # Write mapping report CSV to incidencias folder
                            try:
                                from ..core.paths import AT12Paths
                                from ..core.config import Config as _Cfg
                                cfg = _Cfg()
                                for k, v in self.config.items():
                                    if hasattr(cfg, k):
                                        setattr(cfg, k, v)
                                paths = AT12Paths.from_config(cfg)
                                paths.ensure_directories()
                                import pandas as _pd
                                rep_df = _pd.DataFrame(std_report)
                                rep_path = paths.incidencias_dir / f"HEADER_STANDARDIZATION_{parsed.subtype}_{parsed.date_str}.csv"
                                rep_df.to_csv(rep_path, index=False, encoding='utf-8', sep=self.csv_writer.delimiter, quoting=1)
                                self.logger.info(f"HEADER_STANDARDIZATION -> {rep_path.name} ({len(rep_df)} mappings)")
                            except Exception:
                                pass
                        except Exception as _e:
                            self.logger.warning(f"Header standardization step failed for {filename}: {_e}")
                    
                    valid_files.append(file_path)
                    self.logger.info(f"✓ Validated {filename}: {record_count:,} records")
                    
                except Exception as e:
                    self.logger.error(f"✗ {filename} failed validation: Failed to read file - {str(e)}")
                    errors.append(f"{filename}: Failed to read file - {str(e)}")
                    failed_files.append(filename)
                    continue
                    
            except Exception as e:
                errors.append(f"{file_path}: Validation error - {str(e)}")
                failed_files.append(Path(file_path).name)
                continue
        
        if not valid_files:
            return ProcessingResult(
                success=False,
                message="No valid files passed validation",
                files_processed=0,
                total_records=0,
                errors=errors,
                warnings=warnings
            )
        
        # Log validation summary
        if valid_files:
            self.logger.info(f"Validation completed: {len(valid_files)} files passed, {len(failed_files)} files failed")
            self.logger.info("Files that passed validation:")
            for file_path in valid_files:
                filename = Path(file_path).name
                record_count = file_record_counts.get(filename, 0)
                self.logger.info(f"  ✓ {filename} ({record_count:,} records)")
        
        if failed_files:
            self.logger.info("Files that failed validation:")
            for filename in failed_files:
                self.logger.info(f"  ✗ {filename}")
        
        self.logger.info(f"Total records across all validated files: {total_records:,}")
        
        return ProcessingResult(
            success=True,
            message=f"Validated {len(valid_files)} files",
            files_processed=len(valid_files),
            total_records=total_records,
            errors=errors,
            warnings=warnings,
            output_files=valid_files
        )
    
    def _copy_with_versioning(self, file_paths: List[str], run_id: str) -> ProcessingResult:
        """Copy files to data directory with versioning.
        
        Args:
            file_paths: List of source file paths
            run_id: Unique run identifier
        
        Returns:
            ProcessingResult with copied file paths
        """
        self.logger.info("Starting file copying with versioning")
        
        data_dir = Path(self.config['data_raw_dir'])
        data_dir.mkdir(parents=True, exist_ok=True)
        
        copied_files = []
        errors = []
        warnings = []
        
        for file_path in file_paths:
            try:
                source_path = Path(file_path)
                
                # Generate destination filename with run_id
                base_name = source_path.stem
                extension = source_path.suffix.lower()

                # Convert TXT inputs to CSV in RAW to unify downstream handling
                if extension == '.txt':
                    dest_filename = f"{base_name}__run-{run_id}.csv"
                    dest_path = data_dir / dest_filename
                    # Read TXT with auto-detected encoding and delimiter, then write CSV UTF-8
                    import pandas as _pd
                    try:
                        csv_reader = self.file_reader.csv_reader
                        file_encoding = csv_reader._get_file_encoding(source_path)
                        sep = csv_reader._resolve_csv_delimiter(source_path, file_encoding)
                        if sep == ' ':
                            df = _pd.read_csv(source_path, dtype=str, header=0, sep=r'\s+', engine='python', keep_default_na=False, encoding=file_encoding)
                        else:
                            df = _pd.read_csv(source_path, dtype=str, header=0, sep=sep, engine='python', keep_default_na=False, encoding=file_encoding)
                    except Exception:
                        # Fallback to UTF-16 with whitespace
                        df = _pd.read_csv(source_path, dtype=str, header=0, sep=r'\s+', engine='python', keep_default_na=False, encoding='utf-16')
                    # Standardize columns to schema and enforce dot decimals before saving to RAW CSV
                    try:
                        # Parse subtype from filename
                        from ..core.naming import FilenameParser as _FP
                        from ..core.header_mapping import HeaderMapper as _HM
                        # Use existing parser if available
                        parsed = self.filename_parser.parse_filename(source_path.name)
                        subtype = parsed.subtype if parsed and parsed.is_valid else None
                        if subtype and isinstance(self.schema_headers, dict) and subtype in self.schema_headers:
                            expected = list(self.schema_headers[subtype].keys())
                            df = _HM.standardize_dataframe_to_schema(df, subtype, expected)
                    except Exception as e:
                        self.logger.warning(f"Schema standardization skipped for {source_path.name}: {e}")
                    try:
                        money_candidates = [
                            'Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe',
                            'Monto', 'Monto_Pignorado', 'Intereses_por_Pagar', 'Importe_por_pagar',
                            'valor_inicial', 'intereses_x_cobrar', 'saldo', 'provision', 'provison_NIIF', 'provision_no_NIIF',
                            'mto_garantia_1', 'mto_garantia_2', 'mto_garantia_3', 'mto_garantia_4', 'mto_garantia_5',
                            'LIMITE', 'SALDO'
                        ]
                        for col in money_candidates:
                            if col in df.columns:
                                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                    except Exception:
                        pass
                    # Save as CSV (comma delimiter) in UTF-8 to RAW
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(dest_path, index=False, encoding='utf-8')
                    copied_files.append(str(dest_path))
                    self.logger.info(f"Converted {source_path.name} → {dest_path.name}")
                else:
                    # Normalize/standardize CSV sources; copy others as-is
                    if extension in ('.csv', '.CSV'):
                        dest_filename = f"{base_name}__run-{run_id}.csv"
                        dest_path = data_dir / dest_filename
                        import pandas as _pd
                        try:
                            csv_reader = self.file_reader.csv_reader
                            file_encoding = csv_reader._get_file_encoding(source_path)
                            sep = csv_reader._resolve_csv_delimiter(source_path, file_encoding)
                            df = _pd.read_csv(
                                source_path,
                                dtype=str,
                                header=0,
                                sep=sep,
                                engine='python',
                                keep_default_na=False,
                                encoding=file_encoding
                            )
                            # Standardize columns to schema when possible
                            try:
                                from ..core.header_mapping import HeaderMapper as _HM
                                parsed = self.filename_parser.parse_filename(source_path.name)
                                subtype = parsed.subtype if parsed and parsed.is_valid else None
                                if subtype and isinstance(self.schema_headers, dict) and subtype in self.schema_headers:
                                    expected = list(self.schema_headers[subtype].keys())
                                    df = _HM.standardize_dataframe_to_schema(df, subtype, expected)
                            except Exception as se:
                                self.logger.warning(f"Schema standardization skipped for {source_path.name}: {se}")
                            # Enforce dot decimals on common monetary columns
                            money_candidates = [
                                'Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe',
                                'Monto', 'Monto_Pignorado', 'Intereses_por_Pagar', 'Importe_por_pagar',
                                'valor_inicial', 'intereses_x_cobrar', 'saldo', 'provision', 'provison_NIIF', 'provision_no_NIIF',
                                'mto_garantia_1', 'mto_garantia_2', 'mto_garantia_3', 'mto_garantia_4', 'mto_garantia_5',
                                'mto_xv30d', 'mto_xv60d', 'mto_xv90d', 'mto_xv120d', 'mto_xv180d', 'mto_xv1a',
                                'Mto_xV1a5a', 'Mto_xV5a10a', 'Mto_xVm10a',
                                'mto_v30d', 'mto_v60d', 'mto_v90d', 'mto_v120d', 'mto_v180d', 'mto_v1a', 'mto_vm1a',
                                'mto_a_pagar', 'saldo_original', 'saldo_original_2', 'saldocapital', 'monto_asegurado',
                                'LIMITE', 'SALDO', 'interes_diferido', 'interes_dif', 'tasa_interes', 'Tasa'
                            ]
                            for col in money_candidates:
                                if col in df.columns:
                                    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                        except Exception as e:
                            self.logger.warning(f"CSV decimal normalization skipped for {source_path.name}: {e}")
                            # Fallback to plain read; decimals as-is
                            df = _pd.read_csv(source_path, dtype=str, keep_default_na=False)
                        dest_path.parent.mkdir(parents=True, exist_ok=True)
                        df.to_csv(dest_path, index=False, encoding='utf-8')
                        copied_files.append(str(dest_path))
                        self.logger.info(f"Normalized decimals and copied {source_path.name} → {dest_path.name}")
                    else:
                        dest_filename = f"{base_name}__run-{run_id}{source_path.suffix}"
                        dest_path = data_dir / dest_filename
                        # Copy with versioning
                        final_path, was_versioned = copy_with_versioning(source_path, dest_path, run_id)
                        final_path = final_path
                        copied_files.append(str(final_path))
                        self.logger.info(f"Copied {source_path.name} → {final_path.name}")
                
            except Exception as e:
                errors.append(f"{file_path}: Failed to copy - {str(e)}")
                continue
        
        if not copied_files:
            return ProcessingResult(
                success=False,
                message="No files were successfully copied",
                files_processed=0,
                total_records=0,
                errors=errors,
                warnings=warnings
            )
        
        self.logger.info(f"Successfully copied {len(copied_files)} files to data directory")
        
        return ProcessingResult(
            success=True,
            message=f"Copied {len(copied_files)} files",
            files_processed=len(copied_files),
            total_records=0,
            errors=errors,
            warnings=warnings,
            output_files=copied_files
        )
    
    def _analyze_files(self, file_paths: List[str], year: int, month: int, run_id: str) -> ProcessingResult:
        """Analyze files and generate metrics.
        
        Args:
            file_paths: List of file paths to analyze
            year: Processing year
            month: Processing month
            run_id: Unique run identifier
        
        Returns:
            ProcessingResult with analysis results
        """
        self.logger.info("Starting file analysis")
        
        metrics_dir = Path(self.config['metrics_dir'])
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        all_metrics = {}
        errors = []
        warnings = []
        total_records = 0
        
        self.logger.info("Analyzing individual files:")
        
        for file_path in file_paths:
            try:
                file_path_obj = Path(file_path)
                filename = file_path_obj.name
                
                # Calculate metrics
                file_metrics = self.metrics_calculator.calculate_file_metrics(file_path_obj)
                all_metrics[filename] = asdict(file_metrics)
                total_records += file_metrics.row_count
                
                self.logger.info(f"  📊 {filename}: {file_metrics.row_count:,} records")
                
            except Exception as e:
                errors.append(f"{file_path}: Analysis failed - {str(e)}")
                continue
        
        # Save metrics to file
        metrics_filename = f"exploration_metrics_{self.atom_name}_{year:04d}{month:02d}__run-{run_id}.json"
        metrics_path = metrics_dir / metrics_filename
        
        try:
            with open(metrics_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'atom': self.atom_name,
                    'period': f"{year:04d}-{month:02d}",
                    'run_id': run_id,
                    'timestamp': datetime.now().isoformat(),
                    'files_analyzed': len(file_paths),
                    'total_records': total_records,
                    'file_metrics': all_metrics
                }, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Metrics saved to {metrics_path}")
            
        except Exception as e:
            errors.append(f"Failed to save metrics: {str(e)}")
        
        self.logger.info(f"Analysis completed: {len(file_paths)} files, {total_records:,} total records")
        
        return ProcessingResult(
            success=True,
            message=f"Analyzed {len(file_paths)} files",
            files_processed=len(file_paths),
            total_records=total_records,
            errors=errors,
            warnings=warnings,
            metrics=all_metrics
        )
    
    def transform(self, year: int, month: int, run_id: str) -> ProcessingResult:
        """Execute transformation phase for AT12.
        
        Args:
            year: Processing year
            month: Processing month
            run_id: Unique run identifier
        
        Returns:
            ProcessingResult with transformation results
        """
        from .transformation import AT12TransformationEngine
        from ..core.paths import AT12Paths
        from ..core.transformation import TransformationContext

        self.logger.info(f"Starting AT12 transformation for {format_period(year, month)}")

        try:
            # Initialize transformation engine
            transformation_engine = AT12TransformationEngine(config=self.config)

            # Find input files from data directory (case-insensitive extension, include TXT)
            data_dir = Path(self.config['data_raw_dir'])
            candidates = []
            for patt in [
                f"*__run-{run_id}.csv", f"*__run-{run_id}.CSV",
                f"*__run-{run_id}.txt", f"*__run-{run_id}.TXT",
            ]:
                candidates.extend(list(data_dir.glob(patt)))
            # Deduplicate for case-insensitive filesystems (e.g., Windows)
            input_files = []
            seen_paths = set()
            for p in candidates:
                try:
                    rp = p.resolve()
                except Exception:
                    rp = p
                if rp not in seen_paths:
                    seen_paths.add(rp)
                    input_files.append(p)

            if not input_files:
                return ProcessingResult(
                    success=False,
                    message=f"No input files found for run {run_id}",
                    files_processed=0,
                    total_records=0,
                    errors=[f"No files found with pattern *__run-{run_id}.csv in {data_dir}"],
                    warnings=[]
                )

            self.logger.info(f"Found {len(input_files)} input files for transformation")

            # Create transformation context
            config_obj = Config()
            # Update config object with current config values
            for key, value in self.config.items():
                if hasattr(config_obj, key):
                    setattr(config_obj, key, value)
            
            paths = AT12Paths.from_config(config_obj)
            context = TransformationContext(
                run_id=run_id,
                period=f"{year}{month:02d}01",  # Convert to YYYYMMDD format
                config=config_obj,
                paths=paths,
                source_files=input_files,
                logger=self.logger
            )

            # Load input files into DataFrames (CSV/XLSX/TXT with auto encoding + delimiter)
            import pandas as pd
            import re as _re
            source_data = {}
            for file_path in input_files:
                try:
                    # Read file using appropriate path per extension
                    suffix = file_path.suffix.lower()
                    if suffix == '.txt':
                        # Use CSV reader's encoding + delimiter detection for TXT (supports UTF-16 Unicode Text)
                        csv_reader = self.file_reader.csv_reader
                        file_encoding = csv_reader._get_file_encoding(file_path)
                        sep = csv_reader._resolve_csv_delimiter(file_path, file_encoding)
                        # If Sniffer chose plain space, prefer regex whitespace for robustness
                        if sep == ' ':
                            df = pd.read_csv(
                                file_path,
                                dtype=str,
                                header=0,
                                sep=r'\s+',
                                engine='python',
                                keep_default_na=False,
                                encoding=file_encoding
                            )
                        else:
                            df = pd.read_csv(
                                file_path,
                                dtype=str,
                                header=0,
                                sep=sep,
                                engine='python',
                                keep_default_na=False,
                                encoding=file_encoding
                            )
                    else:
                        # Strict pre-validation for CSV/XLSX to prevent silent data loss
                        try:
                            vres = self.file_reader.validate_file(file_path)
                        except Exception as ve:
                            raise RuntimeError(f"Validation error for {file_path.name}: {ve}")
                        if not vres.is_valid:
                            # Export validation errors for operator review and abort
                            try:
                                from ..core.paths import AT12Paths
                                from ..core.config import Config as _Cfg
                                cfg = _Cfg()
                                for k, v in self.config.items():
                                    if hasattr(cfg, k):
                                        setattr(cfg, k, v)
                                paths = AT12Paths.from_config(cfg)
                                paths.ensure_directories()
                                import pandas as _pd
                                rep_rows = []
                                for msg in (vres.errors or []):
                                    rep_rows.append({'file': file_path.name, 'severity': 'ERROR', 'message': msg})
                                for msg in (vres.warnings or []):
                                    rep_rows.append({'file': file_path.name, 'severity': 'WARNING', 'message': msg})
                                rep_df = _pd.DataFrame(rep_rows or [{'file': file_path.name, 'severity': 'ERROR', 'message': 'Unknown validation failure'}])
                                rep_path = paths.incidencias_dir / f"CSV_FORMAT_ERRORS_{file_path.stem.split('__run-')[0]}_{context.period}.csv"
                                rep_df.to_csv(rep_path, index=False, encoding='utf-8', sep=self.csv_writer.delimiter, quoting=1)
                                self.logger.error(f"CSV_FORMAT_ERRORS -> {rep_path.name} ({len(rep_df)} messages)")
                            finally:
                                raise RuntimeError(f"Strict CSV/XLSX validation failed for {file_path.name}; see CSV_FORMAT_ERRORS report")
                        # CSV/XLSX path: use universal reader (auto delimiter + encoding)
                        df = self.file_reader.read_file(file_path)
                except Exception:
                    # Fallback read paths
                    if file_path.suffix.lower() == '.txt':
                        try:
                            csv_reader = self.file_reader.csv_reader
                            file_encoding = csv_reader._get_file_encoding(file_path)
                            sep = csv_reader._resolve_csv_delimiter(file_path, file_encoding)
                            if sep == ' ':
                                df = pd.read_csv(
                                    file_path,
                                    dtype=str,
                                    header=0,
                                    sep=r'\s+',
                                    engine='python',
                                    keep_default_na=False,
                                    encoding=file_encoding
                                )
                            else:
                                df = pd.read_csv(
                                    file_path,
                                    dtype=str,
                                    header=0,
                                    sep=sep,
                                    engine='python',
                                    keep_default_na=False,
                                    encoding=file_encoding
                                )
                        except Exception:
                            # Last resort: try utf-16 with whitespace
                            df = pd.read_csv(
                                file_path,
                                dtype=str,
                                header=0,
                                sep=r'\s+',
                                engine='python',
                                keep_default_na=False,
                                encoding='utf-16'
                            )
                    else:
                        # Retry via universal reader again
                        df = self.file_reader.read_file(file_path)
                # Derive subtype from filename stem, e.g. BASE_AT12_YYYYMMDD__run-XXXX -> BASE_AT12
                stem = file_path.stem
                m = _re.match(r"^(.+)_\d{8}__run-\d+$", stem)
                subtype = m.group(1) if m else stem
                # Apply internal uniformity: for TXT inputs (no headers), set columns from schema; for CSV, map headers for known subtypes
                try:
                    if file_path.suffix.lower() == '.txt':
                        # Get expected schema headers for subtype
                        expected = []
                        try:
                            if isinstance(self.schema_headers, dict) and subtype in self.schema_headers:
                                expected = list(self.schema_headers[subtype].keys())
                        except Exception:
                            expected = []
                        if expected:
                            # Trim extra columns, pad missing
                            cols_read = df.shape[1]
                            need = len(expected)
                            if cols_read >= need:
                                df = df.iloc[:, :need]
                                df.columns = expected
                            else:
                                # Assign available headers and add missing as empty
                                df.columns = expected[:cols_read]
                                for extra in expected[cols_read:]:
                                    df[extra] = ''
                        else:
                            # No schema available: leave as-is
                            pass
                    else:
                        # Standardize headers for CSV/XLSX using schema when available
                        from ..core.header_mapping import HeaderMapper as _HM
                        expected = []
                        if isinstance(self.schema_headers, dict) and subtype in self.schema_headers:
                            expected = list(self.schema_headers[subtype].keys())
                        if expected:
                            df = _HM.standardize_dataframe_to_schema(df, subtype, expected)
                        else:
                            # Fallback to subtype-specific mapping where defined
                            if subtype in ("TDC_AT12", "AT02_CUENTAS"):
                                mapped_cols = _HM.map_headers(list(df.columns), subtype)
                                if mapped_cols and len(mapped_cols) == len(df.columns):
                                    df.columns = mapped_cols
                except Exception:
                    pass
                source_data[subtype] = df

            # Execute AT12 transformation using engine-specific API
            result = transformation_engine.transform(context, source_data)

            # Convert TransformationResult to ProcessingResult
            if result.success:
                self.logger.info(f"Transformation completed successfully")
                self.logger.info(f"Files processed: {result.total_files_processed}")
                
                if result.processed_files:
                    self.logger.info("Output files generated:")
                    for output_file in result.processed_files:
                        self.logger.info(f"  📄 {Path(output_file).name}")
                        
                return ProcessingResult(
                    success=True,
                    message=f"Transformation completed successfully. Processed {result.total_files_processed} files.",
                    files_processed=result.total_files_processed,
                    total_records=result.metrics.get('total_records', 0),
                    errors=result.errors,
                    warnings=result.warnings,
                    output_files=[str(f) for f in result.processed_files]
                )
            else:
                self.logger.error(f"Transformation failed")
                if result.errors:
                    for error in result.errors:
                        self.logger.error(f"  ❌ {error}")
                        
                return ProcessingResult(
                    success=False,
                    message=f"Transformation failed. Errors: {'; '.join(result.errors)}",
                    files_processed=result.total_files_processed,
                    total_records=result.metrics.get('total_records', 0),
                    errors=result.errors,
                    warnings=result.warnings
                )
            
            return result
            
        except Exception as e:
            error_msg = f"Transformation failed with exception: {str(e)}"
            self.logger.error(error_msg)
            return ProcessingResult(
                success=False,
                message=error_msg,
                files_processed=0,
                total_records=0,
                errors=[error_msg],
                warnings=[]
            )
