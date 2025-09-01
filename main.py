#!/usr/bin/env python3
"""
SBP Atoms Pipeline - Main Orchestrator
Handles exploration and transformation processes for regulatory atoms.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from src.core.config import Config
from src.core.log import get_logger, setup_logging, add_file_logging
from src.core.time_utils import resolve_period
from src.core.reports import create_exploration_report
import importlib.util as _importlib_util
from importlib import import_module as _import_module
from pathlib import Path as _Path

def _load_tui_main():
    """Try to load TUI main() via normal import, then via file path fallback."""
    try:
        return _import_module('scripts.tui').main  # type: ignore[attr-defined]
    except Exception:
        try:
            tui_path = _Path(__file__).resolve().parent / 'scripts' / 'tui.py'
            if tui_path.exists():
                spec = _importlib_util.spec_from_file_location('scripts.tui', str(tui_path))
                if spec and spec.loader:
                    mod = _importlib_util.module_from_spec(spec)
                    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
                    return getattr(mod, 'main', None)
        except Exception:
            return None
    return None
from src.AT12.processor import AT12Processor


def main():
    """Main entry point for the SBP Atoms pipeline."""
    parser = argparse.ArgumentParser(description="SBP Atoms Pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Explore command
    explore_parser = subparsers.add_parser("explore", help="Run exploration process")
    explore_parser.add_argument("--atoms", nargs="+", default=["AT12"], help="Atoms to process")
    explore_parser.add_argument("--year", type=int, help="Year to process")
    explore_parser.add_argument("--month", help="Month to process (number or name)")
    explore_parser.add_argument("--workers", type=int, help="Number of workers")
    explore_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    explore_parser.add_argument("--strict-period", type=bool, help="Enforce strict period validation")
    
    # Transform command
    transform_parser = subparsers.add_parser("transform", help="Run transformation process")
    transform_parser.add_argument("--atoms", nargs="+", default=["AT12"], help="Atoms to process")
    transform_parser.add_argument("--year", type=int, help="Year to process")
    transform_parser.add_argument("--month", help="Month to process (number or name)")
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate PDF reports")
    report_parser.add_argument("--metrics-file", required=True, help="Path to metrics JSON file")
    report_parser.add_argument("--output", help="Output PDF file path")
    report_parser.add_argument("--title", help="Custom report title")
    report_parser.add_argument("--raw-data-dir", help="Path to raw data directory for additional analysis")
    
    # TUI command
    subparsers.add_parser("tui", help="Interactive terminal UI for explore/transform/report")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Initialize configuration
    config = Config()
    config.update_from_args(args)
    
    # Setup console logging early
    verbose = getattr(args, 'verbose', False)
    setup_logging(level=config.log_level, verbose=verbose)
    logger = get_logger(__name__)
    
    try:
        if args.command == "tui":
            tui_main = _load_tui_main()
            if tui_main is None:
                print("TUI is unavailable (import failed)")
                return 1
            tui_main()
            return 0
        if args.command == "report":
            # Handle report generation
            metrics_file = Path(args.metrics_file)
            if not metrics_file.exists():
                logger.error(f"Metrics file not found: {metrics_file}")
                return 1
            
            # Determine output file path
            if args.output:
                output_file = Path(args.output)
            else:
                # Generate default output filename
                output_file = Path(config.reports_dir) / f"{metrics_file.stem}_report.pdf"
            
            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Generate report
            raw_data_dir = Path(args.raw_data_dir) if args.raw_data_dir else Path(config.data_raw_dir)
            
            # Add file logging for this run (try to extract run_id from metrics filename)
            import re as _re
            m = _re.search(r"__run-(\d{6})", metrics_file.stem)
            run_id_for_log = m.group(1) if m else datetime.now().strftime('%Y%m')
            log_file = Path(config.logs_dir) / "AT12" / "report" / f"report_{run_id_for_log}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
            add_file_logging(log_file, level=config.log_level)

            logger.info(f"Generating PDF report from {metrics_file}")
            success = create_exploration_report(
                metrics_file=metrics_file,
                output_file=output_file,
                title=args.title,
                raw_data_dir=raw_data_dir
            )
            
            if success:
                logger.info(f"Report generated successfully: {output_file}")
                return 0
            else:
                logger.error("Failed to generate report")
                return 1
        
        # Handle explore and transform commands
        # Resolve period
        period = resolve_period(args.year, args.month)
        logger.info(f"Processing period: {period}")
        
        # Process each atom
        exit_code = 0
        for atom in args.atoms:
            if atom == "AT12":
                processor = AT12Processor(config.to_dict())
                
                if args.command == "explore":
                    # Add run-based file logger
                    run_id = f"{period[0]}{period[1]:02d}"
                    log_file = Path(config.logs_dir) / atom / "explore" / f"{atom}_explore_{run_id}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
                    add_file_logging(log_file, level=config.log_level)
                    result = processor.explore(period[0], period[1], f"{period[0]}{period[1]:02d}")
                elif args.command == "transform":
                    run_id = f"{period[0]}{period[1]:02d}"
                    log_file = Path(config.logs_dir) / atom / "transform" / f"{atom}_transform_{run_id}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
                    add_file_logging(log_file, level=config.log_level)
                    result = processor.transform(period[0], period[1], f"{period[0]}{period[1]:02d}")
                
                # Update exit code based on result
                if not result.success or result.errors:
                    exit_code = 1
                elif result.warnings and exit_code == 0:
                    exit_code = 2
                    
                logger.info(f"Processing result: {result.message}")
            else:
                logger.error(f"Unknown atom: {atom}")
                exit_code = 1
        
        return exit_code
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
