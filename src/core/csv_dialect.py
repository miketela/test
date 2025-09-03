from pathlib import Path
from typing import Optional, Dict


def detect_dialect_with_frictionless(file_path: Path, sample_rows: int = 200) -> Optional[Dict[str, str]]:
    """Attempt to detect CSV dialect using Frictionless if available.

    Returns a dict like {"delimiter": ";", "quotechar": '"'} or None if unavailable.
    """
    try:
        import frictionless as fl  # type: ignore
    except Exception:
        return None
    try:
        detector = fl.Detector(sample_size=sample_rows)
        res = detector.detect(file_path)
        # dialect may be in res.dialect or as a list of dialects (resource detection)
        dialect = getattr(res, 'dialect', None)
        if dialect is None and hasattr(res, 'dialects'):
            dialects = getattr(res, 'dialects', None)
            dialect = dialects[0] if dialects else None
        if not dialect:
            return None
        delim = getattr(dialect, 'delimiter', None) or getattr(dialect, 'csv', {}).get('delimiter')
        quote = getattr(dialect, 'quote_char', None) or getattr(dialect, 'csv', {}).get('quotechar')
        out: Dict[str, str] = {}
        if delim:
            out['delimiter'] = delim
        if quote:
            out['quotechar'] = quote
        return out or None
    except Exception:
        return None


def detect_dialect_builtin(file_path: Path, candidates: Optional[str] = None, sample_lines: int = 10) -> Optional[Dict[str, str]]:
    import csv
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = []
            for _ in range(sample_lines):
                line = f.readline()
                if not line:
                    break
                if line.strip():
                    lines.append(line)
            text = ''.join(lines)
        if not text:
            return None
        sniff = csv.Sniffer().sniff(text, delimiters=candidates or ',;|\t')
        return {'delimiter': sniff.delimiter}
    except Exception:
        return None


def detect_dialect(file_path: Path) -> Dict[str, str]:
    """Detect dialect via Frictionless if available, otherwise builtin.
    Returns at least {'delimiter': ...} when detected; may include quotechar.
    """
    return (
        detect_dialect_with_frictionless(file_path) or
        detect_dialect_builtin(file_path) or
        {}
    )

