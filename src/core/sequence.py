#!/usr/bin/env python3
"""
Simple persistent sequence/assignment registry.

Used to assign idempotent sequential identifiers to natural keys and
persist the mapping across runs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


@dataclass
class SequenceRegistry:
    """Persistent registry for assigning sequential IDs to natural keys.

    Data is stored in a single JSON file with the following structure:
    {
      "last_number": 852265,
      "assignments": {
        "<natural_key>": 852266,
        ...
      }
    }
    """

    state_path: Path
    start_number: int = 1
    _last_number: int = field(init=False, default=0)
    _assignments: Dict[str, int] = field(init=False, default_factory=dict)

    def __post_init__(self):
        self._load()

    def _load(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if self.state_path.exists():
            try:
                data = json.loads(self.state_path.read_text(encoding="utf-8"))
                self._last_number = int(data.get("last_number", 0))
                self._assignments = {str(k): int(v) for k, v in data.get("assignments", {}).items()}
            except Exception:
                # Corrupt file: reset
                self._last_number = 0
                self._assignments = {}
        else:
            self._last_number = 0
            self._assignments = {}

        if self._last_number <= 0 and self.start_number > 0:
            self._last_number = self.start_number - 1

    def _save(self) -> None:
        data = {
            "last_number": self._last_number,
            "assignments": self._assignments,
        }
        self.state_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def get_or_assign(self, natural_key: str) -> int:
        """Return existing assignment for key or assign next sequential number.

        Returns the integer number; formatting (e.g., padding) is left to caller.
        """
        key = str(natural_key)
        if key in self._assignments:
            return self._assignments[key]
        self._last_number += 1
        self._assignments[key] = self._last_number
        self._save()
        return self._last_number

    def peek(self, natural_key: str) -> Optional[int]:
        return self._assignments.get(str(natural_key))

    def last_number(self) -> int:
        return self._last_number

