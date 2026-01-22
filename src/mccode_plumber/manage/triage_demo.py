#!/usr/bin/env python3
"""Small demo utility to print all styles known to the Triage class.

Run as module:
    python -m mccode_plumber.manage.triage_demo

This prints a sample line for each style key defined in Triage.styles.
"""
from __future__ import annotations

from colorama import init

from mccode_plumber.manage.manager import Triage


def main() -> None:
    init(autoreset=True)
    triage = Triage()

    print("Triage styles (sample lines):")
    # Print each known style with a short sample message
    for level in sorted(triage.styles.keys()):
        sample = f"[{level.upper()}] This is a sample {level} message."
        # Use internal helper to apply style consistently with Manager output
        styled = triage._style_line(level, sample)
        print(styled)


if __name__ == "__main__":
    main()
