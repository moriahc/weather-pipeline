#!/usr/bin/env python
import csv
from typing import Dict, List, Any, TypedDict


def write_output_csv(output_filepath: str, rows: List[Any], fieldnames: List[str]) -> int:
    written_count = 0
    with open(output_filepath, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            written_count += 1
    return written_count
