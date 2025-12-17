import csv
import sys
from pathlib import Path

import pandas as pd

from bbdl.assets import get_assets_path
from bbdl.exceptions import BbdlValidationError
from libb import expandabspath

HEADERS = [
    'Field Mnemonic',
    'Data License Category',
    'Comdty',
    'Equity',
    'Muni',
    'Pfd',
    'M-Mkt',
    'Govt',
    'Corp',
    'Index',
    'Curncy',
    'Mtge',
    'Field Type',
    ]
HEADERS_LOOKUP = set(HEADERS)


def update_fields_asset(filepath):
    """Parses Bloomberg fields.csv into our stored and simplified version
    """
    fields = Path(expandabspath(filepath))
    if fields.name != 'fields.csv':
        raise BbdlValidationError(f"Expected 'fields.csv', got '{fields.name}'")
    if not fields.exists():
        raise BbdlValidationError(f'fields.csv file does not exist at {fields}')

    output = get_assets_path('fields.csv')
    output.unlink(missing_ok=True)

    df = pd.read_csv(fields)

    with fields.open('r') as fr, output.open('w') as fw:
        fw.write(','.join(HEADERS))
        fw.write('\n')
        reader = csv.reader(fr)
        header = next(reader)
        for row in reader:
            this_row = dict(zip(header, [c.strip() for c in row]))
            that_row = [v for k,v in this_row.items() if k in HEADERS_LOOKUP]
            fw.write(','.join(that_row))
            fw.write('\n')


if __name__ == '__main__':
    args = sys.argv[1:]
    update_fields_asset(args[0])
