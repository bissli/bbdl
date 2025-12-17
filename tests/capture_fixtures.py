"""Capture sample Bloomberg request/response data for test fixtures.

This is a ONE-TIME utility script for capturing real Bloomberg responses.
It requires a config module with Bloomberg credentials, which is NOT part of bbdl.

Run from a directory with proper config (e.g., tenor/bin):
    cd ~/Dropbox/code/work/sandbox/main/tenor/bin
    PYTHONPATH=~/Dropbox/code/play/bbdl/src:$PYTHONPATH python ~/Dropbox/code/play/bbdl/tests/capture_fixtures.py

The captured fixtures are checked into the repo under tests/fixtures/samples/
"""
import shutil
from pathlib import Path

from bbdl import SFTPClient
from date import Date

try:
    from tc import config  # External config with Bloomberg credentials
except ImportError:
    raise ImportError(
        'This script requires tc.config with Bloomberg credentials. '
        'Run from tenor/bin or provide your own config module.'
    )

FIXTURES_DIR = Path(__file__).parent / 'fixtures' / 'samples'

# Minimal set of tickers covering different asset types
TICKERS = [
    'AAPL US Equity',
    'IBM US Equity',
]

# Minimal set of fields - mix of types
FIELDS = [
    'ID_BB_GLOBAL',
    'ID_BB_UNIQUE',
    'PARSEKYABLE_DES',
    'SECURITY_DES',
    'PX_LAST',
    'PX_BID',
    'PX_ASK',
    'CNTRY_OF_DOMICILE',
    'COUNTRY_ISO',
    'CRNCY',
]


def capture_current_request():
    """Capture a current (non-historical) getdata request."""
    print('\n=== Capturing CURRENT request (getdata) ===')

    with SFTPClient('bbg.data.ftp', config) as sftp:
        result = sftp.request(TICKERS, FIELDS)

    # Copy the request and response files
    reqfile = sftp.options.tempdir / 'fprp00.req'
    respfile = sftp.options.tempdir / 'fprp00.out'

    outdir = FIXTURES_DIR / 'current'
    outdir.mkdir(parents=True, exist_ok=True)

    if reqfile.exists():
        shutil.copy(reqfile, outdir / 'request.req')
        print(f'Saved: {outdir}/request.req')
        print(f'--- Request content ---')
        print(reqfile.read_text())

    if respfile.exists():
        shutil.copy(respfile, outdir / 'response.out')
        print(f'Saved: {outdir}/response.out')
        print(f'--- Response content ---')
        print(respfile.read_text())

    print(f'\nResult: {len(result.data)} rows, {len(result.errors)} errors')
    if result.data:
        print(f'Columns: {[c[0] for c in result.columns]}')
    return result


def capture_historical_single_date():
    """Capture a historical request for a single date."""
    print('\n=== Capturing HISTORICAL request (single date) ===')

    # Use a recent business day
    target_date = Date.parse('P-1b')

    with SFTPClient('bbg.data.ftp', config) as sftp:
        result = sftp.request(TICKERS, FIELDS, begdate=target_date, enddate=target_date)

    # Copy the request and response files
    reqfile = sftp.options.tempdir / 'fprp00.req'
    respfile = sftp.options.tempdir / 'fprp00.out'

    outdir = FIXTURES_DIR / 'historical_single'
    outdir.mkdir(parents=True, exist_ok=True)

    if reqfile.exists():
        shutil.copy(reqfile, outdir / 'request.req')
        print(f'Saved: {outdir}/request.req')
        print(f'--- Request content ---')
        print(reqfile.read_text())

    if respfile.exists():
        shutil.copy(respfile, outdir / 'response.out')
        print(f'Saved: {outdir}/response.out')
        print(f'--- Response content ---')
        print(respfile.read_text())

    print(f'\nResult: {len(result.data)} rows, {len(result.errors)} errors')
    if result.data:
        print(f'Sample row keys: {list(result.data[0].keys())}')
        # Show if values are wrapped in lists (historical format)
        for key in ['PX_LAST', 'CNTRY_OF_DOMICILE']:
            if key in result.data[0]:
                val = result.data[0][key]
                print(f'  {key}: {val!r} (type: {type(val).__name__})')
    return result


def capture_historical_multi_date():
    """Capture a historical request for multiple dates."""
    print('\n=== Capturing HISTORICAL request (multi date) ===')

    # Use last 3 business days
    end_date = Date.parse('P-1b')
    beg_date = Date.parse('P-3b')

    with SFTPClient('bbg.data.ftp', config) as sftp:
        result = sftp.request(TICKERS, FIELDS, begdate=beg_date, enddate=end_date)

    # Copy the request and response files
    reqfile = sftp.options.tempdir / 'fprp00.req'
    respfile = sftp.options.tempdir / 'fprp00.out'

    outdir = FIXTURES_DIR / 'historical_multi'
    outdir.mkdir(parents=True, exist_ok=True)

    if reqfile.exists():
        shutil.copy(reqfile, outdir / 'request.req')
        print(f'Saved: {outdir}/request.req')
        print(f'--- Request content ---')
        print(reqfile.read_text())

    if respfile.exists():
        shutil.copy(respfile, outdir / 'response.out')
        print(f'Saved: {outdir}/response.out')
        print(f'--- Response content ---')
        print(respfile.read_text())

    print(f'\nResult: {len(result.data)} rows, {len(result.errors)} errors')
    if result.data:
        print(f'Sample row keys: {list(result.data[0].keys())}')
        # Show list lengths for historical data
        for key in ['PX_LAST', 'DATE']:
            if key in result.data[0]:
                val = result.data[0][key]
                if isinstance(val, list):
                    print(f'  {key}: list of {len(val)} values')
                else:
                    print(f'  {key}: {val!r}')
    return result


if __name__ == '__main__':
    print(f'Fixtures will be saved to: {FIXTURES_DIR}')

    try:
        capture_current_request()
    except Exception as e:
        print(f'ERROR capturing current request: {e}')

    try:
        capture_historical_single_date()
    except Exception as e:
        print(f'ERROR capturing historical single date: {e}')

    try:
        capture_historical_multi_date()
    except Exception as e:
        print(f'ERROR capturing historical multi date: {e}')

    print('\n=== Done ===')
    print(f'Check {FIXTURES_DIR} for captured files')
