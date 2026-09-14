BBDL
====

A Python API for interfacing with the Bloomberg Data License via SFTP.

See https://developer.blpprofessional.com/portal/products/dl?chapterId=4564

Uses libb-util (pypi)

Install
=======

```bash
# using pip
pip install -e git+https://github.com/bissli/bbdl.git#egg=bbdl[libb]

# using poetry
poetry add git+https://github.com/bissli/bbdl.git
```

Configuration
=============

Set environment variables or use a config object:

```python
import os

options = {
    'hostname': 'sftp.bloomberg.com',
    'username': os.getenv('BBG_USERNAME'),
    'password': os.getenv('BBG_PASSWORD'),
    'usernumber': os.getenv('BBG_USERNUMBER'),
    'is_bba': True,
    'secure': True,
    'programflag': 'adhoc',
}
```

A dotted path into a config module works in place of the dict, following
the 12factor pattern. `SFTPClient` reads the nested attributes
`hostname`, `username`, `password`, `usernumber`, `sn`, `ws`, `port`,
`secure`, `remotedir` and `programflag` from the leaf it names:

```python
from bbdl import SFTPClient

sftp = SFTPClient('bbg.data.ftp', config_module)
```

`programflag` takes `adhoc` or `oneshot`. Under `oneshot` Bloomberg
locks the data categories pulled for four months, so a single $10k pull
bills at least $10k a month for four months. `oneshot` is cheaper than
`adhoc` only where the fields selected are exactly the ones wanted.

Terminal linking takes one of two shapes. A Bloomberg Anywhere terminal
sets `is_bba=True` and supplies `usernumber` alone; including `sn` and
`ws` can degrade the request. An open terminal supplies all three, where
`sn` is the prefix and `ws` the suffix of the S/N field that `IAM <GO>`
reports.

Basic Usage
===========

```python
from bbdl import SFTPClient

options = {...}  # see Configuration above

identifiers = ['IBM US Equity', 'AAPL US Equity']
fields = ['ID_BB_GLOBAL', 'PX_LAST', 'PX_BID', 'PX_ASK', 'VOLUME']

with SFTPClient(options) as sftp:
    result = sftp.request(identifiers, fields)

# Access data
for row in result.data:
    print(f"{row['IDENTIFIER']}: {row['PX_LAST']}")

# Check for errors
for error in result.errors:
    print(f"Error {error['RETCODE']}: {error['IDENTIFIER']}")

# Convert to DataFrame
df = result.to_dataframe()
```

Historical Data
===============

Request historical time series by providing date range:

```python
with SFTPClient(options) as sftp:
    result = sftp.request(
        ['IBM US Equity', 'AAPL US Equity'],
        ['PX_LAST', 'PX_VOLUME'],
        begdate='2024-01-01',
        enddate='2024-12-31',
    )

# Historical data is returned as lists (time series)
for row in result.data:
    print(f"{row['IDENTIFIER']}:")
    print(f"  Dates: {row['DATE']}")
    print(f"  Prices: {row['PX_LAST']}")
```

Field Overrides
===============

Override default field behavior using the `field_overrides` parameter. The same
API works for both current (getdata) and historical (gethistory) requests, but
the override mechanism differs internally.

### Current Data Overrides (getdata)

For current data requests, overrides apply to identifiers. Common overrides:

- `FUND_PER`: Fundamental period (Q=Quarterly, S=Semi-Annual, A=Annual)
- `EQY_FUND_RELATIVE_PERIOD`: Relative fiscal period (e.g., 2024CY, -1FY)
- `BEST_FPERIOD_OVERRIDE`: Best estimate period (e.g., 2025Y, 2025Q1)

```python
# Get quarterly EBITDA instead of annual (default)
field_overrides = {
    'EBITDA': ('FUND_PER', 'Q'),
    'NET_DEBT': ('FUND_PER', 'Q'),
}

with SFTPClient(options) as sftp:
    result = sftp.request(
        ['IBM US Equity', 'AAPL US Equity'],
        ['PX_LAST', 'EBITDA', 'NET_DEBT'],
        field_overrides=field_overrides,
    )

# Get calendar year 2024 fundamentals
field_overrides = {
    'EBITDA': ('EQY_FUND_RELATIVE_PERIOD', '2024CY'),
}

with SFTPClient(options) as sftp:
    result = sftp.request(identifiers, ['EBITDA'], field_overrides=field_overrides)

# Get 2025 consensus estimates
field_overrides = {
    'BEST_EBITDA': ('BEST_FPERIOD_OVERRIDE', '2025Y'),
    'BEST_EPS': ('BEST_FPERIOD_OVERRIDE', '2025Y'),
}

with SFTPClient(options) as sftp:
    result = sftp.request(identifiers, ['BEST_EBITDA', 'BEST_EPS'],
                          field_overrides=field_overrides)
```

### Historical Data Overrides (gethistory)

For historical requests, overrides are embedded in field names. Common overrides:

- `AE`: Actuals/Estimates (A=Actuals, E=Estimates)
- `L`: Label format

```python
# Get historical consensus estimates instead of actuals
field_overrides = {
    'SALES_REV_TURN': ('AE', 'E'),
    'IS_EPS': ('AE', 'E'),
    'NET_INCOME': ('AE', 'E'),
}

with SFTPClient(options) as sftp:
    result = sftp.request(
        ['IBM US Equity', 'AAPL US Equity'],
        ['SALES_REV_TURN', 'IS_EPS', 'NET_INCOME'],
        field_overrides=field_overrides,
        begdate='2024-01-01',
        enddate='2024-12-31',
    )

# Compare actuals vs estimates
with SFTPClient(options) as sftp:
    # Get actuals (no override)
    actuals = sftp.request(
        identifiers, ['IS_EPS'],
        begdate='2024-01-01', enddate='2024-12-31',
    )

    # Get estimates
    estimates = sftp.request(
        identifiers, ['IS_EPS'],
        field_overrides={'IS_EPS': ('AE', 'E')},
        begdate='2024-01-01', enddate='2024-12-31',
    )
```

### Mixed Overrides

A single request can mix fields with different overrides. For getdata,
fields are grouped by override automatically:

```python
field_overrides = {
    'EBITDA': ('FUND_PER', 'Q'),           # Quarterly
    'NET_DEBT': ('FUND_PER', 'Q'),         # Quarterly (same group)
    'BEST_EBITDA': ('BEST_FPERIOD_OVERRIDE', '2025Y'),  # Estimates
}

with SFTPClient(options) as sftp:
    result = sftp.request(
        identifiers,
        ['PX_LAST', 'EBITDA', 'NET_DEBT', 'BEST_EBITDA'],
        field_overrides=field_overrides,
    )

# All fields merged into single result by identifier
for row in result.data:
    print(f"{row['IDENTIFIER']}:")
    print(f"  Price: {row['PX_LAST']}")
    print(f"  Quarterly EBITDA: {row['EBITDA']}")
    print(f"  2025 Est EBITDA: {row['BEST_EBITDA']}")
```

Result Object
=============

The `Result` object contains:

- `data`: List of dicts, one per identifier
- `errors`: List of dicts for failed identifiers
- `columns`: List of (field_name, type) tuples

```python
result = sftp.request(identifiers, fields)

# Iterate data
for row in result.data:
    identifier = row['IDENTIFIER']
    price = row['PX_LAST']

# Check errors
for error in result.errors:
    code = error['RETCODE']
    msg = error.get('RETMSG', 'Unknown error')
    print(f"{error['IDENTIFIER']}: {code} - {msg}")

# Convert to pandas DataFrame
df = result.to_dataframe()
```

Bulk Fields
===========

Some Bloomberg fields return multi-row, multi-column payloads: call and
put schedules, conversion reset schedules, underwriter lists. By default
these parse to a list of dicts with named columns, taken from
`BULK_FIELD_KEYS` in `mappings.py`:

```python
result = sftp.request(['88579YAW4 Corp'], ['CALL_SCHEDULE'])

for row in result.data:
    for call in row['CALL_SCHEDULE']:
        print(call['Call Date'], call['Call Price'])
```

Setting `use_custom_mappings=False` on the options returns the raw
`list[tuple]` form instead, with no column names.

Error Codes
===========

A failed identifier lands in `result.errors` rather than `result.data`,
carrying its `RETCODE` and a `RETMSG` looked up from the `ERROR_MESSAGE`
table in `request.py`. The common codes:

| code | meaning |
| --- | --- |
| 10 | Bloomberg cannot find the security as specified |
| 11 | restricted security |
| 994 | permission denied |
| 995 | maximum number of fields exceeded |
| 996 | maximum number of data points exceeded |
| 999 | unloadable security |

Negative codes come from the `gethistory` program: `-10` is a start date
later than the end date, `-12` an unavailable field, `-14` a field
`gethistory` does not support.

BVAL Pricing
============

Request BVAL evaluated prices:

```python
with SFTPClient(options) as sftp:
    result = sftp.request(
        ['88579YAW4 Corp', '912828ZT7 Govt'],
        ['PX_LAST', 'PX_BID', 'PX_ASK'],
        bval=True,
    )
```

Retrieve Existing Files
=======================

Retrieve previously submitted request files by date:

```python
from opendate import Date

with SFTPClient(options) as sftp:
    result = sftp.request(try_retrieve_existing_date=Date(2025, 1, 3))
```

Internals
=========

A request runs in four steps:

1. `SFTPClient` opens the SFTP connection as a context manager and
   orchestrates the rest.
2. `Request.build()` writes the request file: headers, the field block,
   then the identifier block. Fields are chunked at 500 per file, and a
   `field_overrides` request is split into one file per override group,
   named `fprp00.req`, `fprp01.req` and so on.
3. `Request.send()` uploads the request, then polls for the response
   file every 10 seconds for up to `wait_time` minutes. A `gethistory`
   response is always gzipped, whatever the `compressed` option says.
4. `Request.parse()` reads the pipe-delimited response into a `Result`.

Two modules carry the conversion logic. `Field` in `parser.py` maps a
Bloomberg value to a Python type using the field metadata embedded in
`assets.py`. `Ticker` in the same module formats tickers and validates
yellow keys.

A non-historical response yields one scalar per field. A historical
response wraps each field's values in a list, one entry per observation
date, aggregated per identifier, with `DATE` holding the matching
observation dates. `Result.unwrap_single_element_lists()` flattens a
single-date historical result back to scalars and converts NaN and Inf
to `None`; `Result.to_dataframe()` calls it before building the frame.

Development
===========

```bash
# install with test dependencies
poetry install -E test

# run the suite
poetry run pytest

# a single file, class, or test
poetry run pytest tests/test_parser.py -v
poetry run pytest tests/test_parser.py::TestFieldToNumber -v

# with log output
poetry run pytest tests/ --log-cli-level=DEBUG

# line coverage
poetry run python -m coverage run --source=src/bbdl -m pytest tests/
poetry run python -m coverage report -m --omit='*/assets.py'

# mutation testing; clear the cache first, or stats go stale
rm -rf mutants mutmut-stats.json
poetry run mutmut run --max-children 8
poetry run mutmut results
poetry run mutmut show <mutant-id>
```

Test paths and flags come from `[tool.pytest.ini_options]` in
`pyproject.toml`, so a bare `poetry run pytest` runs the whole suite.
Warnings are errors there, which keeps a new deprecation from scrolling
past unnoticed.

`tests/fixtures/samples/` holds captured request and response pairs. The
ten `request.req` files are compared byte for byte against
`Request.build` output, so a change to any header line shows up as a
failing golden rather than a silent difference.

Behavior the suite deliberately does not cover is recorded in
`todo/todo_test_coverage_gaps.md`, with the unblock for each.
