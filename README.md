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

You can mix fields with different overrides in a single request. For getdata,
fields are automatically grouped by override:

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
from date import Date

with SFTPClient(options) as sftp:
    result = sftp.request(try_retrieve_existing_date=Date(2025, 1, 3))
```
