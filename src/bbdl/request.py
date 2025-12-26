import contextlib
import gzip
import io
import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path

from bbdl.exceptions import BbdlParseError, BbdlTimeoutError
from bbdl.options import BbdlOptions
from bbdl.parser import Field
from libb import attrdict, unique

__all__ = ['Request']

logger = logging.getLogger(__name__)

TERMINAL_HEADER = """\
USERNUMBER={usernumber}
SN={sn}
WS={ws}
"""

TERMINAL_HEADER_BBA = """\
USERNUMBER={usernumber}
"""

REQUEST_HEADER = """\
START-OF-FILE
FIRMNAME={username}
PROGRAMFLAG={programflag}
DELIMITER={delimiter}
ADJUSTED=yes
DATEFORMAT={dateformat}
"""

BVAL_REQUEST_HEADER = """\
START-OF-FILE
FIRMNAME={username}
DERIVED=yes
DELIMITER={delimiter}
PRICING_SOURCE=BVAL:NY4PM
SECMASTER=yes
PROGRAMNAME=getdata
"""

REQUEST_TRAILER="""\
END-OF-FILE
"""

RC_OK = '0'
COMPRESS_FLAG = 'COMPRESS=yes'
HISTORY_PROGRAM = 'PROGRAMNAME=gethistory'
STATUS_FIELDS = ('IDENTIFIER', 'RETCODE', 'NFIELDS')
YELLOW_KEYS = ('Comdty', 'Equity', 'Muni', 'Pfd', 'M-Mkt',
               'Govt', 'Corp', 'Index', 'Curncy', 'Mtge')

ERROR_MESSAGE = {
    '-14': 'Field is not recognized or supported by the gethistory program.',
    '-13': 'Field, security, and date range combination is not applicable.',
    '-12': 'Field is not available.',
    '-10': 'Start date > End date.',
    '9':   'Asset class not supported for BVAL Tier1 pricing.',
    '10':  'Bloomberg cannot find the security as specified.',
    '11':  'Restricted Security.',
    '123': 'User not authorized for private loan (PRPL).',
    '605': 'Invalid macro value.',
    '988': 'System Error on security level.',
    '989': 'Unrecognized pricing source.',
    '990': 'System Error. Contact Product Support and Technical Assistance.',
    '991': 'Invalid override value (e.g., bad date or number) or Maximum number of overrides (20) exceeded.',
    '992': 'Unknown override field.',
    '993': 'Maximum number of overrides exceeded',
    '994': 'Permission denied.',
    '995': 'Maximum number of fields exceeded.',
    '996': 'Maximum number of data points exceeded (some data for this security is missing).',
    '997': 'General override error (e.g., formatting error).',
    '998': 'Security identifier type (e.g., CUSIP) is not recognized.',
    '999': 'Unloadable security',
}


@dataclass
class Result:
    """Dataclass representing parsed response file

    Initialized as empty dataclass, then fields updated.

    """
    data: list[dict] = field(init=False)
    errors: list[dict] = field(init=False)
    columns: list[(str, type)] = field(init=False)

    def __post_init__(self):
        self.data = []
        self.errors = []
        self._columns = []

    @property
    def columns(self):
        self._columns = unique(self._columns)
        return self._columns

    @columns.setter
    def columns(self, value):
        self._columns = value or []

    def extend(self, other):
        if other.data:
            self.data.extend(other.data)
        if other.errors:
            self.errors.extend(other.errors)
        if other.columns:
            self.columns.extend(other.columns)

    def unwrap_single_element_lists(self):
        """Unwrap single-element lists to scalar values and sanitize NaN/Inf.

        Historical parsing wraps values in lists for time series. When combining
        historical and non-historical files, this normalizes to scalar values.
        Also converts NaN/Inf to None for JSON compatibility.

        For multi-element lists in scalar fields (like country codes), takes
        the first non-null value.
        """
        from bbdl.parser import Field, _is_null

        for row in self.data:
            for key in row:
                val = row[key]
                if isinstance(val, list):
                    # Check if this field should be a scalar type
                    try:
                        ftype = Field.to_type(key)
                    except (ValueError, KeyError):
                        ftype = object
                    # For scalar types (not list), unwrap
                    if ftype is not list:
                        if len(val) == 1:
                            val = val[0]
                        elif len(val) > 1:
                            # Take first non-null value for scalar fields
                            for v in val:
                                if not _is_null(v):
                                    val = v
                                    break
                            else:
                                val = None
                        else:
                            val = None
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    val = None
                row[key] = val

    def to_dataframe(self):
        """Convert result data to a pandas DataFrame with NaN replaced by None.

        Pandas converts None to nan for numeric columns during DataFrame creation.
        This method creates the DataFrame and then replaces nan with None to ensure
        database compatibility.
        """
        import numpy as np
        import pandas as pd

        self.unwrap_single_element_lists()
        df = pd.DataFrame.from_records(self.data, columns=dict(self.columns))
        # Replace pandas NaN with None for database compatibility
        df = df.replace({np.nan: None})
        return df


class Request:
    """Generic utility for processing SFTP Request.
    """

    @staticmethod
    def build(identifiers: list, fields: list, reqfile: Path, options: BbdlOptions) -> Path:
        """Build request.

        See tests for details.

        `identifiers`: list of formatted tickers (IBM US Equity, 88160RAG6 Corp)
                       or list of ticker types ([IBM US, Equity], [88160RAG6, CUSIP])

        """
        headers = [] if not options.headers else options.headers[:]

        if options.begdate or options.enddate:
            if not options.enddate:
                options.enddate = options.begdate
            headers.extend([
                    HISTORY_PROGRAM,
                    'HIST_FORMAT=horizontal',
                    f'DATERANGE={options.begdate:%Y%m%d}|{options.enddate:%Y%m%d}',
                    ])
        else:
            headers.extend([
                    'SECMASTER=yes',
                    'CLOSINGVALUES=yes',
                    'DERIVED=yes',
                    ])

        with Path(reqfile).open('w') as f:
            # headers
            if options.bval:
                f.write(BVAL_REQUEST_HEADER.format(**options.__dict__))
            else:
                f.write(REQUEST_HEADER.format(**options.__dict__))
            if headers:
                f.write('\n'.join(headers) + '\n')
            if options.compressed and COMPRESS_FLAG not in headers:
                f.write(COMPRESS_FLAG + '\n')
            if options.usernumber and options.is_bba:
                f.write(TERMINAL_HEADER_BBA.format(**options.__dict__))
            elif options.usernumber:
                f.write(TERMINAL_HEADER.format(**options.__dict__))
            f.write('\n')
            # fields
            f.write('START-OF-FIELDS\n')
            f.writelines(fld + '\n' for fld in fields)
            f.write('END-OF-FIELDS\n')
            f.write('\n')
            # identifiers
            f.write('START-OF-DATA\n')
            for iden in identifiers:
                # bb tickers don't need a type, just the value
                if not isinstance(iden, tuple | list) or len(iden) == 1 or iden[-1] is None:
                    iden = iden[0] if isinstance(iden, tuple | list) else iden
                    origkey = iden.split(' ')[-1]
                    capkey = origkey.capitalize()
                    # yellow keys must be properly cased
                    if capkey in YELLOW_KEYS:
                        iden = iden.replace(origkey, capkey)
                    f.write(f'{iden}\n')
                # other identifiers need a value and a type
                elif len(iden) == 2:
                    f.write('{}|{}\n'.format(*iden))
                # overrides need a list of field/value pairs
                elif len(iden) > 3 and len(iden) % 2 == 0:
                    f.write('{}|{}|'.format(*iden[:2]))
                    f.write(f'{int(len(iden) / 2 - 1)}|')
                    f.write('|'.join([str(x) for x in iden[2:]]) + '\n')
                else:
                    raise ValueError(f'Unexpected idtype format: {iden}')
            f.write('END-OF-DATA\n')
            f.write('\n')
            # trailer
            f.write(REQUEST_TRAILER)

        with Path(reqfile).open('r') as f:
            logger.debug('Wrote request file:\n' + f.read())

        return reqfile

    @staticmethod
    def send(ftpcn, reqfile: Path, respfile: Path, options: BbdlOptions) -> None:
        reqname = reqfile.name
        respname = respfile.name
        # Bloomberg always compresses gethistory responses regardless of COMPRESS setting
        is_history = options.begdate or options.enddate
        if options.compressed or is_history:
            respfile = respfile.parent / (respfile.name + '.gz')
            respname += '.gz'
        # send request
        with contextlib.suppress(Exception):
            ftpcn.delete(respname)
        ftpcn.putascii(reqfile, reqname)
        # wait for response
        for i in range(options.wait_time*6):
            logger.debug('Waiting for output file...')
            time.sleep(10)
            files = ftpcn.files() or []
            found = [f for f in files if f.endswith(respname)]
            if found:
                logger.debug('Retrieving output file...')
                ftpcn.getbinary(respname, respfile)
                if options.compressed or is_history:
                    _unzip(respfile)
                break
        else:
            raise BbdlTimeoutError(f'Timeout waiting for reply file: {respname}')

    @staticmethod
    def parse(respfile: Path, options: BbdlOptions | None = None) -> Result:
        """Parses respfile

        respfile: Saved from `send` step
        options: BbdlOptions for parsing configuration

        Returns
        data := List[Dict]
        errors := List[Dict]
        columns := List[(str, type)]

        """
        respfile = Path(respfile)
        open_fn = gzip.open if '.gz' in respfile.name else open
        use_custom_mappings = options.use_custom_mappings if options else True
        with open_fn(respfile, 'rt') as f:
            return _parse(f, use_custom_mappings=use_custom_mappings)


def _unzip(zipfile: Path):
    """Unzip the file and leave it in place of the .gz version
    """
    unzipfile = zipfile.parent / zipfile.name[:-3]  # assume .gz
    with gzip.open(zipfile, 'rt') as gz, Path(unzipfile).open('w') as f:
        f.write(gz.read())
    zipfile.unlink(missing_ok=True)


def _parse(f: io.TextIOBase, use_custom_mappings: bool = True):
    """Parses opened respfile.
    """
    res = Result()

    # parse out the field names
    is_history = False
    infields = False
    fields = []
    while True:
        line = f.readline().strip()
        if line == HISTORY_PROGRAM:
            is_history = True
        if line == 'START-OF-FIELDS':
            if infields:
                raise BbdlParseError('Unexpected START-OF-FIELDS: already parsing fields')
            infields = True
            continue
        if line == 'END-OF-FIELDS':
            if not infields:
                raise BbdlParseError('Unexpected END-OF-FIELDS: not parsing fields')
            break
        if not infields:
            continue
        fields.append(line)

    indata = False
    while True:
        line = f.readline().strip()
        if line == 'START-OF-DATA':
            if indata:
                raise BbdlParseError('Unexpected START-OF-DATA: already parsing data')
            indata = True
            continue
        if line == 'END-OF-DATA':
            if not indata:
                raise BbdlParseError('Unexpected END-OF-DATA: not parsing data')
            break
        if not indata:
            continue

        status_fields = list(STATUS_FIELDS)[:]
        if is_history:
            status_fields.append('DATE')
        # ignore the last field since the line ends with a |
        flds = line.split('|')[:-1]
        if flds[1] == RC_OK:
            row = attrdict(zip(status_fields + fields, flds))
            for fld, val in row.items():
                try:
                    row[fld] = Field.to_python(fld, val, use_custom_mappings=use_custom_mappings)
                except Exception as exc:
                    logger.warning(f'Error converting fld={fld}, val={val}: {str(exc)}')
                    row[fld] = None
                try:
                    res.columns.append((fld, Field.to_type(fld)))
                except Exception:
                    res.columns.append((fld, object))
            res.data.append(row)
        else:
            msg = ERROR_MESSAGE.get(flds[1])
            if msg:
                logger.warning(f'Bloomberg Error {flds[1]} ({msg}): {flds[0]}')
            row = attrdict(zip(status_fields, flds[:len(status_fields)]))
            row.RETMSG = msg
            res.errors.append(row)

    # Convert historical data to time series for each field.
    # Right now it's just a bunch of rows, one for each ticker/date
    # combination. So we convert it to one row per identifier and each
    # field is now a list of values, one for each date. The additional
    # DATE field will have the list of actual observation dates for the data.
    if is_history:
        tickers = []
        datamap = {}
        for row in res.data:
            if row.IDENTIFIER not in tickers:
                tickers.append(row.IDENTIFIER)
                datamap[row.IDENTIFIER] = attrdict()
                for key in row:
                    if key in STATUS_FIELDS:
                        datamap[row.IDENTIFIER][key] = row[key]
                    else:
                        datamap[row.IDENTIFIER][key] = [row[key]]
            else:
                for key in row:
                    if key not in STATUS_FIELDS:
                        datamap[row.IDENTIFIER][key].append(row[key])
        # keep them in the same identifier order
        res.data = [datamap[t] for t in tickers]

    return res
