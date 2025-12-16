import contextlib
import gzip
import io
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

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


class Request:
    """Generic utility for processing SFTP Request.
    """

    @staticmethod
    def build(identifiers: list, fields: list, reqfile: Path, options: BbdlOptions):
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
    def send(ftpcn, reqfile: Path, respfile: Path, options: BbdlOptions):
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
            files = ftpcn.files()
            found = [f for f in files if f.endswith(respname)]
            if found:
                logger.debug('Retrieving output file...')
                ftpcn.getbinary(respname, respfile)
                if options.compressed or is_history:
                    _unzip(respfile)
                break
        else:
            raise Exception(f'Timeout waiting for reply file: {respname}')

    @staticmethod
    def parse(respfile: Path) -> Result:
        """Parses respfile

        respfile: Saved from `send` step

        Returns
        data := List[Dict]
        errors := List[Dict]
        columns := List[(str, type)]

        """
        with Path(respfile).open('r') as f:
            return _parse(f)


def _unzip(zipfile: Path):
    """Unzip the file and leave it in place of the .gz version
    """
    unzipfile = zipfile.parent / zipfile.name[:-3]  # assume .gz
    with gzip.open(zipfile, 'rt') as gz, Path(unzipfile).open('w') as f:
        f.write(gz.read())
    zipfile.unlink(missing_ok=True)


def _parse(f: io.TextIOBase):
    """Parses opened respfile
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
            assert not infields
            infields = True
            continue
        if line == 'END-OF-FIELDS':
            assert infields
            break
        if not infields:
            continue
        fields.append(line)

    indata = False
    while True:
        line = f.readline().strip()
        if line == 'START-OF-DATA':
            assert not indata
            indata = True
            continue
        if line == 'END-OF-DATA':
            assert indata
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
                    row[fld] = Field.to_python(fld, val)
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
