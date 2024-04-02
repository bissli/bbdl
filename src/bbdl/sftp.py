import contextlib
import copy
import gzip
import logging
import os
import time
from dataclasses import dataclass, field

from bbdl.parser import Field
from libb import Date, attrdict, config, ftp
from more_itertools import unique_everseen
from typing_extensions import List

logger = logging.getLogger(__name__)

__all__ = ['SFTPClient']


@dataclass
class Options:

    bval: bool = False
    headers: List = field(default_factory=list)
    begdate: Date | None = None
    enddate: Date | None = None
    compressed: bool = False
    dateformat: str = 'yyyymmdd'
    programflag: str = 'oneshot'
    delimiter: str = '|'
    wait_time: int = 20
    sn: str | None = None
    username: str | None = None
    usernumber: str | None = None
    hostname: str | None = None
    password: str | None = None
    fields_path: str = 'fields.csv'
    remotedir: str = '/'
    secure: bool = True

    def __post_init__(self):
        self.begdate = Date(self.begdate) if self.begdate else None
        self.enddate = Date(self.enddate) if self.enddate else None

    @classmethod
    def from_config(cls, sitename: str, config=None):
        this = config
        for level in sitename.split('.'):
            this = getattr(this, level)
        return cls(**this['ftp'])


class SFTPClient:
    """Bloomberg SFTP Client

    Request:
        `begdate`:
        `enddate`:
        `compressed`:
        `bval`:
        `wait_time`:
        `sn`:
        `usernumber`:

    """
    def __init__(self, account: str, config=config, **kwargs):
        self.account = account
        self.config = config
        self.options = Options.from_config(account, config=config)

    def __enter__(self):
        logger.debug('Entering SecureFTP client')
        self.cn = ftp.connect(self.account, config=self.config)
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        logger.debug('Exiting SecureFTP client')
        if exc_ty:
            logger.exception(exc_val)
        with contextlib.suppress(Exception):
            self.cn.close()

    def request(self, identifiers, fields, categories, bval=False,
                headers=None, begdate=None, enddate=None):
        """Request Bloomberg `fields` over `identifiers`.
        """
        opts = copy.deepcopy(self.options)
        opts.bval = bval
        opts.headers = headers
        opts.begdate = Date(begdate) if begdate else None
        opts.enddate = Date(enddate) if enddate else None

        fields = limit_fields_to_categories(fields, categories)

        # chunk 500 fields per request
        nparts = int((len(fields) - 1) / 500) + 1
        alldata, allerrors, allcolumns = [], [], []
        for part in range(nparts):
            logger.info(f'lookup part {part+1:02d} / {nparts:02d}')
            i = part*500
            reqfile = os.path.join(config.tmpdir.dir, f'fprp{part:02d}.req')
            respfile = os.path.join(config.tmpdir.dir, f'fprp{part:02d}.out')
            Request.build(identifiers, fields[i:i+500], reqfile, opts)
            Request.send(self.cn, reqfile, respfile, opts)
            data, errors, columns = Request.parse(respfile)
            alldata.extend(data)
            allerrors.extend(errors)
            allcolumns.extend(columns)
        return alldata, allerrors, list(unique_everseen(allcolumns))


class Request:
    """Generic utility for processing SFTP Request.
    """

    TERMINAL_HEADER = """\
USERNUMBER={usernumber}
SN={sn}
WS=1
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

    @staticmethod
    def build(identifiers, fields, reqfile, opts):
        """Build request.

        See tests for details.

        `identifiers`: list of formatted tickers (IBM US Equity, 88160RAG6 Corp)
                       or list of ticker types ([IBM US, Equity], [88160RAG6, CUSIP])

        """
        headers = [] if not opts.headers else opts.headers[:]

        if opts.begdate or opts.enddate:
            if not opts.enddate:
                opts.enddate = opts.begdate
            headers.extend([
                    Request.HISTORY_PROGRAM,
                    'HIST_FORMAT=horizontal',
                    f'DATERANGE={opts.begdate:"%Y%m%d"}|{opts.enddate:"%Y%m%d"}',
                    ])
        else:
            headers.extend([
                    'SECMASTER=yes',
                    'CLOSINGVALUES=yes',
                    'DERIVED=yes',
                    ])

        with open(reqfile, 'w') as f:
            # headers
            if opts.bval:
                f.write(Request.BVAL_REQUEST_HEADER.format(**opts.__dict__))
            else:
                f.write(Request.REQUEST_HEADER.format(**opts.__dict__))
            if headers:
                f.write('\n'.join(headers) + '\n')
            if opts.compressed and Request.COMPRESS_FLAG not in headers:
                f.write(Request.COMPRESS_FLAG + '\n')
            if opts.sn and opts.usernumber:
                f.write(Request.TERMINAL_HEADER.format(**opts.__dict__))
            f.write('\n')
            # fields
            f.write('START-OF-FIELDS\n')
            for fld in fields:
                f.write(fld + '\n')
            f.write('END-OF-FIELDS\n')
            f.write('\n')
            # identifiers
            f.write('START-OF-DATA\n')
            for iden in identifiers:
                # bb tickers don't need a type, just the value
                if not isinstance(iden, (tuple, list)) or len(iden) == 1 or iden[-1] is None:
                    iden = iden[0] if isinstance(iden, (tuple, list)) else iden
                    origkey = iden.split(' ')[-1]
                    capkey = origkey.capitalize()
                    # yellow keys must be properly cased
                    if capkey in Request.YELLOW_KEYS:
                        iden = iden.replace(origkey, capkey)
                    f.write('%s\n' % iden)
                # other identifiers need a value and a type
                elif len(iden) == 2:
                    f.write('%s|%s\n' % iden)
                # overrides need a list of field/value pairs
                elif len(iden) > 3 and len(iden) % 2 == 0:
                    f.write('%s|%s' % iden[:2])
                    f.write('%d' % (len(iden) / 2 - 1))
                    f.write('|'.join([str(x) for x in iden[2:]]) + '\n')
                else:
                    raise ValueError('Unexpected idtype format: %s' + str(iden))
            f.write('END-OF-DATA\n')
            f.write('\n')
            # trailer
            f.write(Request.REQUEST_TRAILER)

        with open(reqfile, 'r') as f:
            logger.debug('Wrote request file:\n' + f.read())

    @staticmethod
    def send(ftpcn, reqfile, respfile, opts):
        _, reqname = os.path.split(reqfile)
        _, respname = os.path.split(respfile)
        if opts.compressed:
            respfile = respfile + '.gz'
            respname = respname + '.gz'
        # send request
        with contextlib.suppress(Exception):
            ftpcn.delete(respname)
        ftpcn.putascii(reqfile, reqname)
        # wait for response
        for i in range(opts.wait_time*6):
            logger.debug('Waiting for output file...')
            time.sleep(10)
            files = ftpcn.files()
            found = [f for f in files if f.endswith(respname)]
            if found:
                logger.debug('Retrieving output file...')
                ftpcn.getbinary(respname, os.path.join(config.tmpdir.dir, respname))
                if opts.compressed:
                    unzip(respfile)
                break
        else:
            raise Exception(f'Timeout waiting for reply file: {respname}')

    @staticmethod
    def parse(path):
        f = open(path, 'r')

        # parse out the field names
        is_history = False
        infields = False
        fields = []
        while True:
            line = f.readline().strip()
            if line == Request.HISTORY_PROGRAM:
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
        data, errors, columns = [], [], {}
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

            status_fields = list(Request.STATUS_FIELDS)[:]
            if is_history:
                status_fields.append('DATE')
            # ignore the last field since the line ends with a |
            flds = line.split('|')[:-1]
            if flds[1] == Request.RC_OK:
                row = attrdict(zip(status_fields + fields, flds))
                for fld, val in row.items():
                    try:
                        row[fld] = Field.to_python(fld, val)
                    except:
                        logger.debug(f'Error converting fld={fld}, val={val}')
                        row[fld] = None
                    try:
                        columns[fld] = Field.to_type(fld)
                    except:
                        columns[fld] = object
                data.append(row)
            else:
                msg = Request.ERROR_MESSAGE.get(flds[1])
                if msg:
                    logger.warning('Bloomberg Error %s%s: %s' % (flds[1], f' ({msg})', flds[0]))
                row = attrdict(zip(status_fields, flds[:len(status_fields)]))
                row.RETMSG = msg
                errors.append(row)

        # Convert historical data to time series for each field.
        # Right now it's just a bunch of rows, one for each ticker/date
        # combination. So we convert it to one row per identifier and each
        # field is now a list of values, one for each date. The additional
        # DATE field will have the list of actual observation dates for the data.
        if is_history:
            tickers = []
            datamap = {}
            for row in data:
                if row.IDENTIFIER not in tickers:
                    tickers.append(row.IDENTIFIER)
                    datamap[row.IDENTIFIER] = attrdict()
                    for key in row:
                        if key in Request.STATUS_FIELDS:
                            datamap[row.IDENTIFIER][key] = row[key]
                        else:
                            datamap[row.IDENTIFIER][key] = [row[key]]
                else:
                    for key in row:
                        if key not in Request.STATUS_FIELDS:
                            datamap[row.IDENTIFIER][key].append(row[key])
            # keep them in the same identifier order
            data = [datamap[t] for t in tickers]

        return data, errors, list(columns.items())


def get_site(account, config):
    this = config
    for level in account.split('.'):
        this = getattr(this, level)
    return this.ftp


def unzip(zipfile):
    """Unzip the file and and leave it in place of the .gz version
    """
    unzipfile = zipfile[:-3]  # assume .gz
    with open(unzipfile, 'w') as f:
        f.write(gzip.open(zipfile).read())
    os.remove(zipfile)


def limit_fields_to_categories(reqfields, categories):
    """Filter fields to avoid expensive mistakes
    """
    allfields = Field.from_categories(categories)
    fields = sorted([f.upper() for f in reqfields if f.upper() in allfields])
    logger.info(f'Filtered {len(reqfields)} request fields to {len(fields)} match fields.')
    return fields


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
