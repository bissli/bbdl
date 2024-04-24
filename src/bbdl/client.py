import contextlib
import copy
import logging

from bbdl.options import Options
from bbdl.parser import Field
from bbdl.request import Request, Result

import ftp
from date import Date
from libb import load_options

logger = logging.getLogger(__name__)

__all__ = ['SFTPClient']


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
    @load_options(cls=Options)
    def __init__(self, options: str | dict | Options | None = None, /, config=None):
        self.config = config
        self.options = options

    def __enter__(self):
        logger.debug('Entering SecureFTP client')
        _options = ftp.FtpOptions(
            hostname=self.options.hostname,
            username=self.options.username,
            password=self.options.password,
            secure=self.options.secure,
            port=self.options.port)
        self.cn = ftp.connect(_options)
        return self

    def __exit__(self, exc_ty, exc_val, tb):
        logger.debug('Exiting SecureFTP client')
        if exc_ty:
            logger.exception(exc_val)
        with contextlib.suppress(Exception):
            self.cn.close()

    def request(self, identifiers, fields, categories, bval=False,
                headers=None, begdate=None, enddate=None,
                allowopen=True) -> Result:
        """Request Bloomberg `fields` over `identifiers`.
        """
        options = copy.deepcopy(self.options)
        options.bval = bval
        options.headers = headers
        options.begdate = Date(begdate) if begdate else None
        options.enddate = Date(enddate) if enddate else None

        fields = limit_fields_to_categories(fields, categories, allowopen)

        # chunk 500 fields per request
        nparts = int((len(fields) - 1) / 500) + 1
        result = Result()
        for part in range(nparts):
            logger.info(f'Lookup part {part+1:02d} / {nparts:02d}')
            i = part*500
            reqfile = options.tempdir / f'fprp{part:02d}.req'
            respfile = options.tempdir / f'fprp{part:02d}.out'
            Request.build(identifiers, fields[i:i+500], reqfile, options)
            Request.send(self.cn, reqfile, respfile, options)
            _result = Request.parse(respfile)
            result.extend(_result)
        return result


def limit_fields_to_categories(reqfields, categories, allowopen=True):
    """Filter fields to avoid expensive mistakes
    """
    allfields = Field.from_categories(categories)
    if allowopen:
        allfields = allfields | Field.open_fields
    fields = sorted([f.upper() for f in reqfields if (f.upper() in allfields)])
    logger.info(f'Filtered {len(reqfields)} request fields to {len(fields)} match fields.')
    return fields


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
