from __future__ import annotations

import contextlib
import copy
import functools
import logging
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self

import ftp
from bbdl.options import BbdlOptions
from bbdl.request import Request, Result
from date import Date
from libb import load_options

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

logger = logging.getLogger(__name__)


def parse_dates(*date_params: str) -> Callable:
    """Decorator that normalizes date parameters to Date objects.

    Handles strings via Date.parse() and date-like objects via Date.instance().
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for param in date_params:
                if param in kwargs and kwargs[param] is not None:
                    val = kwargs[param]
                    if isinstance(val, str):
                        kwargs[param] = Date.parse(val)
                    elif not isinstance(val, Date):
                        kwargs[param] = Date.instance(val)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def _parse_rundate(filepath: Path) -> Date | None:
    """Extract RUNDATE from Bloomberg response file header."""
    with Path(filepath).open() as f:
        for line in f:
            if line.startswith('RUNDATE='):
                return Date.parse(line.split('=')[1].strip())
            if line.startswith('START-OF-FIELDS'):
                break
    return None


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
    @load_options(cls=BbdlOptions)
    def __init__(self, options: str | dict | BbdlOptions | None = None, /, config: Any = None) -> None:
        self.config = config
        self.options: BbdlOptions = options

    def __enter__(self) -> Self:
        logger.debug('Entering SecureFTP client')
        _options = ftp.FtpOptions(
            hostname=self.options.hostname,
            username=self.options.username,
            password=self.options.password,
            secure=self.options.secure,
            port=self.options.port)
        self.cn = ftp.connect(_options)
        return self

    def __exit__(
        self,
        exc_ty: type[BaseException] | None,
        exc_val: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        logger.debug('Exiting SecureFTP client')
        if exc_ty:
            logger.error(exc_val)
        with contextlib.suppress(Exception):
            self.cn.close()

    @parse_dates('try_retrieve_existing_date', 'begdate', 'enddate')
    def request(
        self,
        sids: list[str | tuple] | None = None,
        fields: list[str] | None = None,
        *,
        try_retrieve_existing_date: str | Date | None = None,
        bval: bool = False,
        headers: list[str] | None = None,
        begdate: str | Date | None = None,
        enddate: str | Date | None = None,
    ) -> Result:
        """Request Bloomberg `fields` over `identifiers`.

        Args:
            sids: Security identifiers to request data for.
            fields: Bloomberg fields to request.
            try_retrieve_existing_date: If provided, retrieve existing file for
                this date instead of submitting a new request. No upload occurs.
            bval: Use BVAL evaluated pricing.
            headers: Extra request headers.
            begdate: Start date for historical requests.
            enddate: End date for historical requests.

        Returns
            Result object with data, errors, and columns.
        """
        if try_retrieve_existing_date:
            return self._fetch_by_date(try_retrieve_existing_date)

        if not sids or not fields:
            raise ValueError('sids and fields are required for new requests')

        options = copy.deepcopy(self.options)
        options.bval = bval
        options.headers = headers
        options.begdate = begdate
        options.enddate = enddate

        # chunk 500 fields per request
        nparts = int((len(fields) - 1) / 500) + 1
        result = Result()
        for part in range(nparts):
            logger.info(f'Lookup part {part+1:02d} / {nparts:02d}')
            i = part*500
            reqfile = options.tempdir / f'fprp{part:02d}.req'
            respfile = options.tempdir / f'fprp{part:02d}.out'
            Request.build(sids, fields[i:i+500], reqfile, options)
            Request.send(self.cn, reqfile, respfile, options)
            _result = Request.parse(respfile)
            result.extend(_result)
        return result

    def _fetch_by_date(self, target_date: Date) -> Result:
        """Find and download existing file for target_date.

        Files contain RUNDATE header - match against target_date.
        """
        files = self.cn.files()

        for filename in files:
            if not filename.endswith('.out'):
                continue
            localpath = self.options.tempdir / filename
            self.cn.getbinary(filename, localpath)

            rundate = _parse_rundate(localpath)
            if rundate == target_date:
                logger.info(f'Found file {filename} for date {target_date}')
                return Request.parse(localpath)

        raise FileNotFoundError(f'No file found for date {target_date}')


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
