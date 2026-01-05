from __future__ import annotations

import contextlib
import copy
import functools
import gzip
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

__all__ = ['SFTPClient']

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
    filepath = Path(filepath)
    open_fn = gzip.open if '.gz' in filepath.name else open
    with open_fn(filepath, 'rt') as f:
        for line in f:
            if line.startswith('RUNDATE='):
                return Date.parse(line.split('=')[1].strip())
            if line.startswith('START-OF-FIELDS'):
                break
    return None


def _group_fields_by_overrides(
    fields: list[str],
    field_overrides: dict[str, tuple[str, str]] | None,
) -> dict[tuple, list[str]]:
    """Group fields by override configuration.

    Returns dict mapping override tuple to list of fields. Empty tuple () for
    fields with no overrides.
    """
    if not field_overrides:
        return {(): fields}

    groups: dict[tuple, list[str]] = {}
    for fld in fields:
        override = field_overrides.get(fld)
        key = override or ()
        if key not in groups:
            groups[key] = []
        groups[key].append(fld)

    return groups


def _apply_overrides_to_identifiers(
    sids: list[str | tuple],
    override_tuple: tuple,
) -> list[str | tuple]:
    """Transform identifiers to include overrides.

    Per Bloomberg docs, format is: identifier||N|field|value|
    Double pipe means empty type. String identifiers become tuples with empty
    type string: ('IBM US Equity', '', 'FUND_PER', 'Q')
    """
    if not override_tuple:
        return sids

    result = []
    for sid in sids:
        if isinstance(sid, str):
            result.append((sid, '') + override_tuple)
        else:
            result.append(tuple(sid) + override_tuple)
    return result


def _transform_fields_for_history(
    fields: list[str],
    field_overrides: dict[str, tuple[str, str]] | None,
) -> list[str]:
    """Transform field names to include gethistory overrides.

    For gethistory, overrides are embedded in field names:
    FIELD_NAME|1|override_abbrev|value|
    """
    if not field_overrides:
        return fields

    result = []
    for fld in fields:
        if fld in field_overrides:
            override = field_overrides[fld]
            result.append(f'{fld}|1|{override[0]}|{override[1]}|')
        else:
            result.append(fld)
    return result


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
        field_overrides: dict[str, tuple[str, str]] | None = None,
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
            field_overrides: Dict mapping field name to (override_name, value) tuple.
                For gethistory: ('AE', 'E') embeds override in field name.
                For getdata: ('FUND_PER', 'Q') applies override to identifiers.
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

        is_history = begdate or enddate
        result = Result()

        if is_history:
            transformed_fields = _transform_fields_for_history(fields, field_overrides)
            nparts = (len(transformed_fields) - 1) // 500 + 1
            for part in range(nparts):
                logger.info(f'Lookup part {part+1:02d} / {nparts:02d}')
                i = part * 500
                reqfile = options.tempdir / f'fprp{part:02d}.req'
                respfile = options.tempdir / f'fprp{part:02d}.out'
                Request.build(sids, transformed_fields[i:i+500], reqfile, options)
                Request.send(self.cn, reqfile, respfile, options)
                _result = Request.parse(respfile, options)
                result.merge(_result)
        else:
            groups = _group_fields_by_overrides(fields, field_overrides)
            file_idx = 0
            for override_key, group_fields in groups.items():
                group_sids = _apply_overrides_to_identifiers(sids, override_key)
                nparts = (len(group_fields) - 1) // 500 + 1
                for part in range(nparts):
                    logger.info(f'Lookup file {file_idx+1:02d}, part {part+1:02d} / {nparts:02d}')
                    i = part * 500
                    reqfile = options.tempdir / f'fprp{file_idx:02d}.req'
                    respfile = options.tempdir / f'fprp{file_idx:02d}.out'
                    Request.build(group_sids, group_fields[i:i+500], reqfile, options)
                    Request.send(self.cn, reqfile, respfile, options)
                    _result = Request.parse(respfile, options)
                    result.merge(_result)
                    file_idx += 1

        result.unwrap_single_element_lists()
        return result

    def _fetch_by_date(self, target_date: Date) -> Result:
        """Find and download all files for target_date, combining results.

        Files contain RUNDATE header - match against target_date.
        Downloads are cached in tempdir to avoid redundant re-downloads.
        """
        files = self.cn.files()
        result = Result()
        matched_files = []

        for filename in files:
            if '.out' not in filename or not filename.startswith('fprp'):
                continue
            localpath = self.options.tempdir / filename

            if not localpath.exists():
                self.cn.getbinary(filename, localpath)

            rundate = _parse_rundate(localpath)
            if rundate == target_date:
                matched_files.append(filename)
                _result = Request.parse(localpath, self.options)
                result.merge(_result)

        if not matched_files:
            raise FileNotFoundError(f'No file found for date {target_date}')

        result.unwrap_single_element_lists()

        logger.info(f'Found {len(matched_files)} files for date {target_date}: {matched_files}')
        return result


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
