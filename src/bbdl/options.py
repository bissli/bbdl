"""WARNINGS:

programflag: when using `oneshot` Bloomberg will lock for four months the
             data categories pulled. So if you pull using `oneshot` $10k
             worth of data in a single pull, you will pay at minimum $10k
             for four months in a row. On the other hand `oneshot` will
             be cheaper than `adhoc` if the fields selected are the actual
             ones desired.

"""

from dataclasses import dataclass, field
from pathlib import Path

from bbdl.exceptions import BbdlValidationError
from date import Date
from libb import ConfigOptions, get_tempdir

__all__ = ['BbdlOptions']


@dataclass
class BbdlOptions(ConfigOptions):

    bval: bool = False
    headers: list = field(default_factory=list)
    begdate: Date | None = None
    enddate: Date | None = None
    compressed: bool = False
    dateformat: str = 'yyyymmdd'
    programflag: str = 'adhoc'
    delimiter: str = '|'
    wait_time: int = 20
    sn: str | None = None
    ws: str | None = None
    username: str | None = None
    usernumber: str | None = None
    hostname: str = 'sftp.bloomberg.com'
    password: str | None = None
    remotedir: str = '/'
    secure: bool = True
    port: int = 22
    tempdir: Path = None
    is_bba: bool = False  # linking to Bloomberg Anywhere (BBA) terminal

    def __post_init__(self):
        self.begdate = Date(self.begdate) if self.begdate else None
        self.enddate = Date(self.enddate) if self.enddate else None
        if self.programflag not in {'oneshot', 'adhoc'}:
            raise BbdlValidationError(f"programflag must be 'oneshot' or 'adhoc', got '{self.programflag}'")
        if is_terminal(self):
            terminal_bba(self) if self.is_bba else terminal_open(self)
        if self.tempdir is None:
            self.tempdir = get_tempdir().dir
        self.tempdir = Path(self.tempdir)


def is_terminal(options):
    return options.sn or options.ws or options.usernumber


def terminal_open(options):
    """Non-BBA (open) terminal linking.
    To locate run IAM <GO>
    From the field S/N: SN is the prefix before the dash. WS is the suffix
    after the dash.
    """
    if not options.sn:
        raise BbdlValidationError('SN must be provided for terminal linking')
    if not options.ws:
        raise BbdlValidationError('WS must be provided for terminal linking')


def terminal_bba(options):
    """BBA terminal linking.

    If linking to a BBA (Bloomberg Anywhere) terminal,
    only the USERNUMBER should be included in the header
    of the request. Including SN and WS could negatively
    impact the processing of the request.
    """
    options.sn = options.ws = None
    if not options.usernumber:
        raise BbdlValidationError('Usernumber must be provided for BBA terminal linking')


if __name__ == '__main__':
    options = BbdlOptions()
    print(options)
