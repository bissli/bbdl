from dataclasses import dataclass, field
from pathlib import Path

from date import Date
from libb import ConfigOptions, get_tempdir


@dataclass
class Options(ConfigOptions):

    bval: bool = False
    headers: list = field(default_factory=list)
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
    hostname: str = 'sftp.bloomberg.com'
    password: str | None = None
    remotedir: str = '/'
    secure: bool = True
    port: int = 22
    tempdir: Path = None

    def __post_init__(self):
        self.begdate = Date(self.begdate) if self.begdate else None
        self.enddate = Date(self.enddate) if self.enddate else None
        if self.tempdir is None:
            self.tempdir = get_tempdir().dir
        self.tempdir = Path(self.tempdir)


if __name__ == '__main__':
    options = Options()
    print(options)
