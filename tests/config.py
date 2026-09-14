"""Settings tree for the tests, in the shape SFTPClient's config argument takes.

A caller reaches these as ``SFTPClient('bbg.mock.ftp', config)``, so the
tree has to exist at module scope under the name ``bbg``.

Notes
-----
- ``Setting._locked`` is a CLASS attribute, so ``Setting.lock()`` locks
  every Setting in the process, not just this tree. Importing a config
  module must not decide that for the whole process, so the prior lock
  state is restored on the way out rather than left locked.
"""

import os

from libb import Setting

_WAS_LOCKED = Setting._locked

Setting.unlock()

bbg = Setting()

bbg.data.ftp.hostname = 'sftp.bloomberg.com'
bbg.data.ftp.username = os.getenv('CONFIG_BBG_FTP_DATA_USERNAME')
bbg.data.ftp.password = os.getenv('CONFIG_BBG_FTP_DATA_PASSWD')
bbg.data.ftp.remotedir = '/'
bbg.data.ftp.usernumber = os.getenv('CONFIG_BBG_FTP_DATA_USERNUMBER')
bbg.data.ftp.sn = os.getenv('CONFIG_BBG_FTP_DATA_SN')
bbg.data.ftp.programflag = 'adhoc'
bbg.data.ftp.secure = True

bbg.mock.ftp.hostname = '127.0.0.1'
bbg.mock.ftp.username = 'foo'
bbg.mock.ftp.password = 'bar'
bbg.mock.ftp.port = 21
bbg.mock.ftp.usernumber = '1234567'
bbg.mock.ftp.sn = '890'
bbg.mock.ftp.ws = '1'
bbg.mock.ftp.programflag = 'adhoc'

if _WAS_LOCKED:
    Setting.lock()
