BBDL
====

An API for interfacing with the Bloomberg Data License. Currently only supports SFTP.

See https://developer.blpprofessional.com/portal/products/dl?chapterId=4564

Uses libb https://github.com/bissli/libb.git

Follows https://12factor.net/config

Install
=======

```python

# using pip
pip install -e git+https://github.com/bissli/bbdl.git#egg=bbdl[libb]

# using poetry
poetry add git+https://github.com/bissli/bbdl.git -E libb

```

Example
=======
config.py
```python

from libb.config import *

Setting.unlock()

bbg = Setting()
bbg.data.ftp.hostname = 'sftp.bloomberg.com'
bbg.data.ftp.username = os.getenv('MY_USERNAME')
bbg.data.ftp.password = os.getenv('MY_PASSWORD')
bbg.data.ftp.remotedir = '/'
bbg.data.ftp.secure = True
# only if using terminal
bbg.data.ftp.usernumber = os.getenv('MY_USERNUMBER')
bbg.data.ftp.sn = os.getenv('MY_SN')

Setting.lock()

```

runner.py
```python

import config
from bbdl import SFTP, Ticker

sftp = SFTP()

identifiers = ['IBM Us Equity', 'AAPL Us Equity']
identifiers = [Ticker.fix_case(ticker) for ticker in identifiers]

fields = [
    'id_bb_unique',
    'id_cusip',
    'id_isin',
    'id_sedol1',
    'parsekyable_des',
    'security_des',
]

sftp = SFTP(localconfig, 'bbg.data')
data, errors = sftp.request(identifiers, fields, categories=['Security Master'])

```
