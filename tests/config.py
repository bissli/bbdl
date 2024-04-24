import os

from libb import Setting

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
bbg.mock.ftp.programflag = 'adhoc'

Setting.lock()
