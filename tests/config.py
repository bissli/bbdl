import os

from libb import Setting

bbg = Setting()
bbg.data.ftp.hostname = 'sftp.bloomberg.com'
bbg.data.ftp.username = os.getenv('CONFIG_BBG_FTP_DATA_USERNAME')
bbg.data.ftp.password = os.getenv('CONFIG_BBG_FTP_DATA_PASSWD')
bbg.data.ftp.fields_path = 'fields.csv'
bbg.data.ftp.remotedir = '/'
bbg.data.ftp.usernumber = os.getenv('CONFIG_BBG_FTP_DATA_USERNUMBER')
bbg.data.ftp.sn = os.getenv('CONFIG_BBG_FTP_DATA_SN')
bbg.data.ftp.programflag = 'adhoc'
bbg.data.ftp.secure = True
