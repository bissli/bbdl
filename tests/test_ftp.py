import os
from collections import deque
from io import IOBase

import config
import pytest
import wrapt

import bbdl

fields = [
    'ID_BB_UNIQUE',
    'ID_BB_GLOBAL',
    'PARSEKYABLE_DES',
    'SECURITY_DES',
    'REAL_BLOOMBERG_DEPT_DES',
    'ISSUE_DT',
    'CPN',
    'CPN_FREQ',
    'DAY_CNT_DES',
    'FIXED',
    'MATURITY',
    'IS_PERPETUAL',
    'CRNCY',
    'AMT_ISSUED',
    'CDS_SPREAD_TICKER_5Y',
    'CALL_DAYS_NOTICE',
    'SOFT_CALL_TRIGGER',
    'SOFT_CALL_DAYS',
    'SOFT_CALL_OUT_OF',
    'SOFT_CALL_END',
    'CALL_SCHEDULE',
    'PUT_SCHEDULE',
    'SOFT_CALL_SCHEDULE',
    'EXPECTED_REPORT_DT',
    'EQY_SPLIT_DT',
    'DVD_PAY_DT',
    'DVD_EX_DT',
    'DVD_FREQ',
    'AMT_OUTSTANDING',
    'EQY_SH_OUT',
    'TRAIL_12M_DVD_PER_SH',
    'PX_LAST',
    'PX_CLOSE_DT',
    'EQY_BETA',
    'VOLATILITY_60D',
    'VOLATILITY_260D',
    'VOLATILITY_360D',
    'VOLUME_AVG_30D',
    'VOLUME_AVG_3M',
    'HIGH_52WEEK',
    'LOW_52WEEK',
    'SHORT_INT',
    'FIVEY_MID_CDS_SPREAD',
    'THIRTYDAY_IMPVOL_100PT0PCTMNY_DF',
    'CURR_ENTP_VAL',
    'EBITDA',
    'NET_DEBT',
    'EST_PE_CUR_YR',
    'CUR_MKT_CAP',
    'IS_INT_EXPENSE',
    'MARKET_STATUS',
    'ULT_PARENT_TICKER_EXCHANGE',
    'ID_BB_COMPANY',
    'ISSUER',
    'CNTRY_OF_DOMICILE',
    'COUNTRY_ISO',
    'GICS_SUB_INDUSTRY',
    'EARNINGS_CONF_CALL_DT',
    'EARNINGS_CONF_CALL_TIME',
    'DVD_SH_LAST',
    'LOW_DT_52WEEK',
    'HIGH_DT_52WEEK',
    'DDIS_AMT_OUTSTANDING_BY_YR_BNDLN',
    'TRAIL_12M_EBITDA',
    'SHORT_AND_LONG_TERM_DEBT',
    'EARNINGS_CONF_CALL_PHONE_NUM',
    'EARNINGS_CONF_CALL_PIN',
    'CENTRAL_INDEX_KEY_NUMBER',
    'EQY_WEIGHTED_AVG_PX',
    'VOLATILITY_30D',
    'EQY_FUND_TICKER',
    'BDVD_NEXT_EST_EX_DT',
    'BDVD_NEXT_EST_PAY_DT',
    'EQY_SPLIT_ADJUSTMENT_FACTOR',
    'BS_TOT_NON_CUR_ASSET',
    'EQY_PO_DT',
    'EQY_FUND_CRNCY',
    'CF_FREE_CASH_FLOW',
    'CASH_AND_MARKETABLE_SECURITIES',
    'INDUSTRY_SECTOR',
    'FIRST_SETTLE_DT',
    'LONG_COMP_NAME',
    'PRIMARY_EXCHANGE_NAME',
    'NXT_PUT_DT',
    'NXT_PUT_PX',
    'PX_ASK',
    'PX_BID',
    'MTY_YEARS',
    'PAR_AMT',
    'SALES_RESULT_DATE',
    'SALES_RESULT_TIME',
    'ALL_HOLDERS_PUBLIC_FILINGS',
    'CREATED_ON',
    'NXT_CPN_DT',
    'PREV_CPN_DT',
    'SECURITY_TYP2',
    'TOT_DEBT_TO_TOT_CAP',
    'FUT_CNV_RISK_CTD',
    'FUT_CTD_TICKER',
    'FUT_CNVS_FACTOR',
    'YAS_RISK',
]

theids = [
    'CDX IG CDSI GEN 5Y Corp',
    'AAPL US Equity',
    'BBG012HQMQH8 Corp',  # COIN 3 3/8 10/01/28
    '91282CKY Govt',  # T 4 5/8 6/30/26
    'TUU4 Comdty',
]


def as_posix(path):
    if not path:
        return path
    return str(path).replace(os.sep, '/')


class MockFTPConnection:
    """Mock FTP lib for testing
    """
    def __init__(self):
        self._files: list = None
        self._size: float = 0
        self._dirlist: list = []
        self._exists: bool = True
        self._stack = deque()
        self._contents: str = ''

    def _set_files(self, files):
        self._files = files

    def _set_dirlist(self, dirlist):
        self._dirlist = dirlist

    def _set_exists(self, exists):
        self._exists = exists

    def _set_contents(self, contents):
        self._contents = contents

    def pwd(self):
        return '/'.join(self._stack)

    def cd(self, path: str):
        path = as_posix(path)
        if not self._exists:
            self._exists = True
            raise Exception("Doesn't exist")
        for dir_ in path.split('/'):
            if dir_ == '..':
                self._stack.pop()
            else:
                self._stack.append(dir_)

    def dir(self, callback):
        for dir_ in self._dirlist:
            callback(dir_)

    def files(self):
        return self._files

    def getascii(self, remotefile, localfile, callback):
        callback(self._contents)

    getbinary = getascii

    def putascii(self, f: IOBase, *_):
        pass

    putbinary = putascii

    def delete(self, remotefile):
        if not self._exists:
            raise Exception("Doesn't exist")
        return True

    def close(self):
        return True


@wrapt.patch_function_wrapper('ftp', 'connect')
def patch_connection(wrapped, instance, args, kwargs):
    """Return our Mocker"""
    return MockFTPConnection()


@pytest.mark.skip(reason='Mock does not return response files; needs proper setup')
def test_starter():
    with bbdl.SFTPClient('bbg.mock.ftp', config) as sftp:
        result = sftp.request(theids, fields)


if __name__ == '__main__':
    pytest.main([__file__])
