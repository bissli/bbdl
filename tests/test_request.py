import os

import pytest
from asserts import assert_equal
from bbdl import sftp
from libb.dir import make_tmpdir


def test_basic_request():
    identifiers = ['IBM US Equity', '88160RAG6 Corp']
    fields = ['ID_BB_GLOBAL', 'PARSEKYABLE_DES', 'PX_LAST']

    with make_tmpdir() as tmpdir:
        reqfile = os.path.join(tmpdir, 'reqfile.out')
        opt = sftp.Options(programflag='adhoc')
        sftp.Request.build(identifiers, fields, reqfile, opt)
        with open(reqfile, 'r') as f:
            resp = f.read()
        expected = """\
START-OF-FILE
FIRMNAME=None
PROGRAMFLAG=adhoc
DELIMITER=|
ADJUSTED=yes
DATEFORMAT=yyyymmdd
SECMASTER=yes
CLOSINGVALUES=yes
DERIVED=yes

START-OF-FIELDS
ID_BB_GLOBAL
PARSEKYABLE_DES
PX_LAST
END-OF-FIELDS

START-OF-DATA
IBM US Equity
88160RAG6 Corp
END-OF-DATA

END-OF-FILE
"""
        assert_equal(resp, expected)


if __name__ == '__main__':
    pytest.main([__file__])
