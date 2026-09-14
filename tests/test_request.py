"""Unit tests for bbdl.request module."""

import io
from pathlib import Path

import pytest
from asserts import assert_equal
from opendate import Date

from bbdl import BbdlOptions, Request, Result
from bbdl.exceptions import BbdlParseError
from bbdl.request import ERROR_MESSAGE, _parse
from libb.dir import make_tmpdir

FIXTURES_DIR = Path(__file__).parent / 'fixtures' / 'samples'


def _find(data, identifier_part):
    """Find a record by partial identifier match."""
    return next(r for r in data if identifier_part in r['IDENTIFIER'])


class TestResultUnwrapSingleElementLists:
    """Tests for Result.unwrap_single_element_lists()"""

    def test_unwraps_single_element_lists(self):
        """Single-element lists should be unwrapped to scalars"""
        result = Result()
        result.data = [
            {'px_last': [100.5], 'volume': [1000], 'name': ['IBM']},
            {'px_last': [101.0], 'volume': [2000], 'name': ['AAPL']},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['px_last'] == 100.5
        assert result.data[0]['volume'] == 1000
        assert result.data[0]['name'] == 'IBM'
        assert result.data[1]['px_last'] == 101.0

    def test_preserves_multi_element_lists_for_bulk_fields(self):
        """Multi-element lists should be preserved for bulk field types"""
        result = Result()
        result.data = [
            {'CALL_SCHEDULE': [(Date(2025, 1, 1), 100.0), (Date(2026, 1, 1), 100.0)], 'single': [42]},
        ]
        result.unwrap_single_element_lists()
        assert len(result.data[0]['CALL_SCHEDULE']) == 2
        assert result.data[0]['single'] == 42

    def test_unwraps_multi_element_lists_for_scalar_fields(self):
        """Multi-element lists for scalar fields should take first non-null value"""
        result = Result()
        result.data = [
            {'COUNTRY_ISO': [None, 'US', 'GB'], 'PX_LAST': [100.5]},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['COUNTRY_ISO'] == 'US'
        assert result.data[0]['PX_LAST'] == 100.5

    def test_unwraps_all_null_list_to_none(self):
        """Multi-element list of all nulls should become None"""
        result = Result()
        result.data = [
            {'COUNTRY_ISO': [None, None, None], 'PX_LAST': [100.5]},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['COUNTRY_ISO'] is None
        assert result.data[0]['PX_LAST'] == 100.5

    def test_unwraps_empty_list_to_none(self):
        """Empty list should become None"""
        result = Result()
        result.data = [
            {'COUNTRY_ISO': [], 'PX_LAST': [100.5]},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['COUNTRY_ISO'] is None
        assert result.data[0]['PX_LAST'] == 100.5

    def test_preserves_scalars(self):
        """Scalar values should remain unchanged"""
        result = Result()
        result.data = [
            {'px_last': 100.5, 'name': 'IBM', 'count': 42},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['px_last'] == 100.5
        assert result.data[0]['name'] == 'IBM'
        assert result.data[0]['count'] == 42

    def test_converts_nan_to_none(self):
        """NaN float values should be converted to None"""
        result = Result()
        result.data = [
            {'px_last': float('nan'), 'volume': [float('nan')]},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['px_last'] is None
        assert result.data[0]['volume'] is None

    def test_converts_inf_to_none(self):
        """Inf float values should be converted to None"""
        result = Result()
        result.data = [
            {'px_last': float('inf'), 'volume': [float('-inf')]},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['px_last'] is None
        assert result.data[0]['volume'] is None

    def test_handles_none_values(self):
        """None values should remain None"""
        result = Result()
        result.data = [
            {'px_last': None, 'name': [None]},
        ]
        result.unwrap_single_element_lists()
        assert result.data[0]['px_last'] is None
        assert result.data[0]['name'] is None

    def test_handles_empty_data(self):
        """Empty data list should not raise"""
        result = Result()
        result.data = []
        result.unwrap_single_element_lists()
        assert result.data == []


class TestResultToDataframe:
    """Tests for Result.to_dataframe()"""

    def test_creates_dataframe_from_data(self):
        """Should create DataFrame from result data"""
        result = Result()
        result.data = [
            {'ticker': 'IBM', 'px_last': 100.5},
            {'ticker': 'AAPL', 'px_last': 150.0},
        ]
        result.columns = [('ticker', str), ('px_last', float)]
        df = result.to_dataframe()
        assert len(df) == 2
        assert list(df.columns) == ['ticker', 'px_last']
        assert df.iloc[0]['ticker'] == 'IBM'
        assert df.iloc[0]['px_last'] == 100.5

    def test_unwraps_single_element_lists_automatically(self):
        """to_dataframe should automatically unwrap single-element lists"""
        result = Result()
        result.data = [
            {'ticker': ['IBM'], 'px_last': [100.5], 'volume': [1000]},
            {'ticker': ['AAPL'], 'px_last': [150.0], 'volume': [2000]},
        ]
        result.columns = [('ticker', str), ('px_last', float), ('volume', int)]
        df = result.to_dataframe()
        assert df.iloc[0]['ticker'] == 'IBM'
        assert df.iloc[0]['px_last'] == 100.5
        assert df.iloc[0]['volume'] == 1000
        assert df.iloc[1]['ticker'] == 'AAPL'

    def test_handles_mixed_scalar_and_list_values(self):
        """Should handle mix of scalars and single-element lists"""
        result = Result()
        result.data = [
            {'identifier': 'IBM', 'px_last': [100.5], 'retcode': 0},
        ]
        result.columns = [('identifier', str), ('px_last', float), ('retcode', int)]
        df = result.to_dataframe()
        assert df.iloc[0]['identifier'] == 'IBM'
        assert df.iloc[0]['px_last'] == 100.5
        assert df.iloc[0]['retcode'] == 0


class TestRequestParse:
    """Tests for Request.parse() response parsing"""

    def test_parse_non_historical_response(self):
        """Should parse non-historical response correctly"""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
ID_BB_GLOBAL
PX_LAST
END-OF-FIELDS
START-OF-DATA
IBM US Equity|0|2|BBG000BLNNH6|145.50|
AAPL US Equity|0|2|BBG000B9XRY4|175.25|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        assert len(result.data) == 2
        assert result.data[0]['IDENTIFIER'] == 'IBM US Equity'
        assert result.data[0]['ID_BB_GLOBAL'] == 'BBG000BLNNH6'
        assert result.data[0]['PX_LAST'] == 145.50
        assert not isinstance(result.data[0]['PX_LAST'], list)
        assert ('IDENTIFIER', str) in result.columns
        assert ('PX_LAST', float) in result.columns

    def test_parse_historical_response_single_date(self):
        """Should parse historical response and wrap values in lists"""
        response = """\
START-OF-FILE
PROGRAMNAME=gethistory
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
START-OF-DATA
IBM US Equity|0|1|20251215|145.50|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        assert len(result.data) == 1
        assert result.data[0]['PX_LAST'] == [145.50]
        assert result.data[0]['DATE'] == [Date(2025, 12, 15)]

    def test_parse_historical_response_multiple_dates(self):
        """Should aggregate historical data by identifier"""
        response = """\
START-OF-FILE
PROGRAMNAME=gethistory
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
START-OF-DATA
IBM US Equity|0|1|20251215|145.50|
IBM US Equity|0|1|20251216|146.00|
IBM US Equity|0|1|20251217|147.25|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        assert len(result.data) == 1
        assert result.data[0]['IDENTIFIER'] == 'IBM US Equity'
        assert result.data[0]['PX_LAST'] == [145.50, 146.00, 147.25]
        assert len(result.data[0]['DATE']) == 3

    def test_parse_historical_single_date_to_dataframe(self):
        """Single-date historical response should unwrap to scalars in DataFrame"""
        response = """\
START-OF-FILE
PROGRAMNAME=gethistory
START-OF-FIELDS
PX_LAST
PX_BID
END-OF-FIELDS
START-OF-DATA
IBM US Equity|0|2|20251215|145.50|145.25|
AAPL US Equity|0|2|20251215|175.25|175.00|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        df = result.to_dataframe()
        assert df.iloc[0]['PX_LAST'] == 145.50
        assert df.iloc[0]['PX_BID'] == 145.25
        assert df.iloc[1]['PX_LAST'] == 175.25
        assert not isinstance(df.iloc[0]['PX_LAST'], list)

    def test_parse_error_response(self):
        """Should capture error responses"""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
START-OF-DATA
INVALID|10|0|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        assert len(result.data) == 0
        assert len(result.errors) == 1
        assert result.errors[0]['RETCODE'] == '10'


class TestParseMalformedResponse:
    """Tests for _parse() guards against a malformed or truncated response"""

    def test_truncated_before_end_of_fields(self):
        """Verify _parse() raises rather than looping at EOF in the field scan.

        Mutation: dropping the `if not raw` guard from the field loop, so
            readline() returns '' forever and the loop never terminates.
        Oracle: a body whose END-OF-FIELDS sentinel is absent; the loop
            either raises or hangs, and pytest.raises distinguishes them.
        """
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
PX_LAST
"""
        with pytest.raises(BbdlParseError, match='END-OF-FIELDS'):
            _parse(io.StringIO(response))

    def test_truncated_before_end_of_data(self):
        """Verify _parse() raises rather than looping at EOF in the data scan.

        Mutation: dropping the `if not raw` guard from the data loop; the
            field loop completes, so only the second guard is under test.
        Oracle: a body that closes END-OF-FIELDS but never END-OF-DATA.
        """
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
START-OF-DATA
IBM US Equity|0|1|145.50|
"""
        with pytest.raises(BbdlParseError, match='END-OF-DATA'):
            _parse(io.StringIO(response))

    def test_duplicate_start_of_fields(self):
        """Verify a second START-OF-FIELDS raises instead of restarting.

        Mutation: inverting `if infields` to `if not infields`, which
            swallows the duplicate and silently discards the first block.
        Oracle: a body with two START-OF-FIELDS lines.
        """
        response = """\
START-OF-FIELDS
PX_LAST
START-OF-FIELDS
"""
        with pytest.raises(BbdlParseError, match='already parsing fields'):
            _parse(io.StringIO(response))

    def test_end_of_data_without_start(self):
        """Verify a bare END-OF-DATA raises instead of returning empty.

        Mutation: inverting `if not indata` to `if indata`, which turns a
            response whose data block never opened into a silent success.
        Oracle: a body that closes the field block, then jumps straight to
            END-OF-DATA.
        """
        response = """\
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
END-OF-DATA
"""
        with pytest.raises(BbdlParseError, match='not parsing data'):
            _parse(io.StringIO(response))

    def test_end_of_fields_without_start(self):
        """Verify a bare END-OF-FIELDS raises instead of yielding no fields.

        Mutation: inverting `if not infields` to `if infields`, which lets
            a response whose field block never opened parse as fieldless.
        Oracle: a body whose first sentinel is END-OF-FIELDS.
        """
        response = """\
START-OF-FILE
END-OF-FIELDS
"""
        with pytest.raises(BbdlParseError, match='not parsing fields'):
            _parse(io.StringIO(response))

    def test_duplicate_start_of_data(self):
        """Verify a second START-OF-DATA raises instead of restarting.

        Mutation: inverting `if indata` to `if not indata`, which swallows
            the duplicate and silently discards the rows already parsed.
        Oracle: a body with two START-OF-DATA lines.
        """
        response = """\
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
START-OF-DATA
START-OF-DATA
"""
        with pytest.raises(BbdlParseError, match='already parsing data'):
            _parse(io.StringIO(response))


class TestParseErrorRows:
    """Tests for _parse()'s non-zero RETCODE and conversion-failure paths"""

    def test_error_row_carries_looked_up_message(self):
        """Verify an error row's RETMSG comes from ERROR_MESSAGE[RETCODE].

        Mutation: ERROR_MESSAGE.get(flds[2]) in place of flds[1], which
            looks the message up under NFIELDS instead of the return code;
            or row.RETMSG = None, which drops the message entirely.
        Oracle: ERROR_MESSAGE['10'], the module's own mapping, against a
            row whose RETCODE is 10 and whose NFIELDS is 0.
        """
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
PX_LAST
END-OF-FIELDS
START-OF-DATA
INVALID|10|0|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        assert len(result.errors) == 1
        assert result.errors[0]['RETMSG'] == ERROR_MESSAGE['10']
        assert result.errors[0]['IDENTIFIER'] == 'INVALID'

    def test_unconvertible_value_becomes_none(self):
        """Verify a field that cannot convert lands as None, not as ''.

        Mutation: row[fld] = "" in the conversion handler, which turns a
            failed numeric conversion into an empty string and breaks any
            caller testing `is None`; or Field.to_type(None) in the column
            append, which raises instead of recording the object fallback.
        Oracle: an unknown mnemonic, for which Field.to_python raises
            ValueError and Field.to_type returns object.
        """
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
NOT_A_REAL_FIELD_XYZ
END-OF-FIELDS
START-OF-DATA
IBM US Equity|0|1|garbage|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response))
        assert len(result.data) == 1
        assert result.data[0]['NOT_A_REAL_FIELD_XYZ'] is None
        assert ('NOT_A_REAL_FIELD_XYZ', object) in result.columns


class TestRequestBuild:
    """Tests for Request.build()"""

    def test_basic_request(self):
        """Should build a valid request file"""
        identifiers = ['IBM US Equity', '88160RAG6 Corp']
        fields = ['ID_BB_GLOBAL', 'PARSEKYABLE_DES', 'PX_LAST']

        with make_tmpdir() as tmpdir:
            reqfile = Path(tmpdir) / 'reqfile.out'
            options = BbdlOptions(programflag='adhoc')
            Request.build(identifiers, fields, reqfile, options)
            with reqfile.open('r') as f:
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


class TestComprehensiveFixtures:
    """Tests using comprehensive fixture files with real Bloomberg data patterns."""

    @pytest.fixture(scope='class')
    def bonds_result(self):
        with (FIXTURES_DIR / 'comprehensive' / 'response_bonds.out').open() as f:
            return _parse(f)

    @pytest.fixture(scope='class')
    def converts_result(self):
        with (FIXTURES_DIR / 'comprehensive' / 'response_converts.out').open() as f:
            return _parse(f)

    @pytest.fixture(scope='class')
    def equity_result(self):
        with (FIXTURES_DIR / 'comprehensive' / 'response_equity.out').open() as f:
            return _parse(f)

    def test_bonds_parsing(self, bonds_result):
        """Test parsing bonds response with various bond types and error codes."""
        # Should have 3 valid securities (GT3 Govt, SNAP Corp, SAVE TL)
        assert len(bonds_result.data) == 3

        # Check government bond
        gt3 = _find(bonds_result.data, 'GT3')
        assert gt3['CPN'] == 3.5
        assert gt3['CPN_FREQ'] == 2
        assert gt3['DAY_CNT_DES'] == 'ACT/ACT'
        assert gt3['DEFAULTED'] is False

        # Check corporate bond with call schedule
        snap = _find(bonds_result.data, 'SNAP')
        assert snap['CPN'] == 6.875
        assert snap['INDUSTRY_SECTOR'] == 'Communications'
        assert snap['CALL_SCHEDULE'] is not None
        assert isinstance(snap['CALL_SCHEDULE'], list)
        assert len(snap['CALL_SCHEDULE']) > 0

        # Check errors (code 10 = invalid, code 11 = restricted)
        assert len(bonds_result.errors) == 2
        error_codes = {e['RETCODE'] for e in bonds_result.errors}
        assert '10' in error_codes
        assert '11' in error_codes

    def test_converts_parsing(self, converts_result):
        """Test parsing convertible bonds with bulk fields."""
        # Should have 4 valid convertibles, 2 errors
        assert len(converts_result.data) == 4
        assert len(converts_result.errors) == 2

        # Check US convertible with soft call schedule (ARRY bond)
        arry = next(r for r in converts_result.data if r['IDENTIFIER'] == 'BBG01VRLWHP4')
        assert arry['CPN'] == 2.875
        assert arry['CV_CNVS_RATIO'] == 123.1262
        assert arry['INITIAL_CONVERSION_PREMIUM'] == 27.5
        assert arry['SOFT_CALL_SCHEDULE'] is not None
        assert isinstance(arry['SOFT_CALL_SCHEDULE'], list)
        assert arry['SOFT_CALL_TRIGGER'] == 130.0

        # Check European convertible (FR)
        su = next(r for r in converts_result.data if r['IDENTIFIER'] == 'BBG01K887NJ0')
        assert su['COUNTRY_ISO'] == 'FR'
        assert su['CPN'] == 1.97

        # Check zero coupon convert (BOX)
        box = next(r for r in converts_result.data if r['IDENTIFIER'] == 'BBG00YVG4RF5')
        assert box['CPN'] == 0

        # Check Japanese convert with put schedule
        kacapi = next(r for r in converts_result.data if r['IDENTIFIER'] == 'BBG01QD4Q4C8')
        assert kacapi['COUNTRY_ISO'] == 'JP'
        assert kacapi['PUT_SCHEDULE'] is not None
        assert kacapi['SOFT_CALL_DAYS'] is None  # N.A. in source

    def test_equity_parsing(self, equity_result):
        """Test parsing equity response with multiple countries and null handling."""
        # Should have 4 valid equities, 1 error
        assert len(equity_result.data) == 4
        assert len(equity_result.errors) == 1

        # Check US equity
        aapl = _find(equity_result.data, 'AAPL')
        assert aapl['COUNTRY_ISO'] == 'US'
        assert aapl['GICS_SECTOR'] == 45
        assert aapl['GICS_SECTOR_NAME'] == 'Information Technology'
        assert isinstance(aapl['EQY_BETA'], float)
        assert isinstance(aapl['CUR_MKT_CAP'], float)

        # Check Japanese equity with N.A. values
        ibiden = _find(equity_result.data, '4062')
        assert ibiden['COUNTRY_ISO'] == 'JP'
        assert ibiden['DVD_FREQ'] == 'Semi-Anl'
        assert ibiden['LOW_52WEEK'] is None
        assert ibiden['LOW_DT_52WEEK'] is None

        # Check Taiwan equity with N.A. values
        gigabyte = _find(equity_result.data, '2376')
        assert gigabyte['COUNTRY_ISO'] == 'TW'
        assert gigabyte['DVD_SH_LAST'] is None

    def test_bulk_field_call_schedule(self, bonds_result):
        """Test detailed parsing of CALL_SCHEDULE bulk field with custom mappings."""
        snap = _find(bonds_result.data, 'SNAP')
        call_schedule = snap['CALL_SCHEDULE']

        # With use_custom_mappings=True (default), should be list of dicts
        assert len(call_schedule) >= 3
        for item in call_schedule:
            assert isinstance(item, dict)
            assert 'Call Date' in item
            assert 'Call Price' in item
            assert isinstance(item['Call Date'], Date)
            assert isinstance(item['Call Price'], (int, float))

    def test_bulk_field_soft_call_schedule(self, converts_result):
        """Test detailed parsing of SOFT_CALL_SCHEDULE bulk field with custom mappings."""
        arry = next(r for r in converts_result.data if r['IDENTIFIER'] == 'BBG01VRLWHP4')
        soft_call = arry['SOFT_CALL_SCHEDULE']

        # With use_custom_mappings=True (default), should be list of dicts
        assert soft_call is not None
        assert isinstance(soft_call, list)
        assert len(soft_call) >= 1
        assert isinstance(soft_call[0], dict)
        assert 'Soft Call Date' in soft_call[0]
        assert 'Soft Call Price' in soft_call[0]

    def test_to_dataframe(self, equity_result):
        """Test converting comprehensive fixture results to DataFrame."""
        df = equity_result.to_dataframe()

        assert len(df) == 4
        assert 'IDENTIFIER' in df.columns
        assert 'PX_LAST' in df.columns
        assert 'COUNTRY_ISO' in df.columns

        # Check values are scalars, not lists
        for col in ['PX_LAST', 'PX_BID', 'PX_ASK']:
            if col in df.columns:
                for val in df[col]:
                    if val is not None:
                        assert not isinstance(val, list)


class TestCustomMappingsOption:
    """Tests for use_custom_mappings option controlling bulk field formatting."""

    def test_custom_mappings_enabled_returns_dicts(self):
        """With use_custom_mappings=True, bulk fields return list[dict]."""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
SOFT_CALL_SCHEDULE
END-OF-FIELDS
START-OF-DATA
TEST|0|1|;2;1;2;5;20310401;3;100.00000;|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response), use_custom_mappings=True)
        soft_call = result.data[0]['SOFT_CALL_SCHEDULE']
        assert isinstance(soft_call, list)
        assert isinstance(soft_call[0], dict)
        assert 'Soft Call Date' in soft_call[0]
        assert 'Soft Call Price' in soft_call[0]
        assert soft_call[0]['Soft Call Date'] == Date(2031, 4, 1)
        assert soft_call[0]['Soft Call Price'] == 100.0

    def test_custom_mappings_disabled_returns_tuples(self):
        """With use_custom_mappings=False, bulk fields return list[tuple]."""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
SOFT_CALL_SCHEDULE
END-OF-FIELDS
START-OF-DATA
TEST|0|1|;2;1;2;5;20310401;3;100.00000;|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response), use_custom_mappings=False)
        soft_call = result.data[0]['SOFT_CALL_SCHEDULE']
        assert isinstance(soft_call, list)
        assert isinstance(soft_call[0], tuple)
        assert soft_call[0] == (Date(2031, 4, 1), 100.0)

    def test_custom_mappings_call_schedule(self):
        """Test CALL_SCHEDULE with both custom mappings modes."""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
CALL_SCHEDULE
END-OF-FIELDS
START-OF-DATA
TEST|0|1|;2;2;2;5;20260315;3;102.5;5;20270315;3;101.0;|
END-OF-DATA
END-OF-FILE
"""
        # With custom mappings enabled
        result_enabled = _parse(io.StringIO(response), use_custom_mappings=True)
        call_schedule = result_enabled.data[0]['CALL_SCHEDULE']
        assert isinstance(call_schedule[0], dict)
        assert call_schedule[0]['Call Date'] == Date(2026, 3, 15)
        assert call_schedule[0]['Call Price'] == 102.5
        assert call_schedule[1]['Call Date'] == Date(2027, 3, 15)
        assert call_schedule[1]['Call Price'] == 101.0

        # With custom mappings disabled
        result_disabled = _parse(io.StringIO(response), use_custom_mappings=False)
        call_schedule = result_disabled.data[0]['CALL_SCHEDULE']
        assert isinstance(call_schedule[0], tuple)
        assert call_schedule[0] == (Date(2026, 3, 15), 102.5)
        assert call_schedule[1] == (Date(2027, 3, 15), 101.0)

    def test_custom_mappings_put_schedule(self):
        """Test PUT_SCHEDULE with custom mappings enabled."""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
PUT_SCHEDULE
END-OF-FIELDS
START-OF-DATA
TEST|0|1|;2;1;2;5;20271105;3;100.00000;|
END-OF-DATA
END-OF-FILE
"""
        result = _parse(io.StringIO(response), use_custom_mappings=True)
        put_schedule = result.data[0]['PUT_SCHEDULE']
        assert isinstance(put_schedule[0], dict)
        assert 'Put Date' in put_schedule[0]
        assert 'Put Price' in put_schedule[0]
        assert put_schedule[0]['Put Date'] == Date(2027, 11, 5)
        assert put_schedule[0]['Put Price'] == 100.0

    def test_non_bulk_fields_unaffected(self):
        """Non-bulk fields should work the same regardless of custom mappings setting."""
        response = """\
START-OF-FILE
PROGRAMNAME=getdata
START-OF-FIELDS
PX_LAST
CPN
END-OF-FIELDS
START-OF-DATA
TEST|0|2|145.50|2.875|
END-OF-DATA
END-OF-FILE
"""
        result_enabled = _parse(io.StringIO(response), use_custom_mappings=True)
        result_disabled = _parse(io.StringIO(response), use_custom_mappings=False)

        assert result_enabled.data[0]['PX_LAST'] == 145.50
        assert result_disabled.data[0]['PX_LAST'] == 145.50
        assert result_enabled.data[0]['CPN'] == 2.875
        assert result_disabled.data[0]['CPN'] == 2.875


class TestGroupFieldsByOverrides:
    """Tests for _group_fields_by_overrides() helper function."""

    def test_no_overrides_returns_single_group(self):
        """All fields in one group when no overrides specified."""
        from bbdl.client import _group_fields_by_overrides
        fields = ['PX_LAST', 'EBITDA', 'NET_DEBT']
        result = _group_fields_by_overrides(fields, None)
        assert result == {(): ['PX_LAST', 'EBITDA', 'NET_DEBT']}

    def test_empty_overrides_returns_single_group(self):
        """All fields in one group when empty overrides dict."""
        from bbdl.client import _group_fields_by_overrides
        fields = ['PX_LAST', 'EBITDA', 'NET_DEBT']
        result = _group_fields_by_overrides(fields, {})
        assert result == {(): ['PX_LAST', 'EBITDA', 'NET_DEBT']}

    def test_single_override_separates_field(self):
        """Field with override goes to separate group."""
        from bbdl.client import _group_fields_by_overrides
        fields = ['PX_LAST', 'EBITDA', 'NET_DEBT']
        overrides = {'NET_DEBT': ('FUND_PER', 'Q')}
        result = _group_fields_by_overrides(fields, overrides)
        assert () in result
        assert ('FUND_PER', 'Q') in result
        assert result[()] == ['PX_LAST', 'EBITDA']
        assert result[('FUND_PER', 'Q')] == ['NET_DEBT']

    def test_multiple_fields_same_override(self):
        """Multiple fields with same override go to same group."""
        from bbdl.client import _group_fields_by_overrides
        fields = ['PX_LAST', 'EBITDA', 'NET_DEBT', 'SHORT_AND_LONG_TERM_DEBT']
        overrides = {
            'NET_DEBT': ('FUND_PER', 'Q'),
            'SHORT_AND_LONG_TERM_DEBT': ('FUND_PER', 'Q'),
        }
        result = _group_fields_by_overrides(fields, overrides)
        assert result[()] == ['PX_LAST', 'EBITDA']
        assert set(result[('FUND_PER', 'Q')]) == {'NET_DEBT', 'SHORT_AND_LONG_TERM_DEBT'}

    def test_different_overrides_separate_groups(self):
        """Fields with different overrides go to different groups."""
        from bbdl.client import _group_fields_by_overrides
        fields = ['PX_LAST', 'EBITDA', 'BEST_EBITDA', 'NET_DEBT']
        overrides = {
            'EBITDA': ('EQY_FUND_RELATIVE_PERIOD', '2024CY'),
            'BEST_EBITDA': ('BEST_FPERIOD_OVERRIDE', '2025Y'),
            'NET_DEBT': ('FUND_PER', 'Q'),
        }
        result = _group_fields_by_overrides(fields, overrides)
        assert result[()] == ['PX_LAST']
        assert result[('EQY_FUND_RELATIVE_PERIOD', '2024CY')] == ['EBITDA']
        assert result[('BEST_FPERIOD_OVERRIDE', '2025Y')] == ['BEST_EBITDA']
        assert result[('FUND_PER', 'Q')] == ['NET_DEBT']

    def test_preserves_field_order_within_groups(self):
        """Fields maintain original order within each group."""
        from bbdl.client import _group_fields_by_overrides
        fields = ['A', 'B', 'C', 'D', 'E']
        overrides = {'B': ('X', '1'), 'D': ('X', '1')}
        result = _group_fields_by_overrides(fields, overrides)
        assert result[()] == ['A', 'C', 'E']
        assert result[('X', '1')] == ['B', 'D']


class TestApplyOverridesToIdentifiers:
    """Tests for _apply_overrides_to_identifiers() helper function."""

    def test_no_overrides_returns_original(self):
        """Empty override tuple returns identifiers unchanged."""
        from bbdl.client import _apply_overrides_to_identifiers
        sids = ['AAPL US Equity', 'IBM US Equity']
        result = _apply_overrides_to_identifiers(sids, ())
        assert result == ['AAPL US Equity', 'IBM US Equity']

    def test_single_override_transforms_string_identifiers(self):
        """String identifiers transformed to tuples with overrides."""
        from bbdl.client import _apply_overrides_to_identifiers
        sids = ['AAPL US Equity', 'IBM US Equity']
        result = _apply_overrides_to_identifiers(sids, ('FUND_PER', 'Q'))
        assert result == [
            ('AAPL US Equity', '', 'FUND_PER', 'Q'),
            ('IBM US Equity', '', 'FUND_PER', 'Q'),
        ]

    def test_tuple_identifiers_extended_with_overrides(self):
        """Tuple identifiers get overrides appended."""
        from bbdl.client import _apply_overrides_to_identifiers
        sids = [('12345678', 'CUSIP'), ('98765432', 'CUSIP')]
        result = _apply_overrides_to_identifiers(sids, ('FUND_PER', 'Q'))
        assert result == [
            ('12345678', 'CUSIP', 'FUND_PER', 'Q'),
            ('98765432', 'CUSIP', 'FUND_PER', 'Q'),
        ]

    def test_mixed_identifiers(self):
        """Mix of string and tuple identifiers handled correctly."""
        from bbdl.client import _apply_overrides_to_identifiers
        sids = ['AAPL US Equity', ('12345678', 'CUSIP')]
        result = _apply_overrides_to_identifiers(sids, ('FUND_PER', 'Q'))
        assert result == [
            ('AAPL US Equity', '', 'FUND_PER', 'Q'),
            ('12345678', 'CUSIP', 'FUND_PER', 'Q'),
        ]

    def test_bloomberg_request_format_verification(self):
        """Verify transformed identifiers produce correct Bloomberg format."""
        from bbdl.client import _apply_overrides_to_identifiers
        sids = ['AAPL US Equity']
        result = _apply_overrides_to_identifiers(sids, ('FUND_PER', 'Q'))
        iden = result[0]
        # Should produce: AAPL US Equity||1|FUND_PER|Q
        assert iden[0] == 'AAPL US Equity'
        assert iden[1] == ''  # Empty type produces double pipe
        assert iden[2] == 'FUND_PER'
        assert iden[3] == 'Q'


class TestResultColumnsProperty:
    """Tests for Result.columns property behavior."""

    def test_columns_getter_does_not_mutate(self):
        """Accessing columns property should not mutate internal state."""
        result = Result()
        result._columns = [('A', str), ('B', int), ('A', str)]  # Duplicates

        columns1 = result.columns
        columns2 = result.columns

        assert columns1 is columns2
        assert len(columns1) == 3

    def test_columns_setter_deduplicates(self):
        """Setting columns should deduplicate the list."""
        result = Result()
        result.columns = [('A', str), ('B', int), ('A', str), ('B', int)]

        assert len(result.columns) == 2
        assert result.columns == [('A', str), ('B', int)]

    def test_add_columns_deduplicates(self):
        """_add_columns should deduplicate when adding new columns."""
        result = Result()
        result.columns = [('A', str), ('B', int)]
        result._add_columns([('B', int), ('C', float), ('A', str)])

        assert len(result.columns) == 3
        assert ('A', str) in result.columns
        assert ('B', int) in result.columns
        assert ('C', float) in result.columns


class TestResultMerge:
    """Tests for Result.merge() method."""

    def test_merge_adds_new_fields(self):
        """Merging adds new fields to existing rows."""
        result1 = Result()
        result1.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01},
            {'IDENTIFIER': 'IBM US Equity', 'PX_LAST': 291.50},
        ]
        result1.columns = [('IDENTIFIER', str), ('PX_LAST', float)]

        result2 = Result()
        result2.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 146848.00},
            {'IDENTIFIER': 'IBM US Equity', 'EBITDA': 18238.00},
        ]
        result2.columns = [('IDENTIFIER', str), ('EBITDA', float)]

        result1.merge(result2)

        assert len(result1.data) == 2
        aapl = next(r for r in result1.data if r['IDENTIFIER'] == 'AAPL US Equity')
        assert aapl['PX_LAST'] == 271.01
        assert aapl['EBITDA'] == 146848.00
        ibm = next(r for r in result1.data if r['IDENTIFIER'] == 'IBM US Equity')
        assert ibm['PX_LAST'] == 291.50
        assert ibm['EBITDA'] == 18238.00

    def test_merge_extends_columns(self):
        """Merging extends the columns list."""
        result1 = Result()
        result1.data = [{'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01}]
        result1.columns = [('IDENTIFIER', str), ('PX_LAST', float)]

        result2 = Result()
        result2.data = [{'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 146848.00}]
        result2.columns = [('IDENTIFIER', str), ('EBITDA', float)]

        result1.merge(result2)

        col_names = [c[0] for c in result1.columns]
        assert 'IDENTIFIER' in col_names
        assert 'PX_LAST' in col_names
        assert 'EBITDA' in col_names

    def test_merge_extends_errors(self):
        """Merging extends the errors list."""
        result1 = Result()
        result1.data = [{'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01}]
        result1.errors = [{'IDENTIFIER': 'BAD1', 'RETCODE': '10'}]

        result2 = Result()
        result2.data = [{'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 146848.00}]
        result2.errors = [{'IDENTIFIER': 'BAD2', 'RETCODE': '11'}]

        result1.merge(result2)

        assert len(result1.errors) == 2
        assert result1.errors[0]['IDENTIFIER'] == 'BAD1'
        assert result1.errors[1]['IDENTIFIER'] == 'BAD2'

    def test_merge_appends_unmatched_identifiers(self):
        """Identifiers in other but not in self get appended."""
        result1 = Result()
        result1.data = [{'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01}]

        result2 = Result()
        result2.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 146848.00},
            {'IDENTIFIER': 'IBM US Equity', 'EBITDA': 18238.00},
        ]

        result1.merge(result2)

        assert len(result1.data) == 2
        identifiers = {r['IDENTIFIER'] for r in result1.data}
        assert identifiers == {'AAPL US Equity', 'IBM US Equity'}

    def test_merge_handles_empty_other(self):
        """Merging empty result is a no-op."""
        result1 = Result()
        result1.data = [{'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01}]
        result1.columns = [('IDENTIFIER', str), ('PX_LAST', float)]

        result2 = Result()

        result1.merge(result2)

        assert len(result1.data) == 1
        assert result1.data[0]['PX_LAST'] == 271.01

    def test_merge_into_empty_result(self):
        """Merging into empty result adds all data."""
        result1 = Result()

        result2 = Result()
        result2.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 146848.00},
            {'IDENTIFIER': 'IBM US Equity', 'EBITDA': 18238.00},
        ]

        result1.merge(result2)

        assert len(result1.data) == 2


class TestOverrideFixtures:
    """Tests using captured Bloomberg override fixtures."""

    @pytest.fixture(scope='class')
    def baseline_result(self):
        """Parse baseline (no overrides) fixture."""
        with (FIXTURES_DIR / 'overrides' / 'baseline' / 'response.out').open() as f:
            return _parse(f)

    @pytest.fixture(scope='class')
    def fund_per_q_result(self):
        """Parse FUND_PER=Q override fixture."""
        with (FIXTURES_DIR / 'overrides' / 'fund_per_q' / 'response.out').open() as f:
            return _parse(f)

    @pytest.fixture(scope='class')
    def eqy_fund_2024cy_result(self):
        """Parse EQY_FUND_RELATIVE_PERIOD=2024CY override fixture."""
        with (FIXTURES_DIR / 'overrides' / 'eqy_fund_2024cy' / 'response.out').open() as f:
            return _parse(f)

    @pytest.fixture(scope='class')
    def best_fperiod_2025y_result(self):
        """Parse BEST_FPERIOD_OVERRIDE=2025Y override fixture."""
        with (FIXTURES_DIR / 'overrides' / 'best_fperiod_2025y' / 'response.out').open() as f:
            return _parse(f)

    def test_baseline_has_annual_ebitda(self, baseline_result):
        """Baseline request returns annual EBITDA values."""
        assert len(baseline_result.data) == 2
        aapl = _find(baseline_result.data, 'AAPL')
        ibm = _find(baseline_result.data, 'IBM')
        assert aapl['EBITDA'] == 146848.00
        assert ibm['EBITDA'] == 18238.00

    def test_fund_per_q_has_quarterly_ebitda(self, fund_per_q_result):
        """FUND_PER=Q override returns quarterly EBITDA values."""
        assert len(fund_per_q_result.data) == 2
        aapl = _find(fund_per_q_result.data, 'AAPL')
        ibm = _find(fund_per_q_result.data, 'IBM')
        # Quarterly values are significantly smaller than annual
        assert aapl['EBITDA'] == 35554.00
        assert ibm['EBITDA'] == 3853.00

    def test_eqy_fund_2024cy_has_cy2024_values(self, eqy_fund_2024cy_result):
        """EQY_FUND_RELATIVE_PERIOD=2024CY override returns CY2024 values."""
        assert len(eqy_fund_2024cy_result.data) == 2
        aapl = _find(eqy_fund_2024cy_result.data, 'AAPL')
        ibm = _find(eqy_fund_2024cy_result.data, 'IBM')
        assert aapl['EBITDA'] == 136661.00
        assert ibm['EBITDA'] == 18238.00

    def test_best_fperiod_2025y_has_estimate_values(self, best_fperiod_2025y_result):
        """BEST_FPERIOD_OVERRIDE=2025Y override returns estimate values."""
        assert len(best_fperiod_2025y_result.data) == 2
        aapl = _find(best_fperiod_2025y_result.data, 'AAPL')
        ibm = _find(best_fperiod_2025y_result.data, 'IBM')
        # BEST fields return estimates (float with decimals)
        assert aapl['BEST_EBITDA'] == pytest.approx(143672.242)
        assert ibm['BEST_EBITDA'] == pytest.approx(18331.0)

    def test_override_values_differ_from_baseline(self, baseline_result, fund_per_q_result):
        """Override values are different from baseline values."""
        aapl_baseline = _find(baseline_result.data, 'AAPL')['EBITDA']
        aapl_quarterly = _find(fund_per_q_result.data, 'AAPL')['EBITDA']
        # Quarterly ~= Annual / 4 (roughly)
        assert aapl_quarterly < aapl_baseline / 2

    def test_merge_baseline_with_override_results(
        self, baseline_result, fund_per_q_result
    ):
        """Merging baseline with override results combines fields."""
        # Create copies to avoid fixture mutation
        merged = Result()
        merged.data = [dict(row) for row in baseline_result.data]
        merged.columns = list(baseline_result.columns)
        merged.errors = list(baseline_result.errors)

        override = Result()
        override.data = [dict(row) for row in fund_per_q_result.data]
        override.columns = list(fund_per_q_result.columns)
        override.errors = list(fund_per_q_result.errors)

        # Rename override EBITDA to EBITDA_Q for distinction
        for row in override.data:
            row['EBITDA_Q'] = row.pop('EBITDA')

        merged.merge(override)

        aapl = _find(merged.data, 'AAPL')
        assert aapl['EBITDA'] == 146848.00  # Annual from baseline
        assert aapl['EBITDA_Q'] == 35554.00  # Quarterly from override


class TestTransformFieldsForHistory:
    """Tests for _transform_fields_for_history() helper function."""

    def test_no_overrides_returns_fields_unchanged(self):
        """Fields returned unchanged when no overrides specified."""
        from bbdl.client import _transform_fields_for_history
        fields = ['SALES_REV_TURN', 'IS_OPER_INC', 'NET_INCOME']
        result = _transform_fields_for_history(fields, None)
        assert result == ['SALES_REV_TURN', 'IS_OPER_INC', 'NET_INCOME']

    def test_empty_overrides_returns_fields_unchanged(self):
        """Fields returned unchanged when empty overrides dict."""
        from bbdl.client import _transform_fields_for_history
        fields = ['SALES_REV_TURN', 'IS_OPER_INC', 'NET_INCOME']
        result = _transform_fields_for_history(fields, {})
        assert result == ['SALES_REV_TURN', 'IS_OPER_INC', 'NET_INCOME']

    def test_ae_override_transforms_field_name(self):
        """AE override embeds suffix in field name."""
        from bbdl.client import _transform_fields_for_history
        fields = ['SALES_REV_TURN', 'IS_EPS']
        overrides = {
            'SALES_REV_TURN': ('AE', 'E'),
            'IS_EPS': ('AE', 'E'),
        }
        result = _transform_fields_for_history(fields, overrides)
        assert result == ['SALES_REV_TURN|1|AE|E|', 'IS_EPS|1|AE|E|']

    def test_mixed_fields_with_and_without_overrides(self):
        """Fields without overrides remain unchanged."""
        from bbdl.client import _transform_fields_for_history
        fields = ['SALES_REV_TURN', 'PX_LAST', 'NET_INCOME']
        overrides = {'SALES_REV_TURN': ('AE', 'E')}
        result = _transform_fields_for_history(fields, overrides)
        assert result == ['SALES_REV_TURN|1|AE|E|', 'PX_LAST', 'NET_INCOME']

    def test_preserves_field_order(self):
        """Transformed fields maintain original order."""
        from bbdl.client import _transform_fields_for_history
        fields = ['A', 'B', 'C', 'D']
        overrides = {'B': ('AE', 'E'), 'D': ('L', '1')}
        result = _transform_fields_for_history(fields, overrides)
        assert result[0] == 'A'
        assert result[1] == 'B|1|AE|E|'
        assert result[2] == 'C'
        assert result[3] == 'D|1|L|1|'


class TestStripHistoryOverrideSuffix:
    """Tests for _strip_history_override_suffix() helper function."""

    def test_strips_ae_override_suffix(self):
        """AE override suffix stripped from field name."""
        from bbdl.request import _strip_history_override_suffix
        assert _strip_history_override_suffix('SALES_REV_TURN|1|AE|E|') == 'SALES_REV_TURN'
        assert _strip_history_override_suffix('IS_EPS|1|AE|E|') == 'IS_EPS'

    def test_strips_l_override_suffix(self):
        """L override suffix stripped from field name."""
        from bbdl.request import _strip_history_override_suffix
        assert _strip_history_override_suffix('PX_LAST|1|L|1|') == 'PX_LAST'

    def test_plain_field_unchanged(self):
        """Field without override suffix returned unchanged."""
        from bbdl.request import _strip_history_override_suffix
        assert _strip_history_override_suffix('SALES_REV_TURN') == 'SALES_REV_TURN'
        assert _strip_history_override_suffix('IS_EPS') == 'IS_EPS'

    def test_field_with_pipe_but_not_override(self):
        """Field with pipe but no |1| pattern returned unchanged."""
        from bbdl.request import _strip_history_override_suffix
        assert _strip_history_override_suffix('SOME|FIELD') == 'SOME|FIELD'


class TestHistoricalOverrideFixtures:
    """Tests using captured Bloomberg historical override fixtures."""

    @pytest.fixture(scope='class')
    def historical_actuals_result(self):
        """Parse historical actuals (no overrides) fixture."""
        with (FIXTURES_DIR / 'overrides_historical' / 'historical_actuals' / 'response.out').open() as f:
            return _parse(f)

    @pytest.fixture(scope='class')
    def historical_estimates_result(self):
        """Parse historical estimates (AE=E override) fixture."""
        with (FIXTURES_DIR / 'overrides_historical' / 'historical_estimates' / 'response.out').open() as f:
            return _parse(f)

    def test_historical_actuals_parses_correctly(self, historical_actuals_result):
        """Historical actuals fixture parses with clean field names."""
        assert len(historical_actuals_result.data) == 2

        aapl = _find(historical_actuals_result.data, 'AAPL')
        assert 'SALES_REV_TURN' in aapl
        assert 'IS_OPER_INC' in aapl
        assert 'NET_INCOME' in aapl
        assert 'IS_EPS' in aapl

        # Verify it's a time series
        assert isinstance(aapl['SALES_REV_TURN'], list)
        assert len(aapl['SALES_REV_TURN']) == 4

    def test_historical_estimates_parses_with_clean_field_names(self, historical_estimates_result):
        """Historical estimates fixture parses with override suffix stripped."""
        assert len(historical_estimates_result.data) == 2

        aapl = _find(historical_estimates_result.data, 'AAPL')
        # Field names should be clean, without the |1|AE|E| suffix
        assert 'SALES_REV_TURN' in aapl
        assert 'IS_OPER_INC' in aapl
        assert 'NET_INCOME' in aapl
        assert 'IS_EPS' in aapl

        # Should NOT have the raw override field names
        assert 'SALES_REV_TURN|1|AE|E|' not in aapl

    def test_historical_estimates_values_differ_from_actuals(
        self, historical_actuals_result, historical_estimates_result
    ):
        """Estimate values differ from actual values."""
        aapl_actuals = _find(historical_actuals_result.data, 'AAPL')
        aapl_estimates = _find(historical_estimates_result.data, 'AAPL')

        # Values should be different (estimates vs actuals)
        # Compare first quarter sales
        assert aapl_actuals['SALES_REV_TURN'][0] != aapl_estimates['SALES_REV_TURN'][0]

    def test_historical_actuals_values(self, historical_actuals_result):
        """Verify specific actual values from fixture."""
        ibm = _find(historical_actuals_result.data, 'IBM')
        # Q4 2024 actuals for IBM
        assert ibm['SALES_REV_TURN'][-1] == 17553000000.0
        assert ibm['IS_EPS'][-1] == 3.15

    def test_historical_estimates_values(self, historical_estimates_result):
        """Verify specific estimate values from fixture."""
        ibm = _find(historical_estimates_result.data, 'IBM')
        # Q4 2024 estimates for IBM (consensus)
        assert ibm['SALES_REV_TURN'][-1] == pytest.approx(17537333333.33, rel=0.01)
        assert ibm['IS_EPS'][-1] == pytest.approx(3.268, rel=0.01)


class TestOverrideChunkingIntegration:
    """Integration tests for override chunking pattern.

    When fields have different overrides, they must be grouped and sent
    as separate requests, then merged back into a single result.
    """

    def test_override_grouping_produces_separate_request_files(self):
        """Fields with different overrides generate separate request files."""
        from bbdl.client import _apply_overrides_to_identifiers
        from bbdl.client import _group_fields_by_overrides

        sids = ['AAPL US Equity', 'IBM US Equity']
        fields = ['PX_LAST', 'EBITDA', 'NET_DEBT', 'BEST_EBITDA']
        field_overrides = {
            'EBITDA': ('FUND_PER', 'Q'),
            'NET_DEBT': ('FUND_PER', 'Q'),
            'BEST_EBITDA': ('BEST_FPERIOD_OVERRIDE', '2025Y'),
        }

        groups = _group_fields_by_overrides(fields, field_overrides)

        # Should have 3 groups: no override, FUND_PER=Q, BEST_FPERIOD_OVERRIDE=2025Y
        assert len(groups) == 3
        assert () in groups
        assert ('FUND_PER', 'Q') in groups
        assert ('BEST_FPERIOD_OVERRIDE', '2025Y') in groups

        # Verify identifiers are transformed correctly for each group
        no_override_sids = _apply_overrides_to_identifiers(sids, ())
        assert no_override_sids == sids  # unchanged

        fund_per_sids = _apply_overrides_to_identifiers(sids, ('FUND_PER', 'Q'))
        assert fund_per_sids[0] == ('AAPL US Equity', '', 'FUND_PER', 'Q')
        assert fund_per_sids[1] == ('IBM US Equity', '', 'FUND_PER', 'Q')

    def test_request_build_with_override_identifiers(self):
        """Request.build correctly formats identifiers with overrides."""
        identifiers = [
            ('AAPL US Equity', '', 'FUND_PER', 'Q'),
            ('IBM US Equity', '', 'FUND_PER', 'Q'),
        ]
        fields = ['EBITDA', 'NET_DEBT']

        with make_tmpdir() as tmpdir:
            reqfile = Path(tmpdir) / 'test.req'
            options = BbdlOptions(programflag='adhoc')
            Request.build(identifiers, fields, reqfile, options)

            content = reqfile.read_text()
            # Verify override format: identifier||N|field|value
            assert 'AAPL US Equity||1|FUND_PER|Q' in content
            assert 'IBM US Equity||1|FUND_PER|Q' in content

    def test_merged_results_contain_all_fields(self):
        """Merging results from different override groups combines all fields."""
        # Simulate results from 3 separate requests
        result_no_override = Result()
        result_no_override.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01},
            {'IDENTIFIER': 'IBM US Equity', 'PX_LAST': 291.50},
        ]

        result_fund_per_q = Result()
        result_fund_per_q.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 35554.00, 'NET_DEBT': -60000.00},
            {'IDENTIFIER': 'IBM US Equity', 'EBITDA': 3853.00, 'NET_DEBT': 50000.00},
        ]

        result_best_fperiod = Result()
        result_best_fperiod.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'BEST_EBITDA': 143672.242},
            {'IDENTIFIER': 'IBM US Equity', 'BEST_EBITDA': 18331.0},
        ]

        # Merge all results
        result_no_override.merge(result_fund_per_q)
        result_no_override.merge(result_best_fperiod)

        # Verify merged result has all fields
        assert len(result_no_override.data) == 2

        aapl = _find(result_no_override.data, 'AAPL')
        assert aapl['PX_LAST'] == 271.01
        assert aapl['EBITDA'] == 35554.00
        assert aapl['NET_DEBT'] == -60000.00
        assert aapl['BEST_EBITDA'] == 143672.242

        ibm = _find(result_no_override.data, 'IBM')
        assert ibm['PX_LAST'] == 291.50
        assert ibm['EBITDA'] == 3853.00
        assert ibm['BEST_EBITDA'] == 18331.0

    def test_historical_override_fields_not_grouped(self):
        """Historical overrides embed in field names, no identifier grouping needed."""
        from bbdl.client import _transform_fields_for_history

        fields = ['SALES_REV_TURN', 'IS_EPS', 'PX_LAST']
        field_overrides = {
            'SALES_REV_TURN': ('AE', 'E'),
            'IS_EPS': ('AE', 'E'),
        }

        transformed = _transform_fields_for_history(fields, field_overrides)

        # All fields in single list with overrides embedded in names
        assert len(transformed) == 3
        assert transformed[0] == 'SALES_REV_TURN|1|AE|E|'
        assert transformed[1] == 'IS_EPS|1|AE|E|'
        assert transformed[2] == 'PX_LAST'  # No override, unchanged


class TestFetchByDateIntegration:
    """Integration tests for try_retrieve_existing_date / _fetch_by_date."""

    def test_parse_rundate_extracts_date(self):
        """_parse_rundate correctly extracts RUNDATE from response header."""
        from bbdl.client import _parse_rundate

        # Use actual fixture file
        filepath = FIXTURES_DIR / 'overrides' / 'baseline' / 'response.out'
        rundate = _parse_rundate(filepath)
        assert rundate == Date(2026, 1, 5)

    def test_parse_rundate_from_historical_fixture(self):
        """_parse_rundate works with historical response files."""
        from bbdl.client import _parse_rundate

        filepath = FIXTURES_DIR / 'overrides_historical' / 'historical_actuals' / 'response.out'
        rundate = _parse_rundate(filepath)
        assert rundate == Date(2026, 1, 5)

    def test_fetch_by_date_merges_multiple_files(self):
        """When multiple files match target date, results are merged."""
        # Create two result objects simulating two downloaded files
        result1 = Result()
        result1.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'PX_LAST': 271.01},
            {'IDENTIFIER': 'IBM US Equity', 'PX_LAST': 291.50},
        ]
        result1.columns = [('IDENTIFIER', str), ('PX_LAST', float)]

        result2 = Result()
        result2.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'EBITDA': 146848.00},
            {'IDENTIFIER': 'IBM US Equity', 'EBITDA': 18238.00},
        ]
        result2.columns = [('IDENTIFIER', str), ('EBITDA', float)]

        # Merge simulates what _fetch_by_date does
        result1.merge(result2)

        # Verify merged result
        assert len(result1.data) == 2
        aapl = _find(result1.data, 'AAPL')
        assert aapl['PX_LAST'] == 271.01
        assert aapl['EBITDA'] == 146848.00

    def test_multiple_fixture_files_can_be_merged(self):
        """Real fixture files can be merged by identifier."""
        # Parse multiple fixture files (simulating files from same date)
        with (FIXTURES_DIR / 'overrides' / 'baseline' / 'response.out').open() as f:
            baseline = _parse(f)

        with (FIXTURES_DIR / 'overrides' / 'fund_per_q' / 'response.out').open() as f:
            quarterly = _parse(f)

        # Rename EBITDA in quarterly to avoid collision
        for row in quarterly.data:
            if 'EBITDA' in row:
                row['EBITDA_Q'] = row.pop('EBITDA')

        baseline.merge(quarterly)

        # Both original and quarterly EBITDA available
        aapl = _find(baseline.data, 'AAPL')
        assert aapl['EBITDA'] == 146848.00  # Annual
        assert aapl['EBITDA_Q'] == 35554.00  # Quarterly


class TestFieldChunkingOver500:
    """Tests for chunking when fields exceed 500 limit."""

    def test_fields_chunked_at_500(self):
        """Fields are split into chunks of 500 for request building."""
        # Generate 600 field names
        fields = [f'FIELD_{i:03d}' for i in range(600)]

        nparts = (len(fields) - 1) // 500 + 1
        assert nparts == 2

        chunk1 = fields[0:500]
        chunk2 = fields[500:600]

        assert len(chunk1) == 500
        assert len(chunk2) == 100
        assert chunk1[0] == 'FIELD_000'
        assert chunk1[-1] == 'FIELD_499'
        assert chunk2[0] == 'FIELD_500'
        assert chunk2[-1] == 'FIELD_599'

    def test_chunked_results_merged_correctly(self):
        """Results from chunked requests merge by identifier."""
        # Simulate results from two chunks
        result1 = Result()
        result1.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'FIELD_000': 1, 'FIELD_001': 2},
            {'IDENTIFIER': 'IBM US Equity', 'FIELD_000': 10, 'FIELD_001': 20},
        ]

        result2 = Result()
        result2.data = [
            {'IDENTIFIER': 'AAPL US Equity', 'FIELD_500': 500, 'FIELD_501': 501},
            {'IDENTIFIER': 'IBM US Equity', 'FIELD_500': 5000, 'FIELD_501': 5010},
        ]

        result1.merge(result2)

        # All fields present in merged result
        aapl = _find(result1.data, 'AAPL')
        assert aapl['FIELD_000'] == 1
        assert aapl['FIELD_001'] == 2
        assert aapl['FIELD_500'] == 500
        assert aapl['FIELD_501'] == 501


if __name__ == '__main__':
    pytest.main([__file__])
