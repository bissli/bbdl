"""Unit tests for bbdl.request module."""

import io
from pathlib import Path

import pytest
from asserts import assert_equal

from bbdl import BbdlOptions, Request, Result
from bbdl.request import _parse
from date import Date
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


if __name__ == '__main__':
    pytest.main([__file__])
