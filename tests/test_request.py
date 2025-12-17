import io
from pathlib import Path

import pytest
from asserts import assert_equal

from bbdl import BbdlOptions, Request, Result
from date import Date
from libb.dir import make_tmpdir


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
        # CALL_SCHEDULE is a bulk field type - should remain as list
        result.data = [
            {'CALL_SCHEDULE': [(Date(2025, 1, 1), 100.0), (Date(2026, 1, 1), 100.0)], 'single': [42]},
        ]
        result.unwrap_single_element_lists()
        assert len(result.data[0]['CALL_SCHEDULE']) == 2
        assert result.data[0]['single'] == 42

    def test_unwraps_multi_element_lists_for_scalar_fields(self):
        """Multi-element lists for scalar fields should take first non-null value"""
        result = Result()
        # COUNTRY_ISO is a scalar (Character) field - multi-element list should take first non-null
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
        # Values should be scalars, not lists
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
        from bbdl.request import _parse
        result = _parse(io.StringIO(response))
        assert len(result.data) == 2
        assert result.data[0]['IDENTIFIER'] == 'IBM US Equity'
        assert result.data[0]['ID_BB_GLOBAL'] == 'BBG000BLNNH6'
        assert result.data[0]['PX_LAST'] == 145.50
        # Non-historical: values should be scalars, not lists
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
        from bbdl.request import _parse
        result = _parse(io.StringIO(response))
        assert len(result.data) == 1
        # Historical: values are wrapped in lists
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
        from bbdl.request import _parse
        result = _parse(io.StringIO(response))
        # Should aggregate to single row per identifier
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
        from bbdl.request import _parse
        result = _parse(io.StringIO(response))
        df = result.to_dataframe()
        # After to_dataframe, single-element lists should be unwrapped
        assert df.iloc[0]['PX_LAST'] == 145.50
        assert df.iloc[0]['PX_BID'] == 145.25
        assert df.iloc[1]['PX_LAST'] == 175.25
        # Verify they're not lists
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
        from bbdl.request import _parse
        result = _parse(io.StringIO(response))
        assert len(result.data) == 0
        assert len(result.errors) == 1
        assert result.errors[0]['RETCODE'] == '10'


def test_basic_request():
    identifiers = ['IBM US Equity', '88160RAG6 Corp']
    fields = ['ID_BB_GLOBAL', 'PARSEKYABLE_DES', 'PX_LAST']

    with make_tmpdir() as tmpdir:
        reqfile = Path(tmpdir) / 'reqfile.out'
        options = BbdlOptions(programflag='adhoc')
        Request.build(identifiers, fields, reqfile, options)
        with Path(reqfile).open('r') as f:
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
