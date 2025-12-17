"""Unit tests for bbdl.parser module."""

import pytest

from bbdl.parser import Field, Ticker, _is_null, to_date, to_datetime, to_time
from date import Date


class TestIsNull:
    """Test _is_null function."""

    def test_empty_string(self):
        assert _is_null('') is True

    def test_none(self):
        assert _is_null(None) is True

    def test_na_values(self):
        assert _is_null('N.A.') is True
        assert _is_null('N.D.') is True
        assert _is_null('N.S.') is True
        assert _is_null('NaN') is True
        assert _is_null('None') is True

    def test_whitespace_na(self):
        assert _is_null('  N.A.  ') is True

    def test_valid_value(self):
        assert _is_null('123') is False
        assert _is_null('hello') is False

    def test_empty_list(self):
        assert _is_null([]) is True

    def test_list_of_nulls(self):
        assert _is_null([None, None, None]) is True
        assert _is_null(['N.A.', 'N.D.']) is True

    def test_list_with_valid_value(self):
        assert _is_null([None, 'value']) is False
        assert _is_null(['N.A.', 'valid']) is False

    def test_list_with_all_valid(self):
        assert _is_null(['a', 'b', 'c']) is False

    def test_zero_is_not_null(self):
        assert _is_null(0) is False
        assert _is_null(0.0) is False

    def test_single_element_list(self):
        assert _is_null([None]) is True
        assert _is_null(['value']) is False


class TestToDate:
    """Test to_date function."""

    def test_valid_date(self):
        result = to_date('2024-12-01')
        assert result == Date(2024, 12, 1)

    def test_null_value(self):
        assert to_date('') is None
        assert to_date('N.A.') is None

    def test_with_format(self):
        result = to_date('12/24', fmt='%m/%y')
        assert result.month == 12
        assert result.year == 2024


class TestToDatetime:
    """Test to_datetime function."""

    def test_null_value(self):
        assert to_datetime('') is None
        assert to_datetime('N.A.') is None


class TestToTime:
    """Test to_time function."""

    def test_null_value(self):
        assert to_time('') is None
        assert to_time('N.A.') is None


class TestFieldToType:
    """Test Field.to_type method."""

    def test_known_field(self):
        assert Field.to_type('PX_LAST') == float

    def test_unknown_field(self):
        assert Field.to_type('UNKNOWN_FIELD_XYZ') == object

    def test_boolean_field(self):
        assert Field.to_type('144A_FLAG') == bool

    def test_date_field(self):
        assert Field.to_type('MATURITY') == Date


class TestFieldToPython:
    """Test Field.to_python method."""

    def test_number_field(self):
        result = Field.to_python('PX_LAST', '123.45')
        assert result == 123.45

    def test_date_field(self):
        result = Field.to_python('MATURITY', '12/1/24')
        assert isinstance(result, Date)

    def test_unknown_field(self):
        with pytest.raises(ValueError, match='Unknown field'):
            Field.to_python('UNKNOWN_FIELD_XYZ', 'value')


class TestFieldToNumber:
    """Test Field._to_number method."""

    def test_integer(self):
        assert Field._to_number('123') == 123

    def test_float(self):
        assert Field._to_number('123.45') == 123.45

    def test_null(self):
        assert Field._to_number('') is None
        assert Field._to_number('N.A.') is None

    def test_comma_separated(self):
        result = Field._to_number('1,234.56')
        assert result == 1234.56


class TestFieldToStr:
    """Test Field._to_str method."""

    def test_valid_string(self):
        assert Field._to_str('hello') == 'hello'

    def test_strips_whitespace(self):
        assert Field._to_str('  hello  ') == 'hello'

    def test_null(self):
        assert Field._to_str('') is None
        assert Field._to_str('N.A.') is None


class TestFieldToBool:
    """Test Field._to_bool method."""

    def test_true_values(self):
        assert Field._to_bool('Y') is True
        assert Field._to_bool('Yes') is True
        assert Field._to_bool('T') is True
        assert Field._to_bool('True') is True
        assert Field._to_bool('1') is True

    def test_false_values(self):
        assert Field._to_bool('N') is False
        assert Field._to_bool('No') is False
        assert Field._to_bool('F') is False
        assert Field._to_bool('False') is False
        assert Field._to_bool('0') is False
        assert Field._to_bool('') is False


class TestFieldToList:
    """Test Field._to_list method for bulk fields."""

    def test_one_dimension(self):
        # ;1;2;5;11/01/2007;5;11/01/2008;
        s = ';1;2;5;11/01/2007;5;11/01/2008;'
        result = Field._to_list(s)
        assert len(result) == 2
        assert result[0] == Date(2007, 11, 1)
        assert result[1] == Date(2008, 11, 1)

    def test_null(self):
        assert Field._to_list('') is None
        assert Field._to_list(None) is None

    def test_invalid_format(self):
        assert Field._to_list('not a bulk field') is None


class TestFieldToCountryCode:
    """Test Field._to_country_code method."""

    def test_valid_country_code(self):
        assert Field._to_country_code('US') == 'US'
        assert Field._to_country_code('GB') == 'GB'

    def test_null_values(self):
        assert Field._to_country_code(None) is None
        assert Field._to_country_code('') is None
        assert Field._to_country_code('N.A.') is None

    def test_removes_noise(self):
        # Bloomberg sometimes adds extra characters
        assert Field._to_country_code('US1') == 'US'
        assert Field._to_country_code('GB/') == 'GB'

    def test_list_takes_first_valid(self):
        assert Field._to_country_code([None, 'US', 'GB']) == 'US'
        assert Field._to_country_code(['N.A.', None, 'CA']) == 'CA'

    def test_list_all_null(self):
        assert Field._to_country_code([None, None, None]) is None
        assert Field._to_country_code(['N.A.', 'N.D.']) is None

    def test_non_string_returns_none(self):
        assert Field._to_country_code(123) is None
        assert Field._to_country_code(12.34) is None

    def test_empty_after_cleanup(self):
        # If only noise characters, return None
        assert Field._to_country_code('123') is None


class TestTickerFixCase:
    """Test Ticker.fix_case method."""

    def test_normal_ticker(self):
        assert Ticker.fix_case('ibm us equity') == 'IBM US Equity'

    def test_already_correct(self):
        assert Ticker.fix_case('IBM US Equity') == 'IBM US Equity'

    def test_mixed_case(self):
        assert Ticker.fix_case('01234abc89 Us EQUITY') == '01234ABC89 US Equity'

    def test_single_word(self):
        assert Ticker.fix_case('cusip123') == 'CUSIP123'

    def test_none(self):
        assert Ticker.fix_case(None) is None

    def test_empty_string(self):
        assert Ticker.fix_case('') is None


class TestTickerIsBbTicker:
    """Test Ticker.is_bb_ticker method."""

    def test_equity_ticker(self):
        assert Ticker.is_bb_ticker('IBM US Equity') is True
        assert Ticker.is_bb_ticker('AAPL US Equity') is True

    def test_commodity_ticker(self):
        assert Ticker.is_bb_ticker('TYZ0 Comdty') is True

    def test_corp_ticker(self):
        assert Ticker.is_bb_ticker('88160RAG6 Corp') is True

    def test_invalid_yellow_key(self):
        assert Ticker.is_bb_ticker('IBM US Invalid') is False

    def test_single_word(self):
        assert Ticker.is_bb_ticker('cusip123') is False

    def test_none(self):
        assert Ticker.is_bb_ticker(None) is False

    def test_empty_string(self):
        assert Ticker.is_bb_ticker('') is False

    def test_all_yellow_keys(self):
        """Test all valid yellow keys."""
        for key in ('Comdty', 'Equity', 'Muni', 'Pfd', 'M-Mkt',
                    'Govt', 'Corp', 'Index', 'Curncy', 'Mtge'):
            assert Ticker.is_bb_ticker(f'TEST {key}') is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
