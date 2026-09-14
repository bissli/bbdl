"""Unit tests for bbdl.parser module."""

from unittest.mock import patch

import numpy as np
import pytest

from bbdl.parser import Field, Ticker, _is_null, to_date, to_datetime, to_time
from opendate import Date, DateTime, Time


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


class TestFieldTypeDispatch:
    """Test Field.to_type/to_python across every Bloomberg field type."""

    # (mnemonic, field type, raw value, expected to_type, expected to_python)
    CASES = [
        ('FIXED', 'Boolean', 'Y', bool, True),
        ('MTG_OID', 'Bulk Format', ';1;1;5;01/01/2025;', list, [Date(2025, 1, 1)]),
        ('NAME', 'Character', '  IBM  ', str, 'IBM'),
        ('END_DT', 'Date', '12/1/24', Date, Date(2024, 12, 1)),
        ('BC_YEAR', 'Integer', '2024', int, 2024),
        ('MLI_OAS', 'Integer/Real', '12.5', float, 12.5),
        ('CLASS', 'Long Character', ' x ', str, 'x'),
        ('FUT_MONTH_YR', 'Month/Year', '12/24', Date, Date(2024, 12, 1)),
        ('ASK', 'Price', '145.50', float, 145.5),
        ('AMT', 'Real', '3.375', float, 3.375),
        ]

    @pytest.mark.parametrize('mnemonic,ftype,raw,want_type,want_value', CASES,
                             ids=[c[1] for c in CASES])
    def test_each_field_type_converts(self, mnemonic, ftype, raw, want_type, want_value):
        """Verify to_type and to_python agree per Bloomberg field type.

        Mutation: any arm of either if-chain reassigned to the wrong
            converter, or a comparison inverted - `if ftype != 'Real'`
            makes every arm below it return float, so a Time field
            arrives as a number.
        Oracle: the mnemonic's own Field Type metadata, asserted, plus a
            hand-computed expected value per row.
        """
        assert Field.all_fields[mnemonic]['Field Type'] == ftype
        assert Field.to_type(mnemonic) == want_type
        assert Field.to_python(mnemonic, raw) == want_value

    def test_date_or_time_field(self):
        """Verify a 'Date or Time' mnemonic converts to DateTime.

        Mutation: the 'Date or Time' arm routed to to_date, which drops
            the time of day silently.
        Oracle: hand-computed 2024-12-31 14:30, compared component-wise
            so the parser's tzinfo does not mask a dropped time.
        """
        assert Field.all_fields['LAST_UPDATE']['Field Type'] == 'Date or Time'
        assert Field.to_type('LAST_UPDATE') is DateTime

        got = Field.to_python('LAST_UPDATE', '12/31/2024 14:30:00')
        assert isinstance(got, DateTime)
        assert (got.year, got.month, got.day) == (2024, 12, 31)
        assert (got.hour, got.minute) == (14, 30)

    def test_time_field(self):
        """Verify a 'Time' mnemonic converts to Time, not to a number.

        Mutation: `if ftype == 'Real'` inverted to `!=`, which catches
            'Time' first and returns 16.0 instead of Time(16, 0).
        Oracle: hand-computed 16:00:00, compared component-wise.
        """
        assert Field.all_fields['LOCAL_TIME']['Field Type'] == 'Time'
        assert Field.to_type('LOCAL_TIME') is Time

        got = Field.to_python('LOCAL_TIME', '16:00:00')
        assert isinstance(got, Time)
        assert (got.hour, got.minute, got.second) == (16, 0, 0)

    def test_unknown_field_type_raises(self):
        """Verify a mnemonic whose metadata type is unknown raises.

        Mutation: deleting the trailing `raise ValueError`, which returns
            None for an unrecognized type and hides a stale field table.
        Oracle: a patched metadata row carrying a type no arm handles.
        """
        rogue = dict(Field.all_fields['ASK'], **{'Field Type': 'Quaternion'})
        with patch.dict(Field.all_fields, {'ROGUE_FIELD': rogue}):
            with pytest.raises(ValueError, match='Unknown type'):
                Field.to_type('ROGUE_FIELD')
            with pytest.raises(ValueError, match='Unknown type'):
                Field.to_python('ROGUE_FIELD', '1')


class TestConvertBulkField:
    """Test Field._convert_bulk_field across every bulk type code."""

    def test_string_codes(self):
        """Verify codes 1, 4 and 11 strip and return the raw string.

        Mutation: any of the three dropped from the set literal, which
            sends it to the trailing raise; or routed to _to_number,
            which returns None for text.
        Oracle: hand-written ' abc ' -> 'abc' per code.
        """
        for code in (1, 4, 11):
            assert Field._convert_bulk_field(code, ' abc ') == 'abc'

    def test_number_codes(self):
        """Verify codes 2, 3, 12 and 13 parse as numbers.

        Mutation: any code dropped from a set literal or routed to
            _to_str, which returns '12.5' instead of 12.5.
        Oracle: hand-written '12.5' -> 12.5 per code, type asserted so a
            string does not compare equal by coercion.
        """
        for code in (2, 3, 12, 13):
            got = Field._convert_bulk_field(code, '12.5')
            assert got == 12.5
            assert isinstance(got, float)

    def test_date_code(self):
        """Verify code 5 parses a US-format date.

        Mutation: code 5 routed to to_datetime or to_time.
        Oracle: hand-computed 01/02/2025 -> Date(2025, 1, 2), which also
            pins month-before-day.
        """
        assert Field._convert_bulk_field(5, '01/02/2025') == Date(2025, 1, 2)

    def test_time_code(self):
        """Verify code 6 parses a time of day.

        Mutation: code 6 routed to to_date, which raises and yields None
            through the caller's except.
        Oracle: hand-computed 16:00:00, compared component-wise.
        """
        got = Field._convert_bulk_field(6, '16:00:00')
        assert isinstance(got, Time)
        assert (got.hour, got.minute, got.second) == (16, 0, 0)

    def test_datetime_code(self):
        """Verify code 7 parses a date and a time together.

        Mutation: code 7 routed to to_date, which silently drops the
            time of day.
        Oracle: hand-computed 2024-12-31 14:30, compared component-wise.
        """
        got = Field._convert_bulk_field(7, '12/31/2024 14:30:00')
        assert isinstance(got, DateTime)
        assert (got.year, got.month, got.day) == (2024, 12, 31)
        assert (got.hour, got.minute) == (14, 30)

    def test_month_year_code(self):
        """Verify code 9 parses month/year to the first of that month.

        Mutation: the fmt string changed from '%m/%y' to '%M/%Y', which
            reads 12 as a minute and raises; or code 9 routed to to_date,
            which misreads '12/24' as a day.
        Oracle: hand-computed '12/24' -> Date(2024, 12, 1).
        """
        assert Field._convert_bulk_field(9, '12/24') == Date(2024, 12, 1)

    def test_boolean_code(self):
        """Verify code 10 parses a Bloomberg boolean.

        Mutation: code 10 routed to _to_str, which returns 'Y' - truthy,
            so a caller testing `is True` breaks while `if value` does
            not.
        Oracle: 'Y' -> True and 'N' -> False, identity-compared.
        """
        assert Field._convert_bulk_field(10, 'Y') is True
        assert Field._convert_bulk_field(10, 'N') is False

    def test_unknown_code_raises(self):
        """Verify an unrecognized bulk type code raises.

        Mutation: deleting the trailing raise, which returns None for an
            unknown code and turns a protocol change into missing data.
        Oracle: code 99, outside the documented 1..13 range.
        """
        with pytest.raises(ValueError, match='Unexpected field type'):
            Field._convert_bulk_field(99, 'x')


class TestDateTimeConverters:
    """Test to_date/to_datetime/to_time beyond their null cases."""

    def test_to_date_parses_and_raises(self):
        """Verify to_date parses a real date and rejects garbage.

        Mutation: raise_err=True flipped to False, which turns an
            unparseable Bloomberg value into a silent None so a missing
            date is indistinguishable from a malformed one.
        Oracle: hand-computed Date(2024, 12, 1), and pytest.raises on a
            value no format matches.
        """
        assert to_date('12/1/24') == Date(2024, 12, 1)
        with pytest.raises(Exception):
            to_date('not-a-date')

    def test_to_date_honors_an_explicit_format(self):
        """Verify the fmt argument drives strptime rather than being ignored.

        Mutation: dropping the `if fmt` branch, which sends '12/24' to
            Date.parse and reads it as a day-month pair.
        Oracle: hand-computed '12/24' under '%m/%y' -> Date(2024, 12, 1).
        """
        assert to_date('12/24', fmt='%m/%y') == Date(2024, 12, 1)

    def test_to_datetime_parses_and_raises(self):
        """Verify to_datetime keeps the time of day and rejects garbage.

        Mutation: raise_err=True flipped to False; or DateTime.parse
            swapped for Date.parse, which drops the time.
        Oracle: hand-computed 2024-12-31 14:30, component-wise.
        """
        got = to_datetime('12/31/2024 14:30:00')
        assert (got.year, got.month, got.day) == (2024, 12, 31)
        assert (got.hour, got.minute) == (14, 30)
        with pytest.raises(Exception):
            to_datetime('not-a-datetime')

    def test_to_datetime_honors_an_explicit_format(self):
        """Verify the fmt argument drives strptime for to_datetime.

        Mutation: dropping the `if fmt` branch, which sends the value to
            DateTime.parse and misreads a day-first date as month-first;
            or swapping strptime's two arguments.
        Oracle: hand-computed 31-12-2024 14:30 under '%d-%m-%Y %H:%M',
            a day-first spelling DateTime.parse would read as month 31.
        """
        got = to_datetime('31-12-2024 14:30', fmt='%d-%m-%Y %H:%M')
        assert (got.year, got.month, got.day) == (2024, 12, 31)
        assert (got.hour, got.minute) == (14, 30)

    def test_to_time_honors_an_explicit_format(self):
        """Verify the fmt argument reaches Time.parse.

        Mutation: fmt=None passed through, or the fmt keyword dropped
            from the call, either of which fails to parse a 12-hour
            clock value and raises instead of returning 16:30.
        Oracle: hand-computed '04.30 PM' under '%I.%M %p' -> 16:30.
        """
        got = to_time('04.30 PM', fmt='%I.%M %p')
        assert (got.hour, got.minute) == (16, 30)

    def test_to_time_parses_and_raises(self):
        """Verify to_time parses a time of day and rejects garbage.

        Mutation: raise_err=True flipped to False, which hides a
            malformed time as a missing one.
        Oracle: hand-computed 16:00:00, component-wise.
        """
        got = to_time('16:00:00')
        assert (got.hour, got.minute, got.second) == (16, 0, 0)
        with pytest.raises(Exception):
            to_time('not-a-time')


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

    def test_nan_returns_none(self):
        """NaN from pandas should return None, not raise TypeError."""
        assert Ticker.fix_case(np.nan) is None

    def test_non_string_types(self):
        """Non-string types should return None."""
        assert Ticker.fix_case(123) is None
        assert Ticker.fix_case(12.34) is None
        assert Ticker.fix_case(['list']) is None


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

    def test_nan_returns_false(self):
        """NaN from pandas should return False, not raise TypeError."""
        assert Ticker.is_bb_ticker(np.nan) is False

    def test_non_string_types(self):
        """Non-string types should return False."""
        assert Ticker.is_bb_ticker(123) is False
        assert Ticker.is_bb_ticker(12.34) is False

    def test_all_yellow_keys(self):
        """Test all valid yellow keys."""
        for key in ('Comdty', 'Equity', 'Muni', 'Pfd', 'M-Mkt',
                    'Govt', 'Corp', 'Index', 'Curncy', 'Mtge'):
            assert Ticker.is_bb_ticker(f'TEST {key}') is True


class TestBulkFieldFormatting:
    """Test bulk field formatting with named keys."""

    def test_soft_call_schedule(self):
        """Verify soft_call_schedule returns dicts with named keys."""
        s = ';2;2;2;5;01/01/2025;3;100.5;5;02/01/2025;3;101.0;'
        result = Field.to_python('SOFT_CALL_SCHEDULE', s)
        assert isinstance(result, list)
        assert len(result) == 2
        assert isinstance(result[0], dict)
        assert 'Soft Call Date' in result[0]
        assert 'Soft Call Price' in result[0]
        assert result[0]['Soft Call Date'] == Date(2025, 1, 1)
        assert result[0]['Soft Call Price'] == 100.5

    def test_call_schedule(self):
        """Verify call_schedule returns dicts with Call Date and Call Price."""
        s = ';2;2;2;5;03/15/2026;3;102.5;5;03/15/2027;3;101.0;'
        result = Field.to_python('CALL_SCHEDULE', s)
        assert isinstance(result, list)
        assert len(result) == 2
        assert 'Call Date' in result[0]
        assert 'Call Price' in result[0]
        assert result[0]['Call Date'] == Date(2026, 3, 15)
        assert result[0]['Call Price'] == 102.5

    def test_put_schedule(self):
        """Verify put_schedule returns dicts with Put Date and Put Price."""
        s = ';2;1;2;5;06/01/2027;3;100.0;'
        result = Field.to_python('PUT_SCHEDULE', s)
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'Put Date' in result[0]
        assert 'Put Price' in result[0]

    def test_conversion_reset_schedule(self):
        """Verify conversion_reset_schedule returns dicts with named keys."""
        s = ';2;2;3;5;01/06/2022;3;80.0;3;0.0;5;01/16/2023;3;77.5;3;0.0;'
        result = Field.to_python('CONVERSION_RESET_SCHEDULE', s)
        assert isinstance(result, list)
        assert len(result) == 2
        assert 'Reset Date' in result[0]
        assert 'Conversion Price' in result[0]
        assert 'Floor' in result[0]
        assert result[0]['Reset Date'] == Date(2022, 1, 6)
        assert result[0]['Conversion Price'] == 80.0
        assert result[0]['Floor'] == 0.0

    def test_redemption_underlying(self):
        """Verify redemption_underlying returns dicts with Ticker and Type."""
        s = ';2;1;2;1;MSFT US;1;Equity;'
        result = Field.to_python('REDEMPTION_UNDERLYING', s)
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'Ticker' in result[0]
        assert 'Type' in result[0]
        assert result[0]['Ticker'] == 'MSFT US'
        assert result[0]['Type'] == 'Equity'

    def test_null_bulk_field(self):
        """Verify null bulk field returns None."""
        result = Field.to_python('SOFT_CALL_SCHEDULE', '')
        assert result is None

    def test_issue_underwriter(self):
        """Verify issue_underwriter returns dicts with all 8 fields."""
        s = ';2;1;8;1;Bookrunner;1;BofA Securities;1;BofA;1;JLMB;1;Joint Lead;3;0.0;2;1;5;06/17/2024;'
        result = Field.to_python('ISSUE_UNDERWRITER', s)
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'Role' in result[0]
        assert 'Firm' in result[0]
        assert 'Abbreviation' in result[0]
        assert result[0]['Role'] == 'Bookrunner'
        assert result[0]['Firm'] == 'BofA Securities'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
