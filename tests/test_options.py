"""Unit tests for bbdl.options module."""

import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from opendate import Date

from bbdl.exceptions import BbdlValidationError
from bbdl.options import BbdlOptions, is_terminal, terminal_bba, terminal_open


class TestBbdlOptions:
    """Test BbdlOptions dataclass."""

    def test_default_values(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()

        assert options.bval is False
        assert options.compressed is False
        assert options.dateformat == 'yyyymmdd'
        assert options.programflag == 'adhoc'
        assert options.delimiter == '|'
        assert options.wait_time == 20
        assert options.hostname == 'sftp.bloomberg.com'
        assert options.port == 22

    def test_programflag_adhoc(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(programflag='adhoc')
        assert options.programflag == 'adhoc'

    def test_programflag_oneshot(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(programflag='oneshot')
        assert options.programflag == 'oneshot'

    def test_invalid_programflag(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            with pytest.raises(BbdlValidationError, match='programflag'):
                BbdlOptions(programflag='invalid')


class TestBbdlOptionsDateCoercion:
    """Test BbdlOptions begdate/enddate coercion."""

    @pytest.mark.parametrize('supplied', [
        '20240102',
        '2024-01-02',
        datetime.date(2024, 1, 2),
        datetime.datetime(2024, 1, 2, 9, 30),
        Date(2024, 1, 2),
        ])
    def test_coerces_every_date_like_input(self, supplied):
        """Verify begdate and enddate accept str, date, datetime and Date.

        Mutation: Date(self.begdate) in place of the parse/instance split,
            which raises TypeError for every one of these inputs.
        Oracle: hand-computed Date(2024, 1, 2) from five spellings of the
            same calendar day.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(begdate=supplied, enddate=supplied)

        assert options.begdate == Date(2024, 1, 2)
        assert options.enddate == Date(2024, 1, 2)
        assert isinstance(options.begdate, Date)
        assert isinstance(options.enddate, Date)

    def test_unset_dates_stay_none(self):
        """Verify an unsupplied or falsy date coerces to None, not today.

        Mutation: dropping the `if not value` guard, so Date.instance(None)
            raises; or returning a default date for a falsy input.
        Oracle: both fields are None when neither is passed.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()

        assert options.begdate is None
        assert options.enddate is None

    def test_tempdir_defaults_to_a_path(self):
        """Verify tempdir falls back to get_tempdir() as a Path.

        Mutation: self.tempdir = None in place of the Path() coercion,
            which defers the failure to client.py's `options.tempdir /
            filename` and reports it far from the cause.
        Oracle: the patched get_tempdir value, compared as a Path.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp/bbdl-test'
            options = BbdlOptions()

        assert options.tempdir == Path('/tmp/bbdl-test')
        assert isinstance(options.tempdir, Path)

    def test_coerces_each_field_independently(self):
        """Verify the enddate arm runs, not just the begdate arm.

        Mutation: dropping the enddate coercion line, which leaves a
            supplied string unconverted while begdate still passes.
        Oracle: distinct hand-computed dates per field, so a copied or
            dropped arm shows as the wrong value rather than the wrong
            type.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(begdate='20240102', enddate='20241231')

        assert options.begdate == Date(2024, 1, 2)
        assert options.enddate == Date(2024, 12, 31)


class TestIsTerminal:
    """Test is_terminal function."""

    @pytest.mark.parametrize('attr', ['sn', 'ws', 'usernumber'])
    def test_each_field_alone_marks_a_terminal_link(self, attr):
        """Verify any one of sn, ws or usernumber makes is_terminal true.

        Mutation: `options.sn and options.ws or options.usernumber` in
            place of the three-way or, which stops sn alone or ws alone
            from counting as a terminal link.
        Oracle: one field set per case, the other two left None, so a
            dropped or conjoined term shows as a falsy return.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
        setattr(options, attr, '123')

        assert is_terminal(options)

    def test_without_terminal(self):
        """Verify no terminal field leaves is_terminal falsy.

        Mutation: returning a constant, or inverting the or-chain, which
            makes every plain options object look terminal-linked.
        Oracle: sn, ws and usernumber all None.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()

        assert options.sn is None
        assert options.ws is None
        assert options.usernumber is None
        assert not is_terminal(options)


class TestTerminalDispatch:
    """Test __post_init__'s choice between BBA and open terminal linking."""

    def test_bba_clears_sn_and_ws(self):
        """Verify is_bba routes to terminal_bba, which drops sn and ws.

        Mutation: `if self.is_bba` forced false, which routes a BBA link
            through terminal_open and leaves SN and WS in the request
            header - the harm options.py's terminal_bba docstring warns
            of; or terminal_bba(None), which raises instead of clearing.
        Oracle: sn and ws supplied and expected back as None, with
            usernumber preserved.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(sn='123', ws='456', usernumber='789',
                                  is_bba=True)

        assert options.sn is None
        assert options.ws is None
        assert options.usernumber == '789'

    def test_open_terminal_keeps_sn_and_ws(self):
        """Verify a non-BBA link routes to terminal_open and keeps sn/ws.

        Mutation: `if self.is_bba` forced true, which clears SN and WS on
            an open terminal and breaks the link; or terminal_open(None),
            which raises instead of validating.
        Oracle: sn and ws supplied and expected back unchanged.
        """
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(sn='123', ws='456')

        assert options.sn == '123'
        assert options.ws == '456'


class TestTerminalOpen:
    """Test terminal_open function."""

    def test_missing_sn(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
            options.ws = '456'
            with pytest.raises(BbdlValidationError, match='SN must be provided'):
                terminal_open(options)

    def test_missing_ws(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
            options.sn = '123'
            with pytest.raises(BbdlValidationError, match='WS must be provided'):
                terminal_open(options)

    def test_valid_terminal(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
            options.sn = '123'
            options.ws = '456'
            # Should not raise
            terminal_open(options)


class TestTerminalBba:
    """Test terminal_bba function."""

    def test_missing_usernumber(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
            with pytest.raises(BbdlValidationError, match='Usernumber must be provided'):
                terminal_bba(options)

    def test_valid_bba(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
            options.usernumber = '12345'
            # Should not raise
            terminal_bba(options)
            # Should clear sn and ws
            assert options.sn is None
            assert options.ws is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
