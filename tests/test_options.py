"""Unit tests for bbdl.options module."""

from unittest.mock import patch

import pytest

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


class TestIsTerminal:
    """Test is_terminal function."""

    def test_with_sn(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(sn='123', ws='456')
        assert is_terminal(options)  # truthy

    def test_with_ws(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(sn='123', ws='456')
        assert is_terminal(options)  # truthy

    def test_with_usernumber(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions(usernumber='12345', is_bba=True)
        assert is_terminal(options)  # truthy

    def test_without_terminal(self):
        with patch('bbdl.options.get_tempdir') as mock_tempdir:
            mock_tempdir.return_value.dir = '/tmp'
            options = BbdlOptions()
        assert not is_terminal(options)  # falsy


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
