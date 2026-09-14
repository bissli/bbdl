"""Unit tests for bbdl.helper module."""

from pathlib import Path
from unittest.mock import patch

import pytest

from bbdl.exceptions import BbdlValidationError
from bbdl.helper import HEADERS, update_fields_asset


def _write_source(directory, header, rows):
    """Write a fake Bloomberg fields.csv and return its path."""
    path = Path(directory) / 'fields.csv'
    lines = [','.join(header), *(','.join(r) for r in rows)]
    path.write_text('\n'.join(lines) + '\n')
    return path


class TestUpdateFieldsAsset:
    """Tests for update_fields_asset()."""

    def test_columns_are_emitted_in_headers_order(self, tmp_path):
        """Verify output columns follow HEADERS, not the source file's order.

        Mutation: selecting by source order - `[v for k, v in
            this_row.items() if k in HEADERS_LOOKUP]` - which writes the
            values under a HEADERS-ordered header line, so every
            downstream lookup of 'Field Type' reads another column.
        Oracle: a source file whose columns are deliberately shuffled
            and padded with two extra columns; the expected output row
            is hand-written in HEADERS order.
        """
        shuffled = ['Field Type', 'Extra One', 'Field Mnemonic', 'Curncy',
                    'Data License Category', 'Comdty', 'Equity', 'Muni',
                    'Pfd', 'M-Mkt', 'Govt', 'Corp', 'Index', 'Mtge',
                    'Extra Two']
        row = ['Real', 'junk', 'PX_LAST', 'Curncy', 'Pricing - Intraday',
               'Comdty', 'Equity', 'Muni', 'Pfd', 'M-Mkt', 'Govt', 'Corp',
               'Index', 'Mtge', 'more junk']
        source = _write_source(tmp_path, shuffled, [row])
        output = tmp_path / 'out.csv'

        with patch('bbdl.helper.get_assets_path', return_value=output):
            update_fields_asset(str(source))

        written = output.read_text().splitlines()
        assert written[0] == ','.join(HEADERS)
        assert written[1] == ('PX_LAST,Pricing - Intraday,Comdty,Equity,Muni,'
                              'Pfd,M-Mkt,Govt,Corp,Index,Curncy,Mtge,Real')

    def test_missing_source_column_becomes_empty(self, tmp_path):
        """Verify a HEADERS column absent from the source is written empty.

        Mutation: indexing this_row[k] instead of .get(k, ''), which
            raises KeyError and aborts the whole regeneration on a
            single dropped Bloomberg column.
        Oracle: a source file with no 'Field Type' column; the expected
            row ends in a trailing comma.
        """
        header = [h for h in HEADERS if h != 'Field Type']
        row = ['PX_LAST', 'Pricing - Intraday', '', '', '', '', '', '', '',
               '', '', '']
        source = _write_source(tmp_path, header, [row])
        output = tmp_path / 'out.csv'

        with patch('bbdl.helper.get_assets_path', return_value=output):
            update_fields_asset(str(source))

        assert output.read_text().splitlines()[1].endswith(',')

    def test_values_are_stripped(self, tmp_path):
        """Verify surrounding whitespace is removed from every value.

        Mutation: dropping the .strip() in the comprehension, which
            leaves ' Real' in the Field Type column so every to_type
            lookup falls through to the trailing raise.
        Oracle: a padded source value and its unpadded expectation.
        """
        row = ['  PX_LAST  ', ' Pricing - Intraday ', '', '', '', '', '',
               '', '', '', '', '', '  Real  ']
        source = _write_source(tmp_path, HEADERS, [row])
        output = tmp_path / 'out.csv'

        with patch('bbdl.helper.get_assets_path', return_value=output):
            update_fields_asset(str(source))

        written = output.read_text().splitlines()[1]
        assert written.startswith('PX_LAST,Pricing - Intraday,')
        assert written.endswith(',Real')

    def test_wrong_filename_is_rejected(self, tmp_path):
        """Verify a source not named fields.csv is refused.

        Mutation: inverting the name guard, which would accept any file
            and overwrite the packaged asset from the wrong source.
        Oracle: a file named otherfields.csv.
        """
        wrong = tmp_path / 'otherfields.csv'
        wrong.write_text('x\n')

        with pytest.raises(BbdlValidationError, match="Expected 'fields.csv'"):
            update_fields_asset(str(wrong))

    def test_missing_source_file_is_rejected(self, tmp_path):
        """Verify a nonexistent source is refused before the asset is unlinked.

        Mutation: inverting the exists guard, or moving it after
            output.unlink(), which deletes the packaged asset and then
            fails, leaving the package without its field table.
        Oracle: a path that was never created.
        """
        with pytest.raises(BbdlValidationError, match='does not exist'):
            update_fields_asset(str(tmp_path / 'fields.csv'))
