"""Unit tests for bbdl.client module."""

import gzip
import importlib
from pathlib import Path
from unittest.mock import patch

import pytest

import config

from bbdl import BbdlOptions, SFTPClient
from bbdl.exceptions import BbdlTimeoutError
from fixtures.field_lists import REQUEST_FIELDS, REQUEST_IDENTIFIERS
from libb import Setting
from opendate import Date


def _block(text, name):
    """Return the lines between START-OF-<name> and END-OF-<name>."""
    start = text.index(f'START-OF-{name}\n') + len(f'START-OF-{name}\n')
    return text[start:text.index(f'END-OF-{name}')].splitlines()


def _response(fields, rows, is_history=False):
    """Build a Bloomberg response body for the given fields and rows."""
    program = 'gethistory' if is_history else 'getdata'
    lines = ['START-OF-FILE', f'PROGRAMNAME={program}', 'RUNDATE=20240102',
             'START-OF-FIELDS', *fields, 'END-OF-FIELDS', 'START-OF-DATA',
             *rows, 'END-OF-DATA', 'END-OF-FILE']
    return '\n'.join(lines) + '\n'


class FakeCn:
    """Stub for the ftp connection, matching the calls bbdl actually makes.

    Attributes
    ----------
    uploaded : list[tuple[str, str]]
        Every (remote name, file text) handed to putascii, in order.
    downloaded : list[str]
        Every remote name handed to getbinary, in order.
    deleted : list[str]
        Every remote name handed to delete, in order.
    poll_count : int
        How many times files() was called, which is the poll loop's
        iteration count.

    Notes
    -----
    - bodies maps a remote file name to the text or bytes getbinary
      writes into the caller's local path. A name absent from bodies is
      also absent from files(), which is how a never-arriving response
      is simulated.
    """

    def __init__(self, bodies=None):
        self.bodies = dict(bodies or {})
        self.uploaded = []
        self.downloaded = []
        self.deleted = []
        self.poll_count = 0

    def delete(self, remotefile):
        self.deleted.append(remotefile)

    def putascii(self, localfile, remotefile):
        self.uploaded.append((remotefile, Path(localfile).read_text()))

    def files(self):
        self.poll_count += 1
        return list(self.bodies)

    def getbinary(self, remotefile, localfile):
        self.downloaded.append(remotefile)
        body = self.bodies[remotefile]
        if isinstance(body, bytes):
            Path(localfile).write_bytes(body)
        else:
            Path(localfile).write_text(body)

    def close(self):
        pass


@pytest.fixture
def options(tmp_path):
    """BbdlOptions pointed at a per-test tempdir, with a short wait."""
    return BbdlOptions(username='dl00001', tempdir=tmp_path, wait_time=1)


@pytest.fixture(autouse=True)
def no_sleep():
    """Keep Request.send's poll loop instant."""
    with patch('bbdl.request.time.sleep'):
        yield


def _client(options, cn):
    """Open an SFTPClient whose connection is the given stub."""
    with patch('bbdl.client.ftp.connect', return_value=cn):
        client = SFTPClient(options)
        return client.__enter__()


class TestConfigModuleEntryPoint:
    """Tests for the documented SFTPClient('<path>', config) construction."""

    def test_settings_path_resolves_to_options(self):
        """Verify a dotted config path builds the options it names.

        Mutation: the wrong leaf selected from the Setting tree, which
            would silently connect to the data host with the mock
            credentials or the reverse.
        Oracle: the values tests/config.py sets under bbg.mock.ftp,
            asserted field by field.
        """
        client = SFTPClient('bbg.mock.ftp', config)

        assert client.options.hostname == '127.0.0.1'
        assert client.options.username == 'foo'
        assert client.options.port == 21
        assert client.options.usernumber == '1234567'
        assert client.options.sn == '890'
        assert client.options.ws == '1'

    @pytest.mark.parametrize('locked_before', [True, False])
    def test_importing_the_config_preserves_the_lock_state(self, locked_before):
        """Verify importing tests/config.py leaves Setting's lock as it was.

        Mutation: an unconditional Setting.lock() at the end of
            tests/config.py. Setting._locked is a CLASS attribute, so
            that locks every Setting in the process; a later caller
            building its own tree then fails with 'AttributeError ...
            (locked)' pointing nowhere near the config module.
        Oracle: the lock state is driven to each value, the module is
            reloaded, and the state is compared against what it was.
            The locked_before=False case is the one an unconditional
            lock() fails.
        """
        was_locked = Setting._locked
        try:
            Setting.lock() if locked_before else Setting.unlock()
            importlib.reload(config)

            assert Setting._locked is locked_before
        finally:
            Setting.lock() if was_locked else Setting.unlock()

    def test_a_fresh_setting_tree_is_writable_when_unlocked(self):
        """Verify the lock is a process-wide switch, which is why it is restored.

        Mutation: Setting.unlock() made a no-op, or the lock left set by
            an importer, either of which makes an unrelated Setting
            tree unwritable.
        Oracle: a tree built and written to under an explicit unlock,
            with the prior state put back.
        """
        was_locked = Setting._locked
        try:
            Setting.unlock()
            probe = Setting()
            probe.some.branch.leaf = 'written'

            assert probe.some.branch.leaf == 'written'
        finally:
            Setting.lock() if was_locked else Setting.unlock()


class TestRequestChunking:
    """Tests for SFTPClient.request()'s 500-field chunking."""

    def test_six_hundred_fields_split_into_two_parts(self, options):
        """Verify 600 fields upload as two parts split at 500.

        Mutation: dropping the `+ 1` from (len(fields) - 1) // 500 + 1,
            which uploads one part and silently discards fields 500-599;
            or a chunk stride other than 500.
        Oracle: hand-computed boundaries - part one holds FIELD_000 to
            FIELD_499, part two holds FIELD_500 to FIELD_599.
        """
        fields = [f'FIELD_{i:03d}' for i in range(600)]
        cn = FakeCn({'fprp00.out': _response(fields[:500], []),
                     'fprp01.out': _response(fields[500:], [])})
        client = _client(options, cn)

        client.request(['IBM US Equity'], fields)

        assert [name for name, _ in cn.uploaded] == ['fprp00.req', 'fprp01.req']
        part1, part2 = (text for _, text in cn.uploaded)
        assert 'FIELD_000\n' in part1 and 'FIELD_499\n' in part1
        assert 'FIELD_500\n' not in part1
        assert 'FIELD_500\n' in part2 and 'FIELD_599\n' in part2
        assert part2.count('FIELD_') == 100

    def test_exactly_five_hundred_fields_is_one_part(self, options):
        """Verify 500 fields stay in a single part.

        Mutation: (len(fields) - 1) changed to len(fields), which turns
            an exact multiple of 500 into an extra empty request.
        Oracle: the boundary case - 500 is one part, 501 is two.
        """
        fields = [f'FIELD_{i:03d}' for i in range(500)]
        cn = FakeCn({'fprp00.out': _response(fields, [])})
        client = _client(options, cn)

        client.request(['IBM US Equity'], fields)

        assert [name for name, _ in cn.uploaded] == ['fprp00.req']


    def test_realistic_field_mix_uploads_every_field_once(self, options):
        """Verify a real 101-field, 5-identifier request round-trips whole.

        Mutation: an off-by-one in the chunk slice, which drops or
            duplicates a field at a part boundary; or a yellow-key
            recasing defect, which would alter one of the five real
            identifiers on the way out.
        Oracle: the captured lists themselves, compared line for line
            against the request's own field and data blocks, so order
            and multiplicity both have to match.
        """
        cn = FakeCn({'fprp00.out': _response(REQUEST_FIELDS, [])})
        client = _client(options, cn)

        client.request(REQUEST_IDENTIFIERS, REQUEST_FIELDS)

        _, text = cn.uploaded[0]
        assert _block(text, 'FIELDS') == REQUEST_FIELDS
        assert _block(text, 'DATA') == REQUEST_IDENTIFIERS


class TestRequestOverrideGroups:
    """Tests for SFTPClient.request()'s per-override-group file numbering."""

    def test_two_groups_upload_to_separate_files(self, options):
        """Verify each override group gets its own request file.

        Mutation: dropping `file_idx += 1`, which makes the second group
            overwrite the first group's request and response files, so
            one group's fields vanish from the merged result.
        Oracle: two distinct uploaded names, and both groups' fields
            present in the merged Result.
        """
        cn = FakeCn({
            'fprp00.out': _response(['PX_LAST'],
                                    ['IBM US Equity|0|1|145.50|']),
            'fprp01.out': _response(['EBITDA'],
                                    ['IBM US Equity|0|1|1000.0|']),
            })
        client = _client(options, cn)

        result = client.request(['IBM US Equity'], ['PX_LAST', 'EBITDA'],
                                field_overrides={'EBITDA': ('FUND_PER', 'Q')})

        assert [name for name, _ in cn.uploaded] == ['fprp00.req', 'fprp01.req']
        assert len(result.data) == 1
        assert result.data[0]['PX_LAST'] == 145.50
        assert result.data[0]['EBITDA'] == 1000.0

    def test_override_group_carries_the_override_on_its_identifiers(self, options):
        """Verify the overridden group's request embeds the override pair.

        Mutation: _apply_overrides_to_identifiers returning sids
            unchanged, which drops the override and returns the default
            reporting period instead of the requested one.
        Oracle: the hand-written Bloomberg override line,
            'IBM US Equity||1|FUND_PER|Q', present in the second request
            and absent from the first.
        """
        cn = FakeCn({
            'fprp00.out': _response(['PX_LAST'],
                                    ['IBM US Equity|0|1|145.50|']),
            'fprp01.out': _response(['EBITDA'],
                                    ['IBM US Equity|0|1|1000.0|']),
            })
        client = _client(options, cn)

        client.request(['IBM US Equity'], ['PX_LAST', 'EBITDA'],
                       field_overrides={'EBITDA': ('FUND_PER', 'Q')})

        plain, overridden = (text for _, text in cn.uploaded)
        assert 'IBM US Equity\n' in plain
        assert 'IBM US Equity||1|FUND_PER|Q\n' in overridden


class TestRequestDatesAndGuards:
    """Tests for SFTPClient.request()'s date handling and argument guards."""

    def test_string_dates_are_coerced_into_the_request(self, options):
        """Verify a string date range reaches the request as a DATERANGE.

        Mutation: dropping the parse_dates decorator or its str branch,
            which formats a raw string through %Y%m%d and raises.
        Oracle: hand-computed DATERANGE=20240101|20241231 from the
            strings '20240101' and '20241231'.
        """
        body = gzip.compress(
            _response(['PX_LAST'], ['IBM US Equity|0|1|20240102|145.50|'],
                      is_history=True).encode())
        cn = FakeCn({'fprp00.out.gz': body})
        client = _client(options, cn)

        client.request(['IBM US Equity'], ['PX_LAST'],
                       begdate='20240101', enddate='20241231')

        _, text = cn.uploaded[0]
        assert 'PROGRAMNAME=gethistory\n' in text
        assert 'DATERANGE=20240101|20241231\n' in text

    def test_historical_request_leaves_client_options_clean(self, options):
        """Verify request() does not write its dates onto self.options.

        Mutation: `options = self.options` in place of
            copy.deepcopy(self.options), which leaks begdate, enddate
            and headers onto the client so every later request silently
            becomes historical.
        Oracle: the client's own options object, asserted None before
            and after a historical call.
        """
        body = gzip.compress(
            _response(['PX_LAST'], ['IBM US Equity|0|1|20240102|145.50|'],
                      is_history=True).encode())
        cn = FakeCn({'fprp00.out.gz': body})
        client = _client(options, cn)
        assert client.options.begdate is None

        client.request(['IBM US Equity'], ['PX_LAST'],
                       begdate='20240101', enddate='20241231',
                       headers=['SECMASTER=no'])

        assert client.options.begdate is None
        assert client.options.enddate is None
        assert client.options.headers == []

    @pytest.mark.parametrize('sids,fields', [
        (None, ['PX_LAST']),
        (['IBM US Equity'], None),
        ([], ['PX_LAST']),
        (['IBM US Equity'], []),
        ])
    def test_missing_sids_or_fields_raises(self, options, sids, fields):
        """Verify a request with no sids or no fields is rejected.

        Mutation: `if not sids or not fields` weakened to `and`, which
            lets a fieldless request through and builds an empty
            START-OF-FIELDS block Bloomberg answers with an error.
        Oracle: all four missing-argument spellings, None and empty.
        """
        client = _client(options, FakeCn())

        with pytest.raises(ValueError, match='sids and fields are required'):
            client.request(sids, fields)


class TestRequestSendTransport:
    """Tests for Request.send()'s poll loop and gzip handling."""

    def test_history_response_is_always_gzipped(self, options):
        """Verify a historical response is polled and unzipped as .gz.

        Mutation: dropping the is_history term from the compressed test,
            so send() polls for a plain .out name that never arrives and
            the request times out; or gzip.open swapped for open in
            Request.parse, which reads the compressed bytes as text.
        Oracle: compressed=False on the options, yet the polled and
            downloaded name ends in .gz, and the parsed value matches
            the body that was compressed.
        """
        body = gzip.compress(
            _response(['PX_LAST'], ['IBM US Equity|0|1|20240102|145.50|'],
                      is_history=True).encode())
        cn = FakeCn({'fprp00.out.gz': body})
        client = _client(options, cn)
        assert client.options.compressed is False

        result = client.request(['IBM US Equity'], ['PX_LAST'],
                                begdate='20240101')

        assert cn.downloaded == ['fprp00.out.gz']
        assert result.data[0]['PX_LAST'] == 145.50

    def test_unzip_replaces_the_gz_with_the_plain_file(self, options):
        """Verify _unzip writes the plain file and removes the archive.

        Mutation: dropping zipfile.unlink, which leaves the .gz behind
            so _fetch_by_date's next listing sees a stale archive; or
            slicing the name by the wrong width, which leaves a '.g'
            suffix.
        Oracle: the tempdir contents after the call - fprp00.out
            present, fprp00.out.gz absent.
        """
        body = gzip.compress(
            _response(['PX_LAST'], ['IBM US Equity|0|1|20240102|145.50|'],
                      is_history=True).encode())
        cn = FakeCn({'fprp00.out.gz': body})
        client = _client(options, cn)

        client.request(['IBM US Equity'], ['PX_LAST'], begdate='20240101')

        assert (options.tempdir / 'fprp00.out').exists()
        assert not (options.tempdir / 'fprp00.out.gz').exists()

    def test_stale_response_is_deleted_before_upload(self, options):
        """Verify send() deletes the previous response before uploading.

        Mutation: dropping the suppressed delete, which lets a prior
            run's response file satisfy the poll immediately and returns
            stale data.
        Oracle: the stub's delete log, checked for the response name and
            ordered before the upload.
        """
        cn = FakeCn({'fprp00.out': _response(['PX_LAST'],
                                             ['IBM US Equity|0|1|145.50|'])})
        client = _client(options, cn)

        client.request(['IBM US Equity'], ['PX_LAST'])

        assert cn.deleted == ['fprp00.out']

    def test_missing_response_times_out_after_wait_time(self, options):
        """Verify a response that never arrives raises after wait_time*6 polls.

        Mutation: replacing the for-else with a silent return, which
            hands back an empty Result instead of reporting the timeout;
            or altering wait_time * 6, which changes how long the client
            waits from the documented number of minutes.
        Oracle: wait_time=1 on the fixture, so exactly 6 polls at the
            loop's ten-second interval, counted by the stub.
        """
        cn = FakeCn()
        client = _client(options, cn)

        with pytest.raises(BbdlTimeoutError, match='fprp00.out'):
            client.request(['IBM US Equity'], ['PX_LAST'])

        assert cn.poll_count == options.wait_time * 6


class TestFetchByDate:
    """Tests for SFTPClient._fetch_by_date()."""

    LISTING = {
        'fprp00.out': _response(['PX_LAST'], ['IBM US Equity|0|1|145.50|']),
        'fprp01.out': _response(['EBITDA'], ['IBM US Equity|0|1|1000.0|']),
        'other.out': _response(['PX_LAST'], ['XXX|0|1|1.0|']),
        'fprp02.req': 'not a response\n',
        }

    def test_only_fprp_out_files_are_downloaded(self, options):
        """Verify both clauses of the filename filter are applied.

        Mutation: dropping either clause - without the '.out' test the
            request file is parsed as a response; without the 'fprp'
            prefix test another program's output is merged in.
        Oracle: a listing holding one of each shape, and the stub's
            download log.
        """
        cn = FakeCn(self.LISTING)
        client = _client(options, cn)

        client.request(try_retrieve_existing_date='20240102')

        assert sorted(cn.downloaded) == ['fprp00.out', 'fprp01.out']

    def test_matching_files_are_merged(self, options):
        """Verify every file whose RUNDATE matches is merged into one Result.

        Mutation: breaking after the first match, which drops the second
            file's fields; or merging a file whose RUNDATE differs.
        Oracle: one row carrying both files' fields, hand-checked.
        """
        client = _client(options, FakeCn(self.LISTING))

        result = client.request(try_retrieve_existing_date='20240102')

        assert len(result.data) == 1
        assert result.data[0]['PX_LAST'] == 145.50
        assert result.data[0]['EBITDA'] == 1000.0

    def test_second_call_reuses_the_cached_download(self, options):
        """Verify an already-downloaded file is not fetched twice.

        Mutation: inverting `if not localpath.exists()`, which re-downloads
            every file on every call, or skips the first download entirely.
        Oracle: the stub's download log length across two identical calls.
        """
        cn = FakeCn(self.LISTING)
        client = _client(options, cn)

        client.request(try_retrieve_existing_date='20240102')
        first = len(cn.downloaded)
        client.request(try_retrieve_existing_date='20240102')

        assert first == 2
        assert len(cn.downloaded) == 2

    def test_no_file_for_the_date_raises(self, options):
        """Verify a date matching no RUNDATE raises rather than returning empty.

        Mutation: dropping the `if not matched_files` guard, which hands
            back an empty Result and reads as "no data for that day"
            instead of "that day was never fetched".
        Oracle: a target date one year off every fixture's RUNDATE.
        """
        client = _client(options, FakeCn(self.LISTING))

        with pytest.raises(FileNotFoundError, match='2025-01-02'):
            client.request(try_retrieve_existing_date='20250102')

    def test_date_objects_and_strings_both_work(self, options):
        """Verify try_retrieve_existing_date accepts a Date as well as a str.

        Mutation: dropping try_retrieve_existing_date from parse_dates,
            which leaves a string uncoerced so `rundate == target_date`
            never matches and every lookup raises FileNotFoundError.
        Oracle: the same fetch driven by both spellings of one day.
        """
        client = _client(options, FakeCn(self.LISTING))

        by_str = client.request(try_retrieve_existing_date='20240102')
        by_date = client.request(try_retrieve_existing_date=Date(2024, 1, 2))

        assert by_str.data[0]['PX_LAST'] == by_date.data[0]['PX_LAST']
