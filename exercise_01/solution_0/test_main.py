import pytest

from main import Reader


@pytest.mark.parametrize("file_content,reads_count,skip_header,want,is_eof", [
    ("""foo,bar\n1,\"a\"\n""", 1, True, "1,\"a\"", False),
    ("""foo,bar\n1,\"a\"\n""", 1, False, "foo,bar", False),
    ("""foo,bar\n1,\"a\"\n""", 2, True, "", True),
    ("", 1, False, "", True),
])
class TestReader:
    def test_reader_single_row(self, mocker, reads_count, skip_header, file_content, want, is_eof):
        mocker.patch("builtins.open", mocker.mock_open(read_data=file_content))
        reader = Reader("foo.csv", skip_header)

        assert reader.header_skipped is not skip_header, "faulty header status"

        got: str = ""

        try:
            for _ in range(reads_count):
                got = next(reader)
            assert got == want
        except StopIteration:
            if is_eof:
                pass
