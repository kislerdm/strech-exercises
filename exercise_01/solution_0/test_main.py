from typing import Optional
from uuid import UUID

import pytest

from main import CSVReader, read_active_users, DataQualityError


@pytest.mark.parametrize("file_content,reads_count,skip_header,want,is_eof", [
    ("""foo,bar\n1,a\n""", 1, True, ["1", "a"], False),
    ("""foo,bar\n1,a\n""", 1, False, ["foo", "bar"], False),
    ("""foo,bar\n1,a\n""", 2, True, None, True),
    ("", 1, False, "", True),
])
class TestReader:
    def test_reader_single_row(self, mocker, reads_count, skip_header, file_content, want, is_eof):
        mocker.patch("builtins.open", mocker.mock_open(read_data=file_content))
        reader = CSVReader("foo.csv", skip_header)

        assert reader.header_skipped is not skip_header, "faulty header status"

        got: Optional[list[str]] = None

        try:
            for _ in range(reads_count):
                got = next(reader)
            assert got == want
        except StopIteration:
            if is_eof:
                pass


@pytest.mark.parametrize("file_content,want,is_error,error_msg", [
    (
            """user_id,is_active
    9f709688-326d-4834-8075-1a477d590af7,1
    999eb541-c1a0-4888-aeb6-92773fc60e69,0
    b923d15c-ce6d-4b2f-913f-31e87ebbcdc2,false
    b1ee6da9-aca5-4bc6-bcfb-21ace2185055,true
    """,
            {UUID("9f709688-326d-4834-8075-1a477d590af7"), UUID("b1ee6da9-aca5-4bc6-bcfb-21ace2185055")},
            False,
            "",
    ),
    (
            """user_id,is_active
    9f709688-326d-4834-8075-1a477d590af7,0
    999eb541-c1a0-4888-aeb6-92773fc60e69,0
    b923d15c-ce6d-4b2f-913f-31e87ebbcdc2,false
    b1ee6da9-aca5-4bc6-bcfb-21ace2185055,false
    """,
            {},
            False,
            "",
    ),
    (
            """user_id,is_active
    9f709688-326d-4834-8075-1a477d590af7
    """,
            {},
            True,
            "wrong number of columns in row 1",
    ),
    (
            """user_id,is_active
    9f709688-326d-4834-8075-1a477d590af7,1
    1,1
    """,
            {},
            True,
            "failed to decode user_id in row 2",
    ),
])
def test_read_active_users(mocker, file_content, want, is_error, error_msg):
    mocker.patch("builtins.open", mocker.mock_open(read_data=file_content))

    reader = CSVReader("foo.csv", skip_header=True)

    try:
        got = read_active_users(reader)
        assert got == want
    except DataQualityError as ex:
        if is_error and ex.__str__() == error_msg:
            pass
