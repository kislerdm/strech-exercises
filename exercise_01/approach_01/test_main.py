from uuid import UUID

import pytest

from exercise_01.approach_01.main import Table, read_users_csv, Column, _to_bool


@pytest.mark.parametrize("param,want", [
    ("1", True),
    ("0", False),
    ("true", True),
    ("false", False),
    ("100", False),
])
def test__to_bool(param, want):
    assert _to_bool(param) == want


@pytest.fixture()
def data_users_csv_str() -> str:
    return """user_id,is_active
9f709688-326d-4834-8075-1a477d590af7,1
999eb541-c1a0-4888-aeb6-92773fc60e69,0
b923d15c-ce6d-4b2f-913f-31e87ebbcdc2,false
b1ee6da9-aca5-4bc6-bcfb-21ace2185055,true
"""


@pytest.fixture()
def table_users() -> Table:
    return Table(
        columns=tuple(
            Column([[UUID("9f709688-326d-4834-8075-1a477d590af7"), UUID("b1ee6da9-aca5-4bc6-bcfb-21ace2185055")]])),
        column_names=tuple(["user_id"]),
    )


@pytest.fixture
def mocker_data(mocker, data_users_csv_str):
    mocker.patch("builtins.open", mocker.mock_open(read_data=data_users_csv_str))


def test_read_users_csv(mocker_data, table_users, data_users_csv_str):
    got = read_users_csv(path="users.csv")
    assert got == table_users
