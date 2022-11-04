from uuid import UUID

import pytest

from main import Table, read_users_csv, Column, _to_bool, read_transactions_csv


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


@pytest.fixture()
def data_transactions_csv_str() -> str:
    return """transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id
ce861100-26f0-4f1a-a8e3-8d6b3ad7a0e8,2022-01-01,9f709688-326d-4834-8075-1a477d590af7,1,100,1
3e6cdc49-f1c5-4ac6-9483-37622eed207a,2022-01-01,9f709688-326d-4834-8075-1a477d590af7,0,200,1
5c2e5c85-75e1-4137-bf13-529a000757f6,2022-02-01,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,true,100,1
35715617-ea5d-4c00-842a-0aa81b224934,2022-02-02,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,false,200,1
ca0d184e-7297-4ac2-95a6-6ed719a67b0a,2022-02-02,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,false,20,2
"""


@pytest.fixture()
def table_transactions() -> Table:
    return Table(
        columns=tuple([
            Column([["3e6cdc49-f1c5-4ac6-9483-37622eed207a", "35715617-ea5d-4c00-842a-0aa81b224934",
                     "ca0d184e-7297-4ac2-95a6-6ed719a67b0a"]]),
            Column([["9f709688-326d-4834-8075-1a477d590af7", "b1ee6da9-aca5-4bc6-bcfb-21ace2185055",
                     "b1ee6da9-aca5-4bc6-bcfb-21ace2185055"]]),
            Column([[200, 200, 20]]),
            Column([[1, 1, 2]]),
        ]),
        column_names=tuple(
            ["transaction_id", "user_id", "transaction_amount", "transaction_category_id"],
        ))


def test_read_users_csv(mocker, table_users, data_users_csv_str):
    mocker.patch("builtins.open", mocker.mock_open(read_data=data_users_csv_str))
    got = read_users_csv(path="users.csv")
    assert got == table_users


def test_read_transactions_csv(mocker, table_transactions, data_transactions_csv_str):
    mocker.patch("builtins.open", mocker.mock_open(read_data=data_transactions_csv_str))
    got = read_transactions_csv(path="transactions.csv")
    assert got == table_transactions


class TestTable:
    @pytest.mark.parametrize(
        "columns,column_names,len_want,is_error,error_msg",
        [
            (
                    (Column([1, 2, 3]), Column(["a", "b", "c"])),
                    ("foo", "bar"),
                    2,
                    False,
                    None,
            )
        ]
    )
    def test_init(self, columns, column_names, len_want, is_error, error_msg):
        got = Table(columns, column_names)
