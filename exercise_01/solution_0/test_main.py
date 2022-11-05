from typing import Optional
from uuid import UUID, uuid4

import pytest

from main import CSVReader, read_active_users, DataQualityError, JOINResult, Transaction, TransactionCategoryKPICalc, \
    new_not_blocked_transaction, TransactionCategoryKPI


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


@pytest.mark.parametrize("row,want,is_error,error_msg", [
    (["ce861100-26f0-4f1a-a8e3-8d6b3ad7a0e8", "2022-01-01", "9f709688-326d-4834-8075-1a477d590af7", "1", "100", "1"],
     None,
     False,
     ""),
    (["5c2e5c85-75e1-4137-bf13-529a000757f6", "2022-02-01", "b1ee6da9-aca5-4bc6-bcfb-21ace2185055", "true", "100", "1"],
     None,
     False,
     ""),
    (["3e6cdc49-f1c5-4ac6-9483-37622eed207a", "2022-01-01", "9f709688-326d-4834-8075-1a477d590af7", "0", "200", "1"],
     Transaction(UUID("3e6cdc49-f1c5-4ac6-9483-37622eed207a"), UUID("9f709688-326d-4834-8075-1a477d590af7"), 200, 1),
     False,
     ""
     ),
])
def test_new_not_blocked_transaction(row, want, is_error, error_msg):
    try:
        got = new_not_blocked_transaction(row)
        assert got == want
    except DataQualityError as ex:
        if is_error and ex.__str__() == error_msg:
            pass


def test_TransactionCategoryKPICalc():
    kpi = TransactionCategoryKPICalc()
    # GIVEN
    # transactions
    # done by a single user
    uid = uuid4()

    transactions: tuple[Transaction, ...] = (
        Transaction(transaction_id=uuid4(), user_id=uid, transaction_amount=1, transaction_category_id=1),
        Transaction(transaction_id=uuid4(), user_id=uid, transaction_amount=1, transaction_category_id=2),
        Transaction(transaction_id=uuid4(), user_id=uid, transaction_amount=10, transaction_category_id=1),
    )

    # WHEN add transaction to KPIs
    for transaction in transactions:
        kpi.add_transaction(transaction)

    # AND perform result calculation
    kpi.calculate()

    # THEN
    assert kpi.sum_amount == sum((transaction.transaction_amount for transaction in transactions)), \
        "total amount does not match expectation"

    assert kpi.num_users == len(set(transaction.user_id for transaction in transactions)), \
        "total numer of unique users does not match expectation"


def test_JOINResult():
    result = JOINResult()

    # GIVEN
    # transactions
    # done by a single active user
    uid = uuid4()

    transactions: tuple[Transaction, ...] = (
        Transaction(transaction_id=uuid4(), user_id=uid, transaction_amount=1, transaction_category_id=1),
        Transaction(transaction_id=uuid4(), user_id=uid, transaction_amount=1, transaction_category_id=2),
        Transaction(transaction_id=uuid4(), user_id=uid, transaction_amount=10, transaction_category_id=3),
        Transaction(transaction_id=uuid4(), user_id=uuid4(), transaction_amount=4, transaction_category_id=1),
    )

    # WHEN add transactions to the result
    for transaction in transactions:
        result.add_transaction(transaction)

    result.calculate()

    # AND perform sort by category
    result.sort_by_transactions_amount(desc=True)

    # THEN
    # the result MUST match
    want = JOINResult({
        transactions[2].transaction_category_id: TransactionCategoryKPICalc(TransactionCategoryKPI(10, 1)),
        transactions[0].transaction_category_id: TransactionCategoryKPICalc(TransactionCategoryKPI(5, 2)),
        transactions[1].transaction_category_id: TransactionCategoryKPICalc(TransactionCategoryKPI(1, 1)),
    })

    assert result == want

    # AND print must yield
    want_print = """transaction_category_id,sum_amount,num_users
3,10,1
1,5,2
2,1,1"""

    assert result.__str__() == want_print
