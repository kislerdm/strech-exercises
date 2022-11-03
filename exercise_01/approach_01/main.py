"""Application to process data using native py.

GIVEN:
- two CSV files `transactions.csv` and `users.csv` with the structure:

CREATE TABLE transactions (
    transaction_id          UUID,
    date                    DATE,
    user_id                 UUID,
    is_blocked              BOOL,
    transaction_amount      INTEGER,
    transaction_category_id INTEGER
);

CREATE TABLE users (
    user_id   UUID,
    is_active BOOLEAN
);

WANT:
- to calculate the following aggregation:

SELECT t.transaction_category_id,
       SUM(t.transaction_amount) AS sum_amount,
       COUNT(DISTINCT t.user_id) AS num_users
FROM transactions t
JOIN users u USING (user_id)
    WHERE t.is_blocked = False
    AND u.is_active = 1
GROUP BY t.transaction_category_id
ORDER BY sum_amount DESC;

- to send the result to stdout
"""
import io
import logging
import os
import sys
from typing import Any, List, Dict, Tuple
from uuid import UUID

Column = list[Any]


class Row(Tuple[Any]):
    """Table ``Row``."""

    def __str__(self) -> str:
        return ",".join((str(v) for v in self))


class Table:
    """Defines column-oriented ``Table``"""

    def __init__(self, columns: Tuple[Column], column_names: Tuple[str]) -> None:
        """Initiates a column-oriented ``Table``.

        Note: columns are ordered and accessible by the order, not name.

        Args:
            columns (tuple): Columns content.
            column_names (tuple): Column names.

        Raises:
            ValueError: when column_names length does not match the ``Table`` size.
        """
        if len(column_names) != len(columns):
            raise ValueError("column_names does not match the table size")

        self.columns: Tuple[Column] = columns
        self.column_names: Tuple[str] = column_names
        self._cnt_rows: int = len(self.columns[0])
        self._iter = 0

    def __len__(self):
        return self._cnt_rows

    def print_head(self, limit: int = 10, output: io.TextIOBase = sys.stdout) -> None:
        """Prints the first rows elements.

        Args:
            limit (int): How many rows to be printed.
            output (TextIO): Writer interface.
        """
        print(",".join(self.column_names), file=output)

        i = 0
        while i < limit and i < len(self):
            print(self.read_row(i), file=output)
            i += 1

    def column_by_name(self, column_name: str) -> Column:
        """Returns the column by its name.

        Args:
            column_name (str): Column name to be used to group.

        Returns:
            Column.

        Raises:
            KeyError: when column is not found.
        """
        return self.columns[self._get_column_index(column_name)]

    def _get_column_index(self, column_name: str) -> int:
        """Returns the column index by its name.

        Args:
            column_name (str): Column name to be used to group.

        Returns:
            Column index.

        Raises:
            KeyError: when column is not found.
        """
        for i, v in enumerate(self.column_names):
            if v == column_name:
                return i

        raise KeyError("column '%s' not found" % column_name)

    def append_row(self, row: Row) -> None:
        """Appends the ``Row``.

        Args:
            row (Row): Row to append to the table.

        Raises:
            KeyError: when row field corresponding to a column name is not found.
        """
        if len(row) != len(self.columns):
            raise KeyError("row does not match the structure of the table")

        for i, _ in enumerate(self.columns):
            self.columns[i].append(row[i])

    def read_row(self, index: int) -> Row:
        """Returns the row at a given index.

        Args:
            index (int): Index of the row.

        Returns:
            The ``Row`` object.

        Raises:
            IndexError: when the index is not found.
        """
        if index > len(self) or index < 0:
            raise IndexError("index is not found")

        return Row(column[index] for column in self.columns)

    def group_by(self, column_name: str) -> Dict[str, "Table"]:
        """Groups the table by column.

        Args:
            column_name (str): Column name to be used to group.

        Returns:
            Dictionary with the key being the `column_name` and values tables subgroups.

        Raises:
            KeyError: when column is not found.
        """
        column_index: int = self._get_column_index(column_name)

        column_group_by: Column = self.columns[column_index]

        group_table_col_names = tuple(v for v in self.column_names if v != column_name)

        result: Dict[str, "Table"] = {}

        for i, value in enumerate(column_group_by):
            row: Row = Row(v for field_index, v in enumerate(self.read_row(i)) if field_index != column_index)

            if result.get(value) is None:
                result[value] = Table(columns=tuple([[]] * len(group_table_col_names)),
                                      column_names=group_table_col_names)

            result[value].append_row(row)

        return result


class RowParsingError(Exception):
    """Error of parsing error."""


def _to_bool(param: Any) -> bool:
    """Helper function to convert to bool."""
    if isinstance(param, str):
        return param.lower() == "true" or param == "1"

    if isinstance(param, int):
        return int(param) > 0


def read_users_csv(path: str, skip_header: bool = True) -> Table:
    """Reads active users from csv file.

    Args:
        path (str): Path to `users.csv` file.
        skip_header (bool): Skip header row.

    Returns:
        ``Table`` object for users.

    Note:
        The csv file must have the header and follow the structure "user_id,is_active".

        The file

        user_id,is_active
        9f709688-326d-4834-8075-1a477d590af7,1
        999eb541-c1a0-4888-aeb6-92773fc60e69,0
        b923d15c-ce6d-4b2f-913f-31e87ebbcdc2,false
        b1ee6da9-aca5-4bc6-bcfb-21ace2185055,true

        corresponds to the table with two rows:

        9f709688-326d-4834-8075-1a477d590af7
        b1ee6da9-aca5-4bc6-bcfb-21ace2185055
    """
    result = Table(tuple([Column()]), column_names=tuple(["user_id"]))

    cnt_row = 0
    with open(path, "r") as f:
        for line in f:
            cnt_row += 1
            if skip_header and cnt_row == 1:
                continue

            lst_columns: List[str] = line.rstrip().split(",")
            if len(lst_columns) < 2:
                raise RowParsingError("cannot parse the row %d" % cnt_row)

            if not _to_bool(lst_columns[1]):
                continue

            try:
                user_id = UUID(lst_columns[0])
            except Exception as ex:
                raise RowParsingError("failed to decode user_id for the row %d" % cnt_row) from ex

            result.append_row(Row(tuple([user_id])))

    return result


def read_transactions_csv(path: str, skip_header: bool = True) -> Table:
    """Reads non-blocker transactions from csv file.

    Args:
        path (str): Path to `transactions.csv` file.
        skip_header (bool): Skip header row.

    Returns:
        ``Table`` object for transactions.

    Note:
        The csv file must have the header and follow
            the structure "transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id".

        The file

        transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id
        ce861100-26f0-4f1a-a8e3-8d6b3ad7a0e8,2022-01-01,9f709688-326d-4834-8075-1a477d590af7,1,100,1
        3e6cdc49-f1c5-4ac6-9483-37622eed207a,2022-01-01,9f709688-326d-4834-8075-1a477d590af7,0,200,1
        5c2e5c85-75e1-4137-bf13-529a000757f6,2022-02-01,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,true,100,1
        35715617-ea5d-4c00-842a-0aa81b224934,2022-02-02,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,false,200,1
        ca0d184e-7297-4ac2-95a6-6ed719a67b0a,2022-02-02,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,false,20,2

        would result in the table with one row:

        3e6cdc49-f1c5-4ac6-9483-37622eed207a,9f709688-326d-4834-8075-1a477d590af7,200,1
        35715617-ea5d-4c00-842a-0aa81b224934,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,200,1
        ca0d184e-7297-4ac2-95a6-6ed719a67b0a,b1ee6da9-aca5-4bc6-bcfb-21ace2185055,20,2
    """
    result = Table(tuple([Column() * 4]),
                   column_names=tuple(["transaction_id", "user_id", "transaction_amount", "transaction_category_id"]))

    cnt_row = 0
    with open(path, "r") as f:
        for line in f:
            if cnt_row == 0 and skip_header:
                continue

            lst_columns: List[str] = line.rstrip().split(",")
            if len(lst_columns) < 2:
                raise RowParsingError("cannot parse the row %d" % cnt_row)

            if not _to_bool(lst_columns[3]):
                continue

            try:
                transaction_id = UUID(lst_columns[0])
            except Exception as ex:
                raise RowParsingError("failed to decode transaction_id for the row %d" % cnt_row) from ex

            try:
                user_id = UUID(lst_columns[3])
            except Exception as ex:
                raise RowParsingError("failed to decode user_id for the row %d" % cnt_row) from ex

            try:
                transaction_amount = int(lst_columns[4])
            except Exception:
                raise RowParsingError("failed to decode transaction_amount for the row %d" % cnt_row)

            try:
                transaction_category_id = int(lst_columns[5])
            except Exception:
                raise RowParsingError("failed to decode transaction_category_id for the row %d" % cnt_row)

            result.append_row(Row(tuple([transaction_id, user_id, transaction_amount, transaction_category_id])))

            cnt_row += 1

    return result


def main(path_users: str, path_transactions: str):
    """Application entrypoint.

    Args:
        path_users (str): Path to `users.csv` file.
        path_transactions (str): Path to `transactions.csv` file.
    """
    users: Table = read_users_csv(path_users)
    transactions: Table = read_transactions_csv(path_transactions)

    transactions_grouped_by_user = transactions.group_by("user_id")

    result_column_names = tuple(["transaction_category_id", "sum_amount", "num_users"])

    result = Table(columns=tuple([[]] * len(result_column_names)),
                   column_names=result_column_names)

    for user_id in set(transactions_grouped_by_user.keys()).intersection(set(users.column_by_name("user_id"))):
        for i in range(len(transactions_grouped_by_user[user_id])):
            transactions_grouped_by_user[user_id].read_row(i)

    result.print_head(100)


if __name__ == "__main__":
    path_users_csv = os.getenv("PATH_USERS", "/data/users.csv")
    path_transactions_csv = os.getenv("PATH_TRANSACTIONS", "/data/transactions.csv")

    logs = logging.Logger("join")

    try:
        main(path_users_csv, path_transactions_csv)
    except Exception as ex:
        logs.error(ex)
