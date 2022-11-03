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

Want:
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
from typing import Type, Any, List, Dict, Tuple, Optional, Set


class RowParsingError(Exception):
    """Defines the error thrown when a csv encoded row parsing error happened"""


# class Row:
#     """Table ``Row``"""
#
#     @abstractmethod
#     def parse_csv_row(self, row: str) -> None:
#         """Parses csv encoded row.
#
#         Args:
#             row (str): Row as csv encoded string.
#         """
#         pass
#
#     def __init__(self, raw_str: str) -> None:
#         """Initialises table's ``Row``.
#
#         Args:
#             raw_str (str): Row as csv encoded string.
#
#         Raises:
#             RowParsingError: when csv encoded string parsing error happened.
#         """
#         try:
#             self.parse_csv_row(raw_str)
#         except Exception as ex:
#             raise RowParsingError(ex) from ex
#
#
# class RowUsers(Row):
#     def parse_csv_row(self, row: str) -> None:
#         raise NotImplemented("todo")
#
#     def __init__(self, raw_str: str):
#         """Reads a user from csv encoded string if it's an active user."""
#         self.user_id: UUID = UUID(bytes=bytes(0), version=4)
#
#         super().__init__(raw_str)
#
#
# class RowTransactions(Row):
#     def parse_csv_row(self, row: str) -> None:
#         raise NotImplemented("todo")
#
#     def __init__(self, raw_str: str):
#         """Reads a transaction from csv encoded string if it's not blocked."""
#         self.transaction_id: UUID = UUID(bytes=bytes(0), version=4)
#         self.user_id: UUID = UUID(bytes=bytes(0), version=4)
#         self.transaction_amount: int = 0
#         self.transaction_category_id: int = 0
#
#         super().__init__(raw_str)


# @dataclass
# class Column:
#     """Defines table ``Column``"""
#     name: str
#     values: List[Any]
#
#     def sort(self, desc: bool = False) -> None:
#         """Sort values.
#
#         Args:
#             desc (bool): Sort in descending order.
#         """
#         self.values.sort(reverse=desc)

Row = Tuple[Any]
Column = List[Any]


class Table:
    """Defines column-oriented ``Table``"""

    def __init__(self, columns: Tuple[Column], column_names: Optional[Set[str]]) -> None:
        """Initiates a column-oriented ``Table``.

        Note: columns are ordered and accessible by the order, not name.

        Args:
            columns (tuple): Columns content.
            column_names (set): Column names.

        Raises:
            ValueError: when column_names length does not match the ``Table`` size.
        """
        if column_names is not None:
            if len(column_names) != len(columns):
                raise ValueError("column_names does not match the table size")

        self.columns: Tuple[Column] = columns
        self.column_names: Set[str] = column_names
        self.cnt_rows: int = len(self.columns[0])

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
            ValueError: when the index is not found.
        """
        if index > self.cnt_rows or index < 0:
            raise ValueError("index is not found")

        return tuple(column[index] for column in self.columns)

    def pop(self, column_index: int) -> Column:
        """Removes column by its name and returns it

        Args:
            column_index (int): Column index.

        Returns:
            The ``Column`` object.

        Raises:
            KeyError: when column is not found.
        """
        if column_index < 0 or column_index > len(self.columns):
            raise KeyError("column not found")

        return self.columns[column_index]

    def group_by(self, column_name: str) -> Dict[str, "Table"]:
        """Groups the table by column.

        Args:
            column_name (str): Column name to be used to group.

        Returns:
            Dictionary with the key being the `column_name` and values tables subgroups.

        Raises:
            KeyError: when column is not found.
        """
        column_index = self.column_names
        column_group_by: Column = self.pop(column_name)

        result: Dict[str, "Table"]

        for i, value in enumerate(column_group_by.values):
            result[value] = None

        return result


def read_users_csv(path: str) -> Table:
    """Reads active users from csv file.

    Args:
        path (str): Path to `users.csv` file.

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
    pass

def read_transactions_csv(path: str) -> Table:
    """Reads non-blocker transactions from csv file.

    Args:
        path (str): Path to `transactions.csv` file.

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
    pass


def main(path_users: str, path_transactions: str):
    """Application entrypoint.

    Args:
        path_users (str): Path to `users.csv` file.
        path_transactions (str): Path to `transactions.csv` file.
    """

    users: Table = read_users_csv(path_users)
    transactions: Table = read_transactions_csv(path_transactions)

    # join_map = join(data_transactions, data_users, ("user_id", "user_id"))
    # result = join_reduce(join_map)
    # result = order(result, "sum_amount")


if __name__ == "__main__":
    main("users.csv", "transactions.csv")
