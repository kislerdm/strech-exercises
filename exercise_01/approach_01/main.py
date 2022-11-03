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
from abc import abstractmethod
from datetime import date
from typing import Callable, Optional, Set, Type
from uuid import UUID


class FilteringError(Exception):
    """Defines the error thrown when filtering error happened"""


class RowParsingError(Exception):
    """Defines the error thrown when a csv encoded row parsing error happened"""


class Row:
    """Table ``Row``"""

    @property
    @abstractmethod
    def header(self) -> Set[str]:
        """Returns the header."""
        pass

    def apply_filter(self, filter_fn: Callable[["Row"], bool]) -> bool:
        """Allies filter function.

        Args:
            filter_fn (Callable): Filter function.

        Returns:
            Flag: true if passes the filter, false otherwise.

        Raises:
            FilteringError: when filtering error happened.
        """
        return filter_fn(self)

    @abstractmethod
    def parse_csv_row(self, row: str) -> None:
        """Parses csv encoded row.

        Args:
            row (str): Row as csv encoded string.
        """
        pass

    def __init__(self, raw_str: str) -> None:
        """Initialises table's ``Row``.

        Args:
            raw_str (str): Row as csv encoded string.

        Raises:
            RowParsingError: when csv encoded string parsing error happened.
        """
        try:
            self.parse_csv_row(raw_str)
        except Exception as ex:
            raise RowParsingError(ex) from ex


Table = list[Row]
Filter = Callable[[Type[Row]], Optional[Type[Row]]]


class RowUsers(Row):
    def parse_csv_row(self, row: str) -> None:
        raise NotImplemented("todo")

    @property
    def header(self):
        return {
            "user_id",
            "is_active",
        }

    @property
    def id(self):
        return self.user_id

    def __init__(self, raw_str: str):
        self.user_id: UUID = UUID(bytes=bytes(0), version=4)
        self.is_active: bool = False

        super().__init__(raw_str)


class RowTransactions(Row):
    def parse_csv_row(self, row: str) -> None:
        raise NotImplemented("todo")

    @property
    def header(self):
        return {
            "transaction_id",
            "date",
            "user_id",
            "is_blocked",
            "transaction_amount",
            "transaction_category_id",
        }

    @property
    def id(self):
        return self.transaction_id

    def __init__(self, raw_str: str):
        self.transaction_id: UUID = UUID(bytes=bytes(0), version=4)
        self.date: date = date(1970, 1, 1)
        self.user_id: UUID = UUID(bytes=bytes(0), version=4)
        self.is_blocked: bool = False
        self.transaction_amount: int = 0
        self.transaction_category_id: int = 0
        super().__init__(raw_str)


class RowJoinResult(Row):
    def parse_csv_row(self, row: str) -> None:
        raise NotImplemented("todo")

    @property
    def header(self):
        return {
            "transaction_category_id",
            "sum_amount",
            "num_users",
        }

    @property
    def id(self):
        return self.transaction_category_id

    def __init__(self, raw_str: str):
        self.transaction_category_id: UUID = UUID(bytes=bytes(0), version=4)
        self.sum_amount: int = 0
        self.num_users: int = 0

        super().__init__(raw_str)

def read_and_filter(path: str, row_type: Type[Row], filter_fn: Filter) -> Table:
    """Reads and filters data from a csv file.

    Args:
        path (str): Path to csv file.
        row_type (Row): Row type.
        filter_fn (Callable): Function to filter the input on row-by-row basis.

    Returns:
        Column oriented ``Table`` object.
    """


def main(path_users: str, filter_user: Optional[Filter], path_transactions: str, filter_transaction: Optional[Filter]):
    """Application entrypoint.

    Args:
        path_users (str): Path to `users.csv` file.
        filter_user (Callable): Function to filter users input on row-by-row basis.
        path_transactions (str): Path to `transactions.csv` file.
        filter_transaction (Callable): Function to filter users input on row-by-row basis.
    """

    data_users = read_and_filter(path_users, RowUsers, filter_user)
    data_transactions = read_and_filter(path_transactions, RowUsers, filter_transaction)

    # join_map = join(data_transactions, data_users, ("user_id", "user_id"))
    # result = join_reduce(join_map)
    # result = order(result, "sum_amount")


if __name__ == "__main__":
    main("users.csv", None, "transactions.csv", None)
