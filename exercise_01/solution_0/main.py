"""Application to process and join tables."""

import logging
import os
from typing import Callable, Optional
from uuid import UUID


def parse_row(row: list[str], converter: Optional[Callable[[list[str]], tuple[any, ...]]]) -> tuple[any, ...]:
    """active users from `users.csv` file"""
    pass


class CSVReader:
    """Reads the csv file."""

    def _open(self) -> None:
        self._file_io = open(self.path, "r")
        self.row_id = -1

    def __init__(self, path: str, skip_header: bool = True):
        self.path = path
        self._open()
        self.line: str = ""
        self.header_skipped = not skip_header

    def __iter__(self):
        return self

    def __next__(self) -> Optional[list[str]]:
        self._readline()
        o = self.line

        if not self.header_skipped:
            self.header_skipped = not self.header_skipped
            return next(self)

        return o.split(",")

    def _readline(self):
        try:
            self.line = next(self._file_io).rstrip()
            self.row_id += 1
        except StopIteration as e:
            self._file_io.close()
            raise e


def _is_true(s: str) -> bool:
    """Helper function to check if True."""
    return s.lower() == "true" or s == "1"


class DataQualityError(Exception):
    """Error raised if data are not valid."""
    pass


def read_active_users(reader: CSVReader) -> set[UUID]:
    """Reads active users from the `users.csv`.

    Args:
        reader (CSVReader): Initialised CSVReader object.

    Returns:
        Set of active user ID.
    """
    o: set[UUID] = set()

    for cols in reader:
        if len(cols) < 2:
            raise DataQualityError("wrong number of columns")

        if _is_true(cols[1]):
            try:
                o.add(UUID(cols[0]))
            except Exception:
                raise DataQualityError("failed to decode user_id")

    return o


def main(path_users: str, path_transactions: str, skip_header: bool = True) -> None:
    """Entrypoint.

    Args:
        path_users (str): Path to `users.csv` file.
        path_transactions (str): Path to `transactions.csv` file.
        skip_header (bool): Skip csv header.

    Note:
        The logic is aiming to apply data transformations according to the query:
            SELECT t.transaction_category_id,
                   SUM(t.transaction_amount) AS sum_amount,
                   COUNT(DISTINCT t.user_id) AS num_users
            FROM transactions t
                     JOIN users u USING (user_id)
            WHERE t.is_blocked = False
              AND u.is_active = 1
            GROUP BY t.transaction_category_id
            ORDER BY sum_amount DESC;
    """
    active_users: set[UUID] = read_active_users(CSVReader(path_users, skip_header))


if __name__ == "__main__":
    path_users_csv = os.getenv("PATH_USERS", "/data/users.csv")
    path_transactions_csv = os.getenv("PATH_TRANSACTIONS", "/data/transactions.csv")

    logs = logging.Logger("join")

    try:
        main(path_users_csv, path_transactions_csv, True)
    except Exception as ex:
        logs.error(ex)
