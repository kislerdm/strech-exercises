"""Application to process and join tables."""

import logging
import os


def read_active_users(path: str, skip_header: bool = True) -> set[str]:
    """Reads active users from `users.csv` file.

    Args:
        path (str): Path to `users.csv` file.
        skip_header (bool): Skip csv header.

    Returns:
        Set of user ID.
    """
    pass


class Reader:
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

    def __next__(self):
        self._readline()
        o = self.line

        if not self.header_skipped:
            self.header_skipped = not self.header_skipped
            return next(self)

        return o

    def _readline(self):
        try:
            self.line = next(self._file_io).rstrip()
            self.row_id += 1
        except StopIteration as e:
            self._file_io.close()
            raise e

    def reopen(self):
        self._file_io.close()
        self._open()


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
    active_users: set[str] = read_active_users(path_users, skip_header)


if __name__ == "__main__":
    path_users_csv = os.getenv("PATH_USERS", "/data/users.csv")
    path_transactions_csv = os.getenv("PATH_TRANSACTIONS", "/data/transactions.csv")

    logs = logging.Logger("join")

    try:
        main(path_users_csv, path_transactions_csv, True)
    except Exception as ex:
        logs.error(ex)
