"""Application to process and join tables."""
import logging
import os
import time
from operator import itemgetter
from typing import Optional
from uuid import UUID


class CSVReader:
    """Reads the csv file."""

    def _open(self) -> None:
        self._file_io = open(self.path, "r")
        self.row_id = -1

    def __init__(self, path: str, skip_header: bool = True) -> None:
        self.path = path
        self._open()
        self.line: str = ""
        self.header_skipped = not skip_header

    def __iter__(self) -> "CSVReader":
        return self

    def __next__(self) -> list[str]:
        self._readline()
        o = self.line

        if not self.header_skipped:
            self.header_skipped = not self.header_skipped
            return next(self)

        return o.split(",")

    def _readline(self) -> None:
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
        reader (``CSVReader``): Initialised CSVReader object.

    Returns:
        Set of active user ID.

    Raises:
        DataQualityError: when data validation error happened.
    """
    o: set[UUID] = set()

    for cols in reader:
        if len(cols) < 2:
            raise DataQualityError("wrong number of columns in row %d" % reader.row_id)

        if _is_true(cols[1]):
            try:
                o.add(UUID(cols[0]))
            except ValueError as e:
                raise DataQualityError("failed to decode user_id in row %d: %s" % (reader.row_id, e.__str__()))

    return o


class Transaction:
    def __init__(
        self, transaction_id: UUID, user_id: UUID, transaction_amount: int, transaction_category_id: int
    ) -> None:
        """Defines a transaction with relevant attributes.

        Args:
            transaction_id (UUID): Transaction ID.
            user_id (UUID): User ID.
            transaction_amount (int): Amount.
            transaction_category_id (int): Category.
        """
        self.transaction_id = transaction_id
        self.user_id = user_id
        self.transaction_amount = transaction_amount
        self.transaction_category_id = transaction_category_id

    def __lt__(self, other: "Transaction") -> bool:
        return self.transaction_amount < other.transaction_amount

    def __eq__(self, other) -> bool:
        if self is None:
            return other is None
        if other is not None:
            return (
                self.transaction_amount == other.transaction_amount
                and self.transaction_category_id == other.transaction_category_id
                and self.user_id == other.user_id
                and self.transaction_id == other.transaction_id
            )
        return False


def new_not_blocked_transaction(row: list[str]) -> Optional[Transaction]:
    """Reads a single non-blocked transaction from the parsed row of `transactions.csv` file.

    Args:
        row (list): Parsed data row.

    Returns:
        ``Transaction`` object.

    Raises:
        DataQualityError: when data validation error happened.
    """
    if len(row) < 6:
        raise DataQualityError("wrong number of columns")

    if _is_true(row[3]):
        return None

    try:
        transaction_id = UUID(row[0])
    except ValueError as e:
        raise DataQualityError("failed to decode transaction_id: %s" % e.__str__())

    try:
        user_id = UUID(row[2])
    except ValueError as e:
        raise DataQualityError("failed to decode user_id: %s" % e.__str__())

    try:
        transaction_amount = int(row[4])
    except ValueError as e:
        raise DataQualityError("failed to decode transaction_amount: %s" % e.__str__())

    try:
        transaction_category_id = int(row[5])
    except ValueError as e:
        raise DataQualityError("failed to decode transaction_category_id: %s" % e.__str__())

    try:
        _ = time.strptime(row[1], "%Y-%m-%d")
    except ValueError as e:
        raise DataQualityError("failed to decode date: %s" % e.__str__())

    return Transaction(transaction_id, user_id, transaction_amount, transaction_category_id)


class TransactionCategoryKPI:
    def __init__(self, sum_amount: int, num_users: int):
        """Join output KPI.

        Args:
            sum_amount (int): Total transactions amount.
            num_users (int): Total numer of users.
        """
        self.sum_amount: int = sum_amount
        self.num_users: int = num_users

    def __lt__(self, other) -> bool:
        return self.sum_amount < other.sum_amount

    def __eq__(self, other) -> bool:
        return self.sum_amount == other.sum_amount and self.num_users == self.num_users


class TransactionCategoryKPICalc(TransactionCategoryKPI):
    """Defines the "container" for KPI fields calculation per transaction category."""

    def __init__(self, kpi: TransactionCategoryKPI = None) -> None:
        super().__init__(kpi.sum_amount, kpi.num_users) if kpi is not None else super().__init__(0, 0)

        self._unique_users: set[UUID] = set()

    def add_transaction(self, transaction: Transaction) -> None:
        """Adds a transaction data.

        Args:
            transaction (int): Transaction object.
        """
        self.sum_amount += transaction.transaction_amount
        self._unique_users.add(transaction.user_id)

    def calculate(self):
        self.num_users = len(self._unique_users)
        del self._unique_users


class QueryResult(dict[int, TransactionCategoryKPICalc]):
    """Define the class to keep results of inner join."""

    def add_transaction(self, transaction: Transaction) -> None:
        """Adds a transaction.

        Args:
            transaction (Transaction): Transaction object.
        """
        kpi: TransactionCategoryKPICalc = self.get(transaction.transaction_category_id, TransactionCategoryKPICalc())
        kpi.add_transaction(transaction)
        self[transaction.transaction_category_id] = kpi

    def calculate(self):
        """Calculates the results."""
        for v in self.values():
            v.calculate()

    def sort_by_transactions_amount(self, desc: bool = True):
        """Sorts by transaction_amount.

        Args:
            desc (bool): Descending order.

        Note:
            Inspired by https://writeonly.wordpress.com/2008/08/30/sorting-dictionaries-by-value-in-python-improved/.
        """
        temp = sorted(self.items(), key=itemgetter(1), reverse=desc)

        while len(self) > 0:
            self.popitem()

        for k, v in temp:
            self[k] = v

    def __eq__(self, other) -> bool:
        if len(self) != len(other):
            return False

        for kl, kr in zip(self.keys(), other.keys()):
            if kl != kr:
                return False

            if self[kl] != other.get(kl, TransactionCategoryKPICalc()):
                return False

        return True

    def __str__(self) -> str:
        header: str = "transaction_category_id,sum_amount,num_users"
        rows: str = "\n".join([f"{k},{v.sum_amount},{v.num_users}" for k, v in self.items()])
        return f"{header}\n{rows}\n"


def main(path_users: str, path_transactions: str, skip_header: bool = True) -> Optional[QueryResult]:
    """Entrypoint.

    Args:
        path_users (str): Path to `users.csv` file.
        path_transactions (str): Path to `transactions.csv` file.
        skip_header (bool): Skip csv header.

    Returns:
        Query results.

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

    if len(active_users) == 0:
        return None

    result: QueryResult = QueryResult()

    for row in CSVReader(path_transactions, skip_header):
        transaction: Optional[Transaction] = new_not_blocked_transaction(row)

        if transaction is None:
            continue

        # JOIN condition:
        if transaction.user_id not in active_users:
            continue

        result.add_transaction(transaction)

    result.calculate()
    result.sort_by_transactions_amount()

    return result


if __name__ == "__main__":
    base_dir = os.getenv("BASE_DIR", "/data")
    path_users_csv = f"{base_dir}/users.csv"
    path_transactions_csv = f"{base_dir}/transactions.csv"

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S.%03d"
    )

    logs = logging.getLogger("joiner")

    try:
        t0 = time.time()
        results = main(path_users_csv, path_transactions_csv, True)
        logs.info("elapsed time: %.0f microseconds" % ((time.time() - t0) * 1_000_000))
        logging.shutdown()

        print(results)
    except Exception as ex:
        logs.error(ex)
