"""Application to process and join tables."""


import csv
import logging
import os


def main(path_users: str, path_transactions: str):
    """Entrypoint.

    Args:
        path_users (str): Path to `users.csv` file.
        path_transactions (str): Path to `transactions.csv` file.

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

        The application follows the execution path:

        - store the set of `user_id` for all active users from `users.csv`
            - motivation: minimise memory allocation by storing "smallest" table from the join operation.
        - read transactions from `transactions.csv` line-by-line and apply filtering conditions (WHERE and JOIN clauses)
            - store result to dict:
                {transaction_category_id: {sum_amount: set(user_id)}}
                motivation:
                    -    

        users:

        data structure: set(user_id)
            where user_id = str or UUID

        motivation:
        - low memory allocation (todo: Memory allocation UUID vs. str)
        - low time-complexity to scan required for join

        Read users.csv line by line:
        1. read line
        2. skip if header
        3. return to 1 if not active (apply WHERE clause)
        4. parse user_id as UUID
        5. store to set

        results: dict[int, dict[int, set(user)]


        Read transactions.csv line by line:
        1. read line
        2. skip if header
        3. return to 1 if blocked (apply WHERE clause)
        4. parse user_id as UUID
        5. return to 1 if user_id is not present in the set of users (apply INNER JOIN a.k.a. default JOIN)
        6. parse transaction_category_id
        7. parse transaction_amount
        8.


      References:
         - https://wiki.python.org/moin/TimeComplexity
         - https://realpython.com/sorting-algorithms-python/
    """
    csv.reader()


if __name__ == "__main__":
    path_users_csv = os.getenv("PATH_USERS", "/data/users.csv")
    path_transactions_csv = os.getenv("PATH_TRANSACTIONS", "/data/transactions.csv")

    logs = logging.Logger("join")

    try:
        main(path_users_csv, path_transactions_csv)
    except Exception as ex:
        logs.error(ex)
