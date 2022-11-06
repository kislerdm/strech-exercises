# Copyright 2020 N26 GmbH
# Copyright 2022 Dmitry Kisler <admin@dkisler.com>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import csv
import datetime
import os
import random
import uuid
from math import ceil
from typing import Any


def generate_transactions(users: dict[str, list[Any]]) -> dict[str, list[Any]]:
    multiplication_factor = 100

    num_users = len(users["data"])
    num_transactions = num_users * multiplication_factor

    header = ["transaction_id", "date", "user_id", "is_blocked", "transaction_amount", "transaction_category_id"]

    data = [
        [
            uuid.uuid4(),
            (
                datetime.date.today()
                - datetime.timedelta(days=random.randint(int(i / num_users), multiplication_factor))
            ).strftime("%Y-%m-%d"),
            users["data"][random.randint(0, num_users - 1)][0],
            random.random() < 0.99,
            int(random.random() * 10000),
            random.randint(0, 10),
        ]
        for i in range(num_transactions)
    ]

    return {"header": header, "data": data}


def generate_users(num_users: int) -> dict[str, list[Any]]:
    header = ["user_id", "is_active"]

    data = [[uuid.uuid4(), random.random() < 0.9] for _ in range(num_users)]

    return {"header": header, "data": data}


def write_data(out: str, header: list[str], data: list[list[Any]], append: bool = False) -> bool:
    try:
        mode: str = "a" if append else "w"

        with open(out, mode) as f:
            writer = csv.writer(f)
            if not append:
                writer.writerow(header)
            writer.writerows(data)
    except Exception:
        print("Failed to write %s" % out)
        return False
    return True


if __name__ == "__main__":
    base_dir = os.getenv("BASE_DIR", "fixtures/test")

    num_users: int = 1000
    num_users_str = os.getenv("NUM_USERS", "1000")

    try:
        num_users = int(num_users_str) if int(num_users_str) > 10 else num_users
    except ValueError:
        pass

    num_users_data_sink_limit: int = 5000
    num_steps: int = ceil(num_users / num_users_data_sink_limit)

    print("generate data for %d users over %d steps" % (num_users, num_steps))

    for step in range(num_steps):
        print("step %d" % step)

        num_users_step = num_users_data_sink_limit if num_users_data_sink_limit < num_users else num_users

        users = generate_users(num_users_step)
        transactions = generate_transactions(users)

        write_data(f"{base_dir}/users.csv", users["header"], users["data"], step > 0)
        write_data(f"{base_dir}/transactions.csv", transactions["header"], transactions["data"], step > 0)

        num_users -= num_users_data_sink_limit
