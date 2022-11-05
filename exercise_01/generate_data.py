# Copyright 2020 N26 GmbH

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


import argparse
import csv
import datetime
import os
import random
import uuid


def generate_transactions(users):
    num_transactions = 100000
    num_users = len(users['data'])

    header = [
        'transaction_id',
        'date',
        'user_id',
        'is_blocked',
        'transaction_amount',
        'transaction_category_id'
    ]

    data = [[
        uuid.uuid4(),
        (datetime.date.today() - datetime.timedelta(days=random.randint(int(i / num_users), 100))).strftime('%Y-%m-%d'),
        users['data'][random.randint(0, num_users - 1)][0],
        random.random() < 0.99,
        '%.2f' % (random.random() * 100),
        random.randint(0, 10)
    ] for i in range(num_transactions)]

    return {'header': header, 'data': data}


def generate_users():
    num_users = 1000
    header = [
        'user_id',
        'is_active'
    ]

    data = [[
        uuid.uuid4(),
        random.random() < 0.9
    ] for _ in range(num_users)]

    return {'header': header, 'data': data}


def write_data(out, header, data):
    if os.path.exists(out):
        print('File %s already exists!' % out)
        return False

    try:
        with open(out, 'w') as f:
          writer = csv.writer(f)
          writer.writerow(header)
          writer.writerows(data)
    except Exception as err:
        print('Failed to write %s' % out)
        return False
    return True


if __name__ == '__main__':
    users = generate_users()
    transactions = generate_transactions(users)

    write_data('users.csv', users['header'], users['data'])
    write_data('transactions.csv', transactions['header'], transactions['data'])
