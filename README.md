# Exercises

## Exercise 1 - Joining Data Sets

### Description

For the task, assume that we have a database with the following schema:

```sql
CREATE TABLE transactions
(
    transaction_id          UUID,
    date                    DATE,
    user_id                 UUID,
    is_blocked              BOOL,
    transaction_amount      INTEGER,
    transaction_category_id INTEGER
);

CREATE TABLE users
(
    user_id   UUID,
    is_active BOOLEAN
);
```

Example data for these tables is stored in the corresponding CSV files `transactions.csv` and `users.csv`, which can be
generated from the generate_data.py script.
We want to compute the result of the following query:

```sql
SELECT t.transaction_category_id,
       SUM(t.transaction_amount) AS sum_amount,
       COUNT(DISTINCT t.user_id) AS num_users
FROM transactions t
         JOIN users u USING (user_id)
WHERE t.is_blocked = False
  AND u.is_active = 1
GROUP BY t.transaction_category_id
ORDER BY sum_amount DESC;
```

But unfortunately the query planner of our database can not optimize the query well enough in order to get the results.
Your task is now to write a Python program using only the Python Standard Library (preferably not using external
libraries at all), which reads data from CSV files transactions.csv and users.csv, and computes the equivalent result of
the SQL query in an efficient way that would be scalable for large data sets as well. The result should be printed to
stdout.
Please note that the scope of the task is not to parse the SQL query or to generalize the computation in any way, but
only to write a program which computes the result of this one specific query in an efficient way.
Reviewing this task, we pay special attention not only to the correctness of results, but especially to the code quality
and efficiency of the data structures and algorithms used.
Remember that it is easier for us to review your task if we can test & run it. Providing Dockerfile and/or Makefile
would be helpful (but not strictly required).

### Solutions

See the code in [the directory](exercise_01).
