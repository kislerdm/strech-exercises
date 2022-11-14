# ML Feature Table Computation

<!-- TOC -->
* [Problem](#problem)
* [Proposed Solution](#proposed-solution)
* [Acceptance Criteria](#acceptance-criteria)
* [Solution](#solution)
  * [Tests](#tests)
* [The Execution Process](#the-execution-process)
  * [Execution Plan for Solution Query](#execution-plan-for-solution-query)
    * [Reference Query](#reference-query)
<!-- TOC -->

## Problem

As a data scientist, I want timeseries data with the total number of transactions a user made in the last 7 days from a
given date.

The timeseries should have the following dimensions:

- transaction_id
- user_id

## Proposed Solution

Prepare a SQL query to calculate required KPI using the data from the `transactions` table:

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
```

The desired table would look as following.

| transaction_id |  user_id  |    date    | # Transaction within previous 7 days |
|:--------------:|:---------:|:----------:|-------------------------------------:|
|   ef05-4247    | becf-457e | 2020-01-01 |                                    0 |
|   c8d1-40ca    | becf-457e | 2020-01-05 |                                    1 |
|   fc2b-4b36    | becf-457e | 2020-01-07 |                                    2 |
|   3725-48c4    | becf-457e | 2020-01-15 |                                    0 |        
|   5f2a-47c2    | becf-457e | 2020-01-16 |                                    1 |        
|   7541-412c    | 5728-4f1c | 2020-01-01 |                                    0 |        
|   3deb-47d7    | 5728-4f1c | 2020-01-12 |                                    0 |

## Acceptance Criteria

- Postgres compatible [ANSI compliant](https://www.oninit.com/manual/informix/100/ddi/ddi32.htm) SQL query is designed.
- Walkthrough on the steps the data database engine follows when one executes a query is documented.
- Answer to the question "what database engine considers to execute query effectively?" is provided.

## Solution

Please find the solution in the [solution.sql](solution.sql).

### Tests

_Requirements_:

- [docker](https://docs.docker.com/get-docker/) ~> 20.10
- [gnuMake](https://www.gnu.org/software/make/)

Run tests by executing the command:

```commandline
make tests
```

## The Execution Process

Although query execution details depend on the type and version of database engine, all relational databases operate in
the request-response communication model:

- Establishment of a client **connection**;
- Deserialization of the request query into the **raw** [**AST**](http://ns.inria.fr/ast/sql/index.html);
- Generation of **query AST** by mapping the raw AST onto underlying database objects referenced in the request query;
- Generation of **query plan** by selecting optimal strategy to execute the query;
- **Execution** of the query plan, and results bufferization;
- **Return** the execution result back to the client.

[Postgres](https://www.postgresql.org/) is assumed for illustration purposes. Its engine roughly follows the sequence
below when a _client_ attempts to execute a query.

1. _Establish connection_. A TCP connection with the client is being authenticated, and a dedicated **backend process**
   to handle requests is initiated by the orchestrator, _postmaster_.
   <br>One backend process corresponds to exactly one client following the "process per user" model. In the context, the
   client is the process which understand the database communication protocol. _Note_: SaaS vendors may provide
   abstraction on top of the TCP and the database protocol. For example, AWS Aurora provides the HTTP interface with a
   Restful API.
   <br>Once a connection is established, the client transmits a query and waits for the server's
   response. _Note_: SaaS vendors may support async execution providing requests orchestration layer.

2. _Parse input_. The query arrived as plain text is deserialized as the list of raw (un-analyzed) parse trees. The
   query _syntactic_ structure is evaluated during the parsing process.
    1. The _lexical analysis_: the query is scanned and tokenized; a _token_ is generated for every found SQL key word.
    2. The _grammar analysis_: the generated tokens are analyzed against the _grammar rules_. Corresponding _action_ is
       applied every time a token matches the rule. The results of actions are stored to the list of trees.

3. _Analyse semantics_. The transformation process takes the raw tree to generate the _query tree_ by
   making system catalog lookups to identify the tables, functions, operators referenced in the request query. The query
   tree and the raw tree are structurally similar, but the query tree contains data types and makes

4. _Re-write_. The query tree is being re-written according to the _rules_ in the system catalog. For example, if the
   query references a view, its underlying query will be embodied explicitly to the tree.

5. _Plan execution_. The query tree can be traversed in a plethora of ways leading to the same result. The
   planner/optimizer assesses possible execution _paths_ in terms of execution _cost_. A path contains the least amount
   of information for the planner to make the decision. Once the "cheapest" path is selected, a complete _plan tree_ is
   generated for executor.
   <br><br>Paths' assessment steps:
    1. _Scan individual relations_ to determine if there are indices on the attributes referenced in the query. The
       sequential scan path is always created by default. On top of that, an execution path to scan index is created for
       every case when the attribute matches the index key. Index scan plans are also generated for indices that have a
       sort ordering matching the "ORDER BY" clause, or a sort ordering that might be useful for merge joining.
    2. _Analyse join conditions_ after all feasible paths have been created for every single relation. If query
       contains "JOIN" clause for two, or more relations, the different strategies (see the table below) are considered
       for every pair.<br>The planner prioritises joins between relations which have filtering conditions in
       the "WHERE" clause. All possible plans are generated for every join pair considered by the planner, and the one
       that is (estimated to be) the cheapest is chosen. Join pairs with the relations not having join clauses are
       considered only with the absense of other choices.
    3. _Assign filtering conditions_ form the WHERE clause to the appropriate nodes of the plan tree.

|        Strategy        | Definition                                                                                                                                                                                                                                                                                                                 | Worse scenario time complexity                                                                                                                                                                                                                                                                                     | 
|:----------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|    Nested loop join    | The right relation is scanned once for every row in the left relation.<br/>The loop is replaced with the index join if the right relation has index on the join keys: a row from the left relation is used as keys for the index scan in the right relation.                                                               | <em>O(n * m)</em>                                                                                                                                                                                                                                                                                                  |
| Mege (sort-merge) join | 1. Joined relations are sorted on join keys;<br/>2. Both relations are scanned in parallel, and matching rows are combined as join rows.                                                                                                                                                                                   | <em>O(n * log(n) + m * log(m))</em> - in general;<br/><em>O(n + m)</em> - if both relations are sorted, or have index on join keys, i.e. the relations are only scanned;<br/><em>O(m + n * log(n))</em> - if the right relation has index and does not require sorting, whilst the left relation has to be sorted. |
|       Hash join        | 1. The hash lookup table is prepared using the right relation;<br/>2. The hash value is calculated for each row of the left relation to match the right relation's rows using the hash table.<br/><b>Note</b>: the right relation must fit in memory, hence the smaller relation shall be used as the right join relation. | <em>O(m + n)</em>                                                                                                                                                                                                                                                                                                  |

**Note**: _n_ and _m_ are the numbers of rows of the left and the right relations respectively.

6. _Execute_ by traversing the plan tree and extracting required sets of rows recursively. Every time a plan's node is
   called, a row is delivered. The executor interacts with the storage system when scanning relations, performs
   query operation and returns the derived rows.
7. The execution results are serialised and sent back to the client. Note that the results can reside in a caching layer
   in case of async execution. The query execution status is being updated in such case, thus the client would be able
   to make a call to retrieve the results using the query execution ID.

_References_:

- [SQL AST](http://ns.inria.fr/ast/sql/index.html)
- [Postgres Documentation](https://www.postgresql.org/)
- [Postgres Codebase](https://github.com/postgres/postgres)

### Execution Plan for Solution Query

|             Query             | Execution time [ms] |
|:-----------------------------:|:--------------------|
|   [Solution](solution.sql)    | 3260.573            |
| [Reference](#reference-query) | 4545.436            |

**Note**: the execution time is assessed by the postgres engine during a single query execution against the table
of 100000 rows. The comparison shall be considered qualitative, rather than quantitative. The database ran on a MacBook
Pro with Apple M1 Pro and 16Gb of RAM using docker.

The postgres engine follows the plan to execute "solution".

```text
GroupAggregate  (cost=32365.64..33060.09 rows=27778 width=68) (actual time=3057.981..3240.512 rows=100000 loops=1)
"  Group Key: a.user_id, a.date, orig.transaction_id"
  CTE transactions_daily
    ->  GroupAggregate  (cost=12976.82..14076.82 rows=10000 width=28) (actual time=83.202..413.281 rows=50916 loops=1)
"          Group Key: transactions.user_id, transactions.date"
          ->  Sort  (cost=12976.82..13226.82 rows=100000 width=36) (actual time=83.182..144.831 rows=100000 loops=1)
"                Sort Key: transactions.user_id, transactions.date"
                Sort Method: external merge  Disk: 4488kB
                ->  Seq Scan on transactions  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.023..12.702 rows=100000 loops=1)
  ->  Sort  (cost=18288.82..18358.27 rows=27778 width=44) (actual time=3057.965..3093.731 rows=448516 loops=1)
"        Sort Key: a.user_id, a.date, orig.transaction_id"
        Sort Method: external sort  Disk: 25376kB
        ->  Merge Left Join  (cost=10588.58..16238.58 rows=27778 width=44) (actual time=674.948..2532.734 rows=448516 loops=1)
              Merge Cond: (a.user_id = b.user_id)
              Join Filter: ((b.date < a.date) AND (b.date >= (a.date - 7)))
              Rows Removed by Join Filter: 4672418
              ->  Sort  (cost=9724.19..9736.69 rows=5000 width=36) (actual time=653.383..664.232 rows=100000 loops=1)
                    Sort Key: a.user_id
                    Sort Method: external merge  Disk: 4488kB
                    ->  Hash Join  (cost=4217.00..9417.00 rows=5000 width=36) (actual time=149.470..623.834 rows=100000 loops=1)
                          Hash Cond: ((a.user_id = orig.user_id) AND (a.date = orig.date))
                          ->  CTE Scan on transactions_daily a  (cost=0.00..200.00 rows=10000 width=20) (actual time=83.205..424.473 rows=50916 loops=1)
                          ->  Hash  (cost=1935.00..1935.00 rows=100000 width=36) (actual time=66.093..66.093 rows=100000 loops=1)
                                Buckets: 65536  Batches: 2  Memory Usage: 3839kB
                                ->  Seq Scan on transactions orig  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.010..8.604 rows=100000 loops=1)
              ->  Sort  (cost=864.39..889.39 rows=10000 width=28) (actual time=21.547..747.962 rows=5115484 loops=1)
                    Sort Key: b.user_id
                    Sort Method: external sort  Disk: 2096kB
                    ->  CTE Scan on transactions_daily b  (cost=0.00..200.00 rows=10000 width=28) (actual time=0.014..3.907 rows=50916 loops=1)
Planning time: 0.409 ms
Execution time: 3260.573 ms
```

#### Reference Query

To compare, let's consider an alternative query below.

```sql
SELECT a.transaction_id
     , a.user_id
     , a.date
     , COALESCE(COUNT(DISTINCT b.transaction_id), 0) AS total
FROM transactions AS a
LEFT JOIN transactions AS b ON b.user_id = a.user_id
                       AND b.date < a.date
                       AND b.date >= a.date - 7
GROUP BY 1, 2, 3
ORDER BY 2, 3
;
```

Its execution plan looks as following.

```text
GroupAggregate  (cost=421742.90..436832.25 rows=100000 width=44) (actual time=3430.284..4522.810 rows=100000 loops=1)
"  Group Key: a.user_id, a.date, a.transaction_id"
  ->  Sort  (cost=421742.90..424560.77 rows=1127148 width=52) (actual time=3430.262..3854.149 rows=1090418 loops=1)
"        Sort Key: a.user_id, a.date, a.transaction_id"
        Sort Method: external merge  Disk: 66016kB
        ->  Hash Left Join  (cost=3967.00..231384.60 rows=1127148 width=52) (actual time=41.550..2083.095 rows=1090418 loops=1)
              Hash Cond: (a.user_id = b.user_id)
              Join Filter: ((b.date >= (a.date - 7)) AND (b.date <= (a.date - 1)))
              Rows Removed by Join Filter: 9014672
              ->  Seq Scan on transactions a  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.041..59.053 rows=100000 loops=1)
              ->  Hash  (cost=1935.00..1935.00 rows=100000 width=36) (actual time=40.930..40.930 rows=100000 loops=1)
                    Buckets: 65536  Batches: 4  Memory Usage: 2188kB
                    ->  Seq Scan on transactions b  (cost=0.00..1935.00 rows=100000 width=36) (actual time=0.003..15.785 rows=100000 loops=1)
Planning time: 0.441 ms
Execution time: 4545.436 ms
```
