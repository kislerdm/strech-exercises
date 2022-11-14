# ML Feature Table Computation

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

## The Query Execution Process

Although query execution details depend on the type and version of database engine, all relational databases operate in
the request-response communication model:

- Establishment of a client **connection**;
- Deserialization of the request query into the **raw** [**AST**](http://ns.inria.fr/ast/sql/index.html);
- Generation of **query AST** by mapping the raw AST onto underlying database objects referenced in the request query;
- Generation of **query plan** by selecting optimal strategy to execute the query;
- **Execution** of the query plan, and results bufferization;
- **Return** the execution result back to the client.

[Postgres](https://www.postgresql.org/) is assumed for illustration purposes. Its engine roughly follows the sequence
when a _client_ attempts to execute a query.

1. _Establish connection_. A TCP connection with the client is being authenticated, and a dedicated **backend process**
   to
   handle requests is initiated by the orchestrator, _postmaster_.
   <br>One backend process corresponds to exactly one client following the "process per user" model. In the context, the
   client is the process which understand the database communication protocol. _Note_: SaaS vendors may provide
   abstraction on top of the TCP and the database protocol. For example, AWS Aurora provides the HTTP interface with a
   Restful API. <br>Once a connection is established, the client transmits a query and waits for the server's
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

## References

- [SQL AST](http://ns.inria.fr/ast/sql/index.html)
- [Postgres Documentation](https://www.postgresql.org/)
- [Postgres Codebase](https://github.com/postgres/postgres)
