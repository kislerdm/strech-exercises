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

**Note**: the docker compose v2 is used, i.e. `docker compose` instead of `docker-compose` command.

Run tests by executing the command:

```commandline
make tests
```

Expected output:

```commandline
Run tests
INFO:  TEST: optimised query results must match the reference. <PASS>
 test 
------
 
(1 row)
```

## The Query Execution Process

Although query execution details depend on the type and version of database engine, all relational databases operate in
the request-response communication model:   

- Establishment of a client **connection**;
- Deserialization of the request query into the **raw** [**AST**](http://ns.inria.fr/ast/sql/index.html);
- Generation of **query AST** by mapping the raw AST onto underlying database objects referenced in the request query;
- Generation of **query plan** by selecting optimal strategy to traverse the query AST;
- **Execution** of the query plan, and results bufferization;
- **Return** the execution result back to the client.

[Postgres](https://www.postgresql.org/) is assumed for illustration purposes. Its engine roughly follows the sequence
when a _client_ attempts to execute a query.

1. _Connection stage_. A TCP connection with the client is being authenticated, and a dedicated **backend process** to
   handle requests is initiated by the orchestrator, _postmaster_.
   <br>One backend process corresponds to exactly one client following the "process per user" model. In the context, the
   client is the process which understand the database communication protocol. _Note_: SaaS vendors may provide
   abstraction on top of the TCP and the database protocol. For example, AWS Aurora provides the HTTP interface with a
   Restful API. <br>Once a connection is established, the client transmits a query and waits for the server's
   response. _Note_: SaaS vendors may support async execution providing requests orchestration layer.

2. _Parser stage_. The backend process performs lexical and grammatical analysis, and validation of the query arrived as
   plain text. It returns an error to the client if validation fails; it passes the _parse tree_ to the next step of the
   parser stage otherwise.
    1. The query is scanned and tokenized. A _token_ is generated for every found _SQL key word_.
    2. Generated tokens are analyzed against the _grammar rules_. Corresponding _action_ is applied every time a token
       matches the rule. The results of actions are stored to the list of "raw" trees.
3.

## References

- [SQL AST](http://ns.inria.fr/ast/sql/index.html)
- [Postgres Documentation](https://www.postgresql.org/)
- [Postgres Codebase](https://github.com/postgres/postgres)
