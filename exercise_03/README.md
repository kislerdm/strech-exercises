# [DEFECT] Dimension Deduplication

<!-- TOC -->
* [Problem](#problem)
  * [Example](#example)
* [Expected Behaviour](#expected-behaviour)
* [Proposed Solution](#proposed-solution)
* [Acceptance Criteria](#acceptance-criteria)
* [Solution](#solution)
    * [Tests](#tests)
* [The Logic](#the-logic)
<!-- TOC -->

## Problem

As a data engineer, I want to fix the data quality problem caused by a faulty ETL process which led to redundant records
in the table `dim_dep_agreement`.

### Example

| sk  | agrmnt_id | actual_from_dt | actual_to_dt | client_id | product_id | interest_rate |
|:---:|:---------:|:--------------:|:------------:|:---------:|:----------:|:-------------:|
|  1  |    101    |   2015-01-01   |  2015-02-20  |    20     |    305     |     3.5%      |
|  2  |    101    |   2015-02-21   |  2015-05-17  |    20     |    345     |      4%       |
|  3  |    101    |   2015-05-18   |  2015-07-05  |    20     |    345     |      4%       |
|  4  |    101    |   2015-07-06   |  2015-08-22  |    20     |    539     |      6%       |
|  5  |    101    |   2015-08-23   |  9999-12-31  |    20     |    345     |      4%       |
|  6  |    102    |   2016-01-01   |  2016-06-30  |    25     |    333     |     3.7%      |
|  7  |    102    |   2016-07-01   |  2016-07-25  |    25     |    333     |     3.7%      |
|  8  |    102    |   2016-07-26   |  2016-09-15  |    25     |    333     |     3.7%      |
|  9  |    102    |   2016-09-16   |  9999-12-31  |    25     |    560     |     5.9%      |
| 10  |    103    |   2011-05-22   |  9999-12-31  |    30     |    560     |      2%       |

In the above sample, a redundancy can be seen within the records 2, 3 and 6, 7, 8.

## Expected Behaviour

New row in the table is created if at least one out of three business attributes (`client_id`, `product_id`
, `interest_rate`) changed for a given agreement (`agrmnt_id`).

## Proposed Solution

Prepare a SQL script to create new table `dim_dep_agreement_compacted` with redundant records "collapsed".
For example, instead of the rows 2-3, there should be a single row with the period from 2015-02-21 (`actual_from_dt`)
to 2015-07-05 (`actual_to_dt`).

## Acceptance Criteria

- The SQL query is designed as a single statement, i.e. without using intermediate/temporary tables, updates or deletes.
- The table `dim_dep_agreement_compacted` should have "smooth history" for every agreement (`agrmnt_id`), i.e. no gaps
  or intersections for the validity intervals (from `actual_from_dt` to `actual_to_dt`).
- The query logic is described and documented.

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

## The Logic

The solution follows the steps:

1. Identify the rows with redundant records - the CTE `deduplication_ranges`.
    1. Define if the given row's combination (`client_id`, `product_id`, `interest_rate`) differs from the corresponding
       combination from the _following_ row;
    2. Define if the given row's combination (`client_id`, `product_id`, `interest_rate`) differs from the corresponding
       combination from the _preceding_ row.
2. Subset the nonduplicated rows to keep them aside from deduplication - the CTE `deduplication_trivial`;
3. Identify the beginning (`actual_from_dt`) of the ranges with duplicates - the CTE `duplication_ranges_left`
4. Identify the end (`actual_to_dt`) of the ranges with duplicates - the CTE `duplication_ranges_right`
5. "Collapse" the rows with duplicates by combining the beginning and the end of respective ranges. Unite the result
   with the nonduplicated rows. See the CTE `deduplication_result`.
6. Add the incremental ID column `sk` defined as the row number - the final "SELECT" statement.
