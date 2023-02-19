# WareFlow

## A DSL to write data flows for Data Warehouses

The main idea is to write typical Apache Spark or Flink-like pipelines, compile them to SQL queries and run them on systems that only have SQL endpoints.

## Example:

Given the following tables on our system:
```
Amount of Sales
┌───────────────────────────────────────┬──────────────────────────────────────┐
│id                                     │amount                                │
├───────────────────────────────────────┼──────────────────────────────────────┤
│product2                               │4                                     │
│product2                               │2                                     │
│product1                               │1                                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
Product Prices
┌───────────────────────────────────────┬──────────────────────────────────────┐
│id                                     │price                                 │
├───────────────────────────────────────┼──────────────────────────────────────┤
│product2                               │4                                     │
│product1                               │2                                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
```

If we want the best sale that occurred we would write with SQL:
```SQL
SELECT 
    id,
    max(amount*price)
FROM sales 
left join prices on sales.id == prices.id
GROUP BY id
```

with DPL we would write:

```scala
    t"sales"
      .leftJoin(t"prices")
      .on(left(c"id") == right(c"id"))
      .groupBy(c"id")
      .agg((c"amount"*c"price").max.as("best_sale_value"))
      .show
```



## Quick Start

### Pre-requisites
- Scala 2.13
- SBT

### Installation
- Clone the repository
- Run `sbt publish-local`
- Import in your project with: `libraryDependencies += com.github.majestic" %% "dpl" % "0.1.0-SNAPSHOT"`

### Write a pipeline

- Source tables can be declared with:
  - `t"my_source_table"`
  - or `Source("my_source_table")`
    Then transformations on a table can be written with `withColumn()`, `filter()`, `groupBy()`, `join()` etc...
- Existing fields can be referenced with:
  - `c"my_field"`
  - or `col("my_field")`
    Then common operations can be applied on this field with: `==`, `*`, `+`, `-` etc...
- Write a literal (constant value): `lit("constantValue")`
- Define an alias for a calculated column: `myColumn.as("name_of_my_column")`


## Structure of the repo
- com.github.majestic.dpl.core contains the structure of the API for the developer. It should be as universal as possible
- com.github.majestic.dpl.grammars contains the the necessary code to convert the structure to SQL code. There is a `CommonSQLGrammar` the aims to be broad and compatible to most systems, however as SQL is not a standard language, other grammars can be introduced by extending and overriding this common one.
- com.github.majestic.dpl.runtime contains the code to run, get the results and show them on an arbitrary JDBC connection.
- Tests contain just the basics to check the structure is consistent and runnable. They are run against an In-memory DuckDB instance.

