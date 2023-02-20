# WareFlow

## A DSL to write data flows for Data Warehouses

The main idea is to write typical DataFrame-style pipelines, compile them to SQL queries and run them on systems that only have SQL endpoints.

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

with Wareflow we would write:

```scala
    t"sales"
      .leftJoin(t"prices")
      .on(left(c"id") == right(c"id"))
      .groupBy(c"id")
      .agg((c"amount"*c"price").max.as("best_sale_value"))
      .show
```

For a complete guide on syntax: [Wareflow Cheat Sheet](cheatsheet.md)

## Quick Start

### Pre-requisites
- Scala 2.13
- SBT

### Installation
The project is not published on any repository yet. Therefore it requires local installation:

- Clone the repository
- Run `sbt publish-local`
- Import in your project with: `libraryDependencies += com.github.majestic" %% "dpl" % "0.1.0-SNAPSHOT"`

### Syntax

Complete syntax can be found in: [Wareflow Cheat Sheet](cheatsheet.md)

### Executing your pipeline

- Import a grammar matching your system: `import com.github.majyphi.wareflow.grammars.CommonSQLGrammar._`
- Create a connection to your DataWarehouse : `implicit val connection: Connection = DriverManager.getConnection(...)`

Now pipelines can be executed by calling `.run`, `.show`

### Extending the framework by providing your own grammar

- You can create your own grammar by extending the trait `Grammar`, and overriding the method `treeToSQL`
  For simplicity, you can extend `CommonSQLGrammar` and override what is needed
- You can create custom SQL expressions by extending `Expression` and add entries in your implementation of `evalExpression` in your grammar. 


## Structure of the repo
- com.github.majestic.dpl.core contains the structure of the API for the developer. It should be as universal as possible
- com.github.majestic.dpl.grammars contains the the necessary code to convert the structure to SQL code. There is a `CommonSQLGrammar` the aims to be broad and compatible to most systems, however as SQL is not a standard language, other grammars can be introduced by extending and overriding this common one.
- com.github.majestic.dpl.runtime contains the code to run, get the results and show them on an arbitrary JDBC connection.
- Tests contain just the basics to check the structure is consistent and runnable. They are run against an In-memory DuckDB instance.

