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

---

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

- Import a grammar matching your system: 
```scala
import com.github.majyphi.wareflow.grammars.CommonSQLGrammar._
```
- Create a connection to your DataWarehouse : 
```scala
implicit val connection: Connection = DriverManager.getConnection(...)
```

Now pipelines can be executed by calling `.run` or `.show`

## Extending the framework by providing your own grammar

- You can create your own grammar by extending the trait `Grammar`, and overriding the method `treeToSQL`
  For simplicity, you can extend `CommonSQLGrammar` and override what is needed
- You can create custom SQL expressions by extending `Expression` and add entries in your implementation of `evalExpression` in your grammar. 


## Structure of the repo
- `com.github.majestic.dpl.core`: The structure of the API for the developer. It should be as universal as possible
- `com.github.majestic.dpl.grammars`: The conversion code to translate the Pipeline Tree to SQL
- `com.github.majestic.dpl.runtime`: Run enable to execute the pipeline and show the results
- Tests contain just the basics to check the structure is consistent and runnable. They are ran against an In-memory DuckDB instance.

