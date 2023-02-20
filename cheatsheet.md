
### Referencing a table
`Table("prices")` 
`t"prices"`


### Referencing a column/field
`col("product_id")`
`c"product_id"`


### Create a literal (constant value)
`lit("my_constant_value")`

---

### Data for examples:

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
│product2                               │3                                     │
│product1                               │2                                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
```
---

### Selections and filtering

<table>
<tr>
<th>Operation</th>
<th>Wareflow</th>
<th>SQL</th>
</tr>
<tr>
<td>Select all columns</td>
<td>

```scala
t"prices"
  .show
```

</td>
<td>

```sql
SELECT * FROM prices
```

</td>
</tr>
<tr>
<td>Add a column</td>
<td>

```scala
t"prices"
  .withColumn(lit("a value").as("my_constant"))
  .show
```

</td>
<td>

```sql
SELECT
    *,
    'a value' as my_constant
FROM prices
```

</td>
</tr>
<tr>
<td>Select a specific set of columns</td>
<td>

```scala
t"prices"
    .select(c"product_id", c"price")
    .show
```

</td>
<td>

```sql
SELECT
    product_id,
    price
FROM prices
```

</td>
</tr>
<tr>
<td>Filter/Where</td>
<td>

```scala
t"prices"
    .filter(c"value" > lit(0))
    .show
```

</td>
<td>

```sql
SELECT *
FROM prices
WHERE value > 0
```

</td>
</tr>
</table>

### Aggregations

<table>
<tr>
<th>Operation</th>
<th>Wareflow</th>
<th>SQL</th>
</tr>
<tr>
<td>GroupBy + Aggregation</td>
<td>

```scala
t"prices"
    .groupBy(c"product_id")
    .agg(c"price".max)
    .show
```

</td>
<td>

```sql
SELECT
    product_id,
    max(price)
FROM prices
GROUP BY product_id
```

</td>
</tr>
<tr>
<td>Windows</td>
<td>

```scala
val deduplicationWindow = Window
  .partitionBy(c"product_id")
  .orderBy(c"amount".asc)

t"prices"
    .withColumn(
      rank()
        .over(deduplicationWindow)
        .as("rank")
    )
    .filter(c"rank" == lit("1"))
    .show
```

</td>
<td>

```sql
SELECT
    product_id,
    max(price)
FROM prices
GROUP BY product_id
```

</td>
</tr>
</table>

### Join

<table>
<tr>
<th>Operation</th>
<th>Wareflow</th>
<th>SQL</th>
</tr>
<tr>
<td>Left Join</td>
<td>

```scala
t"sales"
    .leftJoin(t"prices")
    .on(left(c"product_id") == right(c"product_id"))
    .show
```

</td>
<td>

```sql
SELECT
    *
FROM sales LEFT JOIN prices
ON sales.product_id == prices.product_id
```

</td>
</tr>
</table>

