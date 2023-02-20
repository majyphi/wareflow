package com.github.majyphi.wareflow

import com.github.majyphi.wareflow.core.Helpers._
import com.github.majyphi.wareflow.core._
import com.github.majyphi.wareflow.grammars.DuckDBGrammar._
import com.github.majyphi.wareflow.runtime.QueryExecution
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, DriverManager}

class ProcessingTreeTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {

  implicit val connection: Connection = DriverManager.getConnection("jdbc:duckdb:")

  override def beforeAll() = {
    QueryExecution.run("CREATE TABLE sales (id VARCHAR, amount INT)")
    QueryExecution.run("INSERT INTO sales values ('product1', 1), ('product2', 2), ('product2', 4)")

    QueryExecution.run("CREATE TABLE prices (id VARCHAR, price INT)")
    QueryExecution.run("INSERT INTO prices values ('product1', 2), ('product2', 3), ('product2', 4)")
  }

  "SQL query building on a projection" should "return a runnable select+filter query" in {
    val result = t"sales"
      .select(c"id")
      .filter(c"id" == lit("product1"))
      .run

    result shouldBe a(Symbol("Success"))
    result.get.head should contain theSameElementsAs Seq("product1")

  }

  "SQL query building on an aggregation" should "return a runnable aggregating query" in {
    val result = t"sales"
      .groupBy(c"id")
      .agg(c"amount".max)
      .run

    result shouldBe a(Symbol("Success"))
    result.get should contain theSameElementsAs Seq(Seq("product2", "4"), Seq("product1", "1"))

  }

  "SQL query building on an join" should "return a runnable join query" in {
    val result = t"sales"
      .leftJoin(t"prices")
      .on(left(c"id") == right(c"id"))
      .select(c"id", c"amount", c"price")
      .run

    result shouldBe a(Symbol("Success"))
    result.get should contain theSameElementsAs Seq(
      Seq("product2", "4", "3"),
      Seq("product2", "2", "3"),
      Seq("product2", "4", "4"),
      Seq("product2", "2", "4"),
      Seq("product1", "1", "2")
    )

  }

  "SQL query building on a Window Aggregation" should "return a runnable window query" in {
    val window = Window.partitionBy(c"id").orderBy(c"price".desc)
    val topPrices = t"prices"
      .withColumn(rank().over(window).as("rank"))
      .filter(c"rank" == lit("1"))

    val sales = t"sales"

    val joinResult = sales.leftJoin(topPrices)
      .on(left(c"id") == right(c"id"))
      .groupBy(c"id")
      .agg((c"amount" * c"price").max)

      joinResult.printQuery
      joinResult.show
  }

}
