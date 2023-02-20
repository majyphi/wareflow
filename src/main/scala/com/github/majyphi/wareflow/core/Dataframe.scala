package com.github.majyphi.wareflow.core

import com.github.majyphi.wareflow.grammars.Grammar
import com.github.majyphi.wareflow.runtime.QueryExecution

import java.sql.Connection
import scala.util.Try

trait Dataframe {

  def withColumn(column: Expression) : Projection = Projection(source = this).withColumn(column)

  def select(columns: Expression*) = Projection(columns, Seq(), this)

  def filter(filter: Expression): Projection = Projection(filters  = Seq(filter), source =  this)

  def groupBy(keyColumns: Expression*) = GroupedDataframe(keyColumns, source = this)

  def leftJoin(dataframe: Dataframe) = PreJoin(this, dataframe, JoinType.LeftJoin)
  def rightJoin(dataframe: Dataframe) = PreJoin(this, dataframe, JoinType.RightJoin)
  def innerJoin(dataframe: Dataframe) = PreJoin(this, dataframe, JoinType.InnerJoin)
  def fullJoin(dataframe: Dataframe) = PreJoin(this, dataframe, JoinType.FullJoin)
  def crossJoin(dataframe: Dataframe) = PreJoin(this, dataframe, JoinType.CrossJoin)

  def printQuery(implicit grammar: Grammar): Unit = {
    println(grammar.treeToSQL(this))
  }

  def show(implicit grammar : Grammar, connection : Connection) : Unit = {
    val sqlQuery = grammar.treeToSQL(this)
    QueryExecution.show(sqlQuery)
  }

  def run(implicit grammar : Grammar, connection : Connection) : Try[Seq[Seq[String]]] = {
    val sqlQuery = grammar.treeToSQL(this)
    QueryExecution.run(sqlQuery)
  }

}

case class Table(name: String) extends Dataframe

case class Projection(columns: Seq[Expression] = Seq(*()), filters: Seq[Expression] = Seq(), source: Dataframe) extends Dataframe {
  override def withColumn(column: Expression): Projection = this.copy(columns = columns ++ Seq(column))
  override def filter(filter: Expression): Projection = this.copy(filters = filters ++ Seq(filter))
}

case class GroupedDataframe(keyColumns: Seq[Expression], aggColumns: Seq[Expression] = Seq(), source: Dataframe) {
  def agg(column: Expression) = Aggregation(keyColumns, Seq(column), source)
}

case class Aggregation(keyColumns: Seq[Expression], aggColumns: Seq[Expression] = Seq(), source: Dataframe) extends Dataframe {
  def agg(column: Expression) = this.copy(aggColumns = aggColumns ++ Seq(column))
}

case class PreJoin(sourceLeft: Dataframe, sourceRight: Dataframe, joinType: JoinType) {
  def on(joinCondition: Expression) = Join(sourceLeft,sourceRight, joinType, Seq(joinCondition))
}

case class Join(sourceLeft: Dataframe, sourceRight: Dataframe, joinType: JoinType, joinConditions: Seq[Expression] = Seq()) extends Dataframe {
  def on(joinCondition: Expression) = Join(sourceLeft,sourceRight, joinType, joinConditions ++ Seq(joinCondition))
}

abstract class JoinType(value: String) {
  override def toString: String = value
}

object JoinType {

  case class DefinedJoinType(value: String) extends JoinType(value)

  val LeftJoin = DefinedJoinType("LEFT JOIN")
  val LeftOuterJoin = LeftJoin
  val RightJoin = DefinedJoinType("RIGHT JOIN")
  val RightOuterJoin = RightJoin
  val InnerJoin = DefinedJoinType("INNER JOIN")
  val FullJoin = DefinedJoinType("FULL JOIN")
  val CrossJoin = DefinedJoinType("CROSS JOIN")

}


