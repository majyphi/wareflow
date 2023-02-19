package com.github.majyphi.wareflow.core

import com.github.majyphi.wareflow.grammars.Grammar
import com.github.majyphi.wareflow.runtime.QueryExecution

import java.sql.Connection
import scala.util.Try

trait Dataframe {

  def withColumn(column: Column) = Projection(Seq(column), Seq(), this)

  def select(columns: Column*) = Projection(columns, Seq(), this)

  def filter(filter: Column): Projection = Projection(filters  = Seq(filter), source =  this)

  def groupBy(keyColumns: Column*) = GroupedDataframe(keyColumns, source = this)

  def leftJoin(dataframe: Dataframe, joinType: JoinType = JoinType.LeftJoin) = PreJoin(this, dataframe, joinType)
  def rightJoin(dataframe: Dataframe, joinType: JoinType = JoinType.RightJoin) = PreJoin(this, dataframe, joinType)
  def innerJoin(dataframe: Dataframe, joinType: JoinType = JoinType.InnerJoin) = PreJoin(this, dataframe, joinType)
  def fullJoin(dataframe: Dataframe, joinType: JoinType = JoinType.FullJoin) = PreJoin(this, dataframe, joinType)
  def crossJoin(dataframe: Dataframe, joinType: JoinType = JoinType.CrossJoin) = PreJoin(this, dataframe, joinType)

  def show(implicit grammar : Grammar, connection : Connection) : Unit = {
    val sqlQuery = grammar.treeToSQL(this)
    QueryExecution.show(sqlQuery)
  }
  def run(implicit grammar : Grammar, connection : Connection) : Try[Seq[Seq[String]]] = {
    val sqlQuery = grammar.treeToSQL(this)
    QueryExecution.run(sqlQuery)
  }
}

object Dataframe {
  implicit class StringToTableHelper(val sc: StringContext) {
    def t(args: Any*): Table = {
      Table(sc.s(args: _*))
    }
  }
}

case class Table(name: String) extends Dataframe

case class Projection(columns: Seq[Column] = Seq(col("*")), filters: Seq[Column] = Seq(), source: Dataframe) extends Dataframe {

  override def withColumn(column: Column): Projection = this.copy(columns = columns ++ Seq(column))

  override def filter(filter: Column): Projection = this.copy(filters = filters ++ Seq(filter))

}

case class GroupedDataframe(keyColumns: Seq[Column], aggColumns: Seq[Column] = Seq(), source: Dataframe) {
  def agg(column: Column) = Aggregation(keyColumns, Seq(column), source)
}

case class Aggregation(keyColumns: Seq[Column], aggColumns: Seq[Column] = Seq(), source: Dataframe) extends Dataframe {

  def agg(column: Column) = this.copy(aggColumns = aggColumns ++ Seq(column))

}

case class PreJoin(sourceLeft: Dataframe, sourceRight: Dataframe, joinType: JoinType) {
  def on(joinCondition: Column) = Join(sourceLeft,sourceRight, joinType, Seq(joinCondition))
}

case class Join(sourceLeft: Dataframe, sourceRight: Dataframe, joinType: JoinType, joinConditions: Seq[Column] = Seq()) extends Dataframe {

  def on(joinCondition: Column) = Join(sourceLeft,sourceRight, joinType, joinConditions ++ Seq(joinCondition))

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


