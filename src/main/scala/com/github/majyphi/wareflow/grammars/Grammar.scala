package com.github.majyphi.wareflow.grammars

import com.github.majyphi.wareflow.core.{Aggregation, Column, Dataframe, Join, Projection, Table, addition, alias, col, equality, left, lit, maximum, product, right, subtraction}

trait Grammar {

  implicit val instance : Grammar = this //For implicit import

  def treeToSQL(tree : Dataframe) : String

  def evalColumnExpresion(col : Column) : String
  def evalSelectExpression(col : Column) : String
  def evalName(col : Column) : String

}

trait CommonSQLGrammar extends Grammar {

  val cteLeftName = "cte_left"
  val cteRightName = "cte_right"

  override def evalColumnExpresion(c: Column): String = c match {
    case alias(col,_) => evalColumnExpresion(col)
    case col(name) => s"\"$name\""
    case lit(value) => s"'$value'"
    case addition(left, right) => s"${evalName(left)} + ${evalName(right)}"
    case subtraction(left, right) => s"${evalName(left)} - ${evalName(right)}"
    case product(left, right) => s"${evalName(left)}*${evalName(right)}"
    case equality(left, right) => s"${evalName(left)} == ${evalName(right)}"
    case maximum(col) => s"max(${evalName(col)})"
    case _ => throw GrammarException("Unknown Column Expression: " + c.getClass.toString)
  }

  override def evalSelectExpression(c: Column): String = c match {
    case alias(col,name) => s"${evalColumnExpresion(col)} as $name"
    case c : Column => evalColumnExpresion(c)
  }

  override def evalName(c: Column): String = c match {
    case alias(_,name) => name
    case left(col) => s"$cteLeftName.${evalName(col)}"
    case right(col) => s"$cteRightName.${evalName(col)}"
    case c : Column => evalColumnExpresion(c)
  }

  override def treeToSQL(tree: Dataframe): String =
    tree match {
      case df: Aggregation => aggregation(df)
      case df: Join => join(df)
      case df: Projection => projection(df)
      case df: Table => source(df)
      case df => throw GrammarException("Unknown Processing Stage type: " + df.getClass.toString)
    }

  def source(df: Table): String = s"SELECT * FROM ${df.name}"

  def projection(df: Projection): String = {
    s"""  with cte as (
       |    ${treeToSQL(df.source)}
       |  )
       |  SELECT ${df.columns.map(evalSelectExpression).mkString(",\n")}
       |  FROM cte
       |  ${if (df.filters.nonEmpty) "WHERE" else ""} ${df.filters.map(evalColumnExpresion).mkString("\nAND")}
       |""".stripMargin
  }

  def aggregation(df: Aggregation): String = {
    s"""  with cte as (
       |    ${treeToSQL(df.source)}
       |  )
       |  SELECT
       |  ${(df.keyColumns ++ df.aggColumns).map(evalSelectExpression).mkString(",\n")}
       |  FROM cte
       |  GROUP BY ${df.keyColumns.map(evalName).mkString(",")}
       |""".stripMargin
  }


  def join(df: Join): String = {

    s"""  with cte_left as (
       |    ${treeToSQL(df.sourceLeft)}
       |  ),
       |  cte_right as (
       |    ${treeToSQL(df.sourceRight)}
       |  )
       |  SELECT
       |  *
       |  FROM cte_left
       |  ${df.joinType} cte_right on (${df.joinConditions.map(evalColumnExpresion).mkString("\nAND")})
       |""".stripMargin
  }


}



