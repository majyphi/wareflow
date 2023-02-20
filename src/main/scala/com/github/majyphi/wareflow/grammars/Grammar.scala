package com.github.majyphi.wareflow.grammars

import com.github.majyphi.wareflow.core._

trait Grammar {

  implicit val instance : Grammar = this //For implicit import

  def treeToSQL(tree : Dataframe) : String

}

trait CommonSQLGrammar extends Grammar {

  private val cteLeftName = "cte_left"
  private val cteRightName = "cte_right"

  def evalExpresion(c: Expression): String = c match {
    case alias(e,_)                             => evalExpresion(e)
    case *()                                    => "*"
    case col(name)                              => s"\"$name\""
    case lit(value)                             => s"'$value'"
    case addition(left, right)                  => s"${evalName(left)} + ${evalName(right)}"
    case subtraction(left, right)               => s"${evalName(left)} - ${evalName(right)}"
    case product(left, right)                   => s"${evalName(left)}*${evalName(right)}"
    case equality(left, right)                  => s"${evalName(left)} == ${evalName(right)}"
    case maximum(e)                             => s"max(${evalName(e)})"
    case minimum(e)                             => s"min(${evalName(e)})"
    case rank()                                 => s"rank()"
    case windowAggregation(e, window)           => s"${evalExpresion(e)} over (${evalWindow(window)})"
    case _                                      => throw GrammarException("Unimplemented Column Expression: " + c.getClass.toString)
  }

  def evalSelectExpression(c: Expression): String = c match {
    case alias(col,name)              => s"${evalExpresion(col)} as $name"
    case c : Expression               => evalExpresion(c)
  }

  def evalName(e: Expression): String = e match {
    case alias(_,name)                => name
    case left(col)                    => s"$cteLeftName.${evalName(col)}"
    case right(col)                   => s"$cteRightName.${evalName(col)}"
    case c : Expression               => evalExpresion(c)
  }

  def evalWindow(w: Window): String =
    s"""
       |${if (w.partitions.nonEmpty) "PARTITION BY" else ""} ${w.partitions.map(evalName).mkString(", ")}
       |${if (w.orderBy.nonEmpty) "ORDER BY" else ""} ${w.orderBy.map(evalOrder).mkString(", ")}
       |""".stripMargin

  def evalOrder(o: Order): String = evalName(o.column) + " " + o.direction.toString

  override def treeToSQL(tree: Dataframe): String =
    tree match {
      case df: Aggregation            => aggregation(df)
      case df: Join                   => join(df)
      case df: Projection             => projection(df)
      case df: Table                  => source(df)
      case df                         => throw GrammarException("Unimplemented Processing Stage type: " + df.getClass.toString)
    }

  def source(df: Table): String = s"SELECT * FROM ${df.name}"

  def projection(df: Projection): String = {
    s"""with cte_select as (
       |    with cte as (
       |       ${treeToSQL(df.source)}
       |    )
       |  SELECT ${df.columns.map(evalSelectExpression).mkString(",\n")}
       |  FROM cte
       |)
       |SELECT * FROM cte_select
       |${if (df.filters.nonEmpty) "WHERE" else ""} ${df.filters.map(evalExpresion).mkString("\nAND")}
       |""".stripMargin
  }

  def aggregation(df: Aggregation): String = {
    s"""with cte as (
       |  ${treeToSQL(df.source)}
       |)
       |SELECT
       |${(df.keyColumns ++ df.aggColumns).map(evalSelectExpression).mkString(",\n")}
       |FROM cte
       |GROUP BY ${df.keyColumns.map(evalName).mkString(",")}
       |""".stripMargin
  }


  def join(df: Join): String = {

    s"""with cte_left as (
       |  ${treeToSQL(df.sourceLeft)}
       |),
       |cte_right as (
       |  ${treeToSQL(df.sourceRight)}
       |)
       |SELECT * FROM cte_left ${df.joinType} cte_right
       |ON (${df.joinConditions.map(evalExpresion).mkString("\nAND")})
       |""".stripMargin
  }


}



