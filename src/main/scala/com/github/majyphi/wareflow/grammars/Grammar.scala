package com.github.majyphi.wareflow.grammars

import com.github.majyphi.wareflow.core._

trait Grammar {

  implicit val instance : Grammar = this //For implicit import

  def evalTree(tree : Dataframe) : String

  def evalExpression[T <: Expression](c : T) : String

}

trait CommonSQLGrammar extends Grammar {

  private val cteLeftName = "cte_left"
  private val cteRightName = "cte_right"

  override def evalExpression[E <: Expression](e: E): String = e match {
    case alias(e,_)                             => evalExpression(e)
    case *()                                    => "*"
    case col(name)                              => s"\"$name\""
    case lit(value)                             => evalLiteralValue(value)
    case addition(left, right)                  => s"${evalName(left)} + ${evalName(right)}"
    case subtraction(left, right)               => s"${evalName(left)} - ${evalName(right)}"
    case product(left, right)                   => s"${evalName(left)}*${evalName(right)}"
    case equality(left, right)                  => s"${evalName(left)} == ${evalName(right)}"
    case maximum(e)                             => s"max(${evalName(e)})"
    case minimum(e)                             => s"min(${evalName(e)})"
    case rank()                                 => s"rank()"
    case windowAggregation(e, window)           => s"${evalExpression(e)} over (${evalWindow(window)})"
    case _                                      => throw GrammarException("Unimplemented Column Expression: " + e.getClass.toString)
  }

  def evalLiteralValue(value : Any) : String = value match {
    case string : String => s"'$string'"
    case int : Int => int.toString
    case double : Double => double.toString
    case _ => throw GrammarException("Unimplemented type for literal: " + value.getClass.toString)
  }

  def evalSelectExpression[E <: Expression](e: E): String = e match {
    case alias(col,name)              => s"${evalExpression(col)} as $name"
    case c : Expression               => evalExpression(c)
  }

  def evalName[E <: Expression](e: E): String = e match {
    case alias(_,name)                => name
    case left(col)                    => s"$cteLeftName.${evalName(col)}"
    case right(col)                   => s"$cteRightName.${evalName(col)}"
    case c : Expression               => evalExpression(c)
  }

  def evalWindow[W <: Window](w: W): String =
    s"""
       |${if (w.partitions.nonEmpty) "PARTITION BY" else ""} ${w.partitions.map(evalName).mkString(", ")}
       |${if (w.orderBy.nonEmpty) "ORDER BY" else ""} ${w.orderBy.map(evalOrder).mkString(", ")}
       |""".stripMargin

  def evalOrder[O <: Order](o: O): String = evalName(o.column) + " " + o.direction.toString

  override def evalTree(tree: Dataframe): String =
    tree match {
      case df: Aggregation            => evalAggregation(df)
      case df: Join                   => evalJoin(df)
      case df: Projection             => evalProjection(df)
      case df: Table                  => evalSource(df)
      case df                         => throw GrammarException("Unimplemented Processing Stage type: " + df.getClass.toString)
    }

  def evalSource(df: Table): String = s"SELECT * FROM ${df.name}"

  def evalProjection(df: Projection): String = {
    s"""with cte_select as (
       |    with cte as (
       |       ${evalTree(df.source)}
       |    )
       |  SELECT ${df.columns.map(evalSelectExpression).mkString(",\n")}
       |  FROM cte
       |)
       |SELECT * FROM cte_select
       |${if (df.filters.nonEmpty) "WHERE" else ""} ${df.filters.map(evalExpression).mkString("\nAND")}
       |""".stripMargin
  }

  def evalAggregation(df: Aggregation): String = {
    s"""with cte as (
       |  ${evalTree(df.source)}
       |)
       |SELECT
       |${(df.keyColumns ++ df.aggColumns).map(evalSelectExpression).mkString(",\n")}
       |FROM cte
       |GROUP BY ${df.keyColumns.map(evalName).mkString(",")}
       |""".stripMargin
  }


  def evalJoin(df: Join): String = {

    s"""with cte_left as (
       |  ${evalTree(df.sourceLeft)}
       |),
       |cte_right as (
       |  ${evalTree(df.sourceRight)}
       |)
       |SELECT * FROM cte_left ${df.joinType} cte_right
       |ON (${df.joinConditions.map(evalExpression).mkString("\nAND")})
       |""".stripMargin
  }


}



