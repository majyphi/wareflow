package com.github.majyphi.wareflow.grammars.duckdb

import com.github.majyphi.wareflow.core.Expression
import com.github.majyphi.wareflow.grammars.CommonSQLGrammar

object DuckDBGrammar extends CommonSQLGrammar {

  trait DuckDBExpression extends Expression

  case class array_extract(array : Expression, position : Expression) extends DuckDBExpression

  override def evalExpression[T <: Expression](c: T): String = c match {
    case array_extract(array,position) => s"array_extract(${evalExpression(array)},${evalExpression(position)})"
    case _ => super.evalExpression(c)
  }



}



