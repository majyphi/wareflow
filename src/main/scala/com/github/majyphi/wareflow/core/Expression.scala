package com.github.majyphi.wareflow.core

trait Expression {

  def as(name: String): Expression = alias(this, name)
  def +(e: Expression): Expression = addition(this, e)
  def -(e: Expression): Expression = subtraction(this, e)
  def *(e: Expression): Expression = product(this, e)
  def >(e: Expression): Expression = greaterThan(this, e)
  def >=(e: Expression): Expression = greaterOrEqual(this, e)
  def <(e: Expression): Expression = lowerThan(this, e)
  def <=(e: Expression): Expression = lowerOrEqual(this, e)
  def equalTo(e: Expression): Expression = equality(this, e)
  def ==(e: Expression): Expression = equalTo(e)
  def max: Expression = maximum(this)
  def min: Expression = minimum(this)
  def over(window: Window): Expression = windowAggregation(this,window)
  def asc: Order = Order(this, Direction.ASC)
  def desc: Order = Order(this, Direction.DESC)
}

trait Column extends Expression

object Helpers {
  implicit class StringToColumnHelper(val sc: StringContext) {
    def c(args: Any*): Column = {
      col(sc.s(args: _*))
    }
  }

  implicit class StringToTableHelper(val sc: StringContext) {
    def t(args: Any*): Table = {
      Table(sc.s(args: _*))
    }
  }
}


case class *() extends Expression


case class col(name: String) extends Column
case class lit(value: String) extends Expression

case class windowAggregation(column: Expression, window : Window) extends Expression

case class rank() extends Expression

case class alias(e: Expression, name: String) extends Expression
case class addition(left: Expression, right: Expression) extends Expression
case class subtraction(left: Expression, right: Expression) extends Expression
case class product(left: Expression, right: Expression) extends Expression
case class greaterThan(left: Expression, right: Expression) extends Expression
case class greaterOrEqual(left: Expression, right: Expression) extends Expression
case class lowerThan(left: Expression, right: Expression) extends Expression
case class lowerOrEqual(left: Expression, right: Expression) extends Expression
case class equality(left: Expression, right: Expression) extends Expression
case class maximum(e: Expression) extends Expression
case class minimum(e: Expression) extends Expression
case class left(e: Column) extends Expression
case class right(e: Column) extends Expression
