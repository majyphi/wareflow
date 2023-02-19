package com.github.majyphi.wareflow.core

trait Column {

  def as(name : String) : Column = alias(this,name)
  def +(c : Column) : Column = addition(this,c)
  def -(c : Column) : Column = subtraction(this,c)
  def *(c : Column) : Column = product(this,c)
  def equalTo(c:Column) : Column = equality(this,c)
  def ==(c:Column) : Column = equalTo(c)
  def max : Column = maximum(this)
  def min : Column = minimum(this)

}

object Column {
  implicit class StringToColumnHelper(val sc: StringContext) {
    def c(args: Any*): Column = {
      col(sc.s(args: _*))
    }

  }
}

case class alias(c : Column, name : String) extends Column
case class lit(value: String) extends Column
case class col(name: String) extends Column
case class addition(left : Column, right : Column) extends Column
case class subtraction(left : Column, right : Column) extends Column
case class product(left : Column, right : Column) extends Column
case class equality(left: Column, right: Column) extends Column
case class maximum(c : Column) extends Column
case class minimum(c : Column) extends Column
case class left(c : Column) extends Column
case class right(c : Column) extends Column
