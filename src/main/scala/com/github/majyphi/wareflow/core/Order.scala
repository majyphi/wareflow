package com.github.majyphi.wareflow.core

case class Order(column: Expression, direction: Direction)


abstract class Direction(value: String) {
  override def toString: String = value
}

object Direction {

  case class DefinedDirection(value: String) extends Direction(value)

  val ASC = DefinedDirection("ASC")
  val DESC = DefinedDirection("DESC")

}
