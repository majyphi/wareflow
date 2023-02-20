package com.github.majyphi.wareflow.core

case class Window(partitions : Seq[Expression] = Seq(), orderBy : Seq[Order]  = Seq()) {
  def partitionBy(columns: Expression*): Window = this.copy(partitions = partitions++columns)

  def orderBy(order: Order*): Window = this.copy(orderBy = orderBy++order)
}


object Window {
  def partitionBy(column : Expression* ) : Window = Window(partitions = column)
  def orderBy(order : Order*) : Window = Window(orderBy = order)
}
