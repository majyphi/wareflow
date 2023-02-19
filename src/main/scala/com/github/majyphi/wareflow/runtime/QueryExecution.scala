package com.github.majyphi.wareflow.runtime

import de.vandermeer.asciitable.AsciiTable

import java.sql.{Connection, ResultSet}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object QueryExecution {

  def run(query: String)(implicit connection: Connection): Try[Seq[Seq[String]]] = {
    val statement = connection.createStatement()
    Try(statement.execute(query))
      .flatMap(_ => Try(statement.getResultSet))
      .map(rs => {
          val meta = rs.getMetaData
          val header = (1 to meta.getColumnCount).map(meta.getColumnName)

          @tailrec
          def loop(resultSet: ResultSet, agg: Seq[Seq[String]]): Seq[Seq[String]] = {
            if (resultSet.next()) {
              loop(resultSet, Seq(header.map(field => rs.getObject(field).toString)) ++ agg)
            } else agg
          }

          loop(rs, Seq())
      })
  }

  def show(query: String)(implicit connection: Connection): Unit = {
    val statement = connection.createStatement()
    Try(statement.execute(query))
      .flatMap(_ => Try(statement.getResultSet))
      .map(rs => {
        val meta = rs.getMetaData
        val columnRange = (1 to meta.getColumnCount)
        val header = columnRange.map(meta.getColumnName)

        @tailrec
        def loop(resultSet: ResultSet, agg: Seq[Seq[String]]): Seq[Seq[String]] = {
          if (resultSet.next()) {
            loop(resultSet, Seq(columnRange.map(index => Option(rs.getObject(index)).map(_.toString).getOrElse("null"))) ++ agg)
          } else agg
        }

        (loop(rs, Seq()), header)
      }) match {
      case Failure(exception) => throw exception
      case Success((results,header)) =>
        println(getTableString(results, header))

    }

  }

  private def getTableString(data: Seq[Seq[String]], header: Seq[String]) = {
    val at = new AsciiTable
    at.addRule()
    at.addRow(header: _*)
    at.addRule()
    data.foreach(line => {
      at.addRow(line: _*)
    })
    at.addRule()
    s"""${at.render}"""
  }

}
