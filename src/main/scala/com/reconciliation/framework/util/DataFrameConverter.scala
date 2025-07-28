package com.reconciliation.framework.util

import org.apache.spark.sql.DataFrame

object DataFrameConverter {

  def toHtml(df: DataFrame): String = {
    val header = df.columns.map(c => s"<th>$c</th>").mkString("<tr>", "", "</tr>")
    val rows = df.collect().map { row =>
      val cells = row.toSeq.map(c => s"<td>${c.toString}</td>").mkString("<tr>", "", "</tr>")
      cells
    }.mkString("")
    s"<table>$header$rows</table>"
  }
}
