package com.reconciliation.framework.util

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.DataFrame

object ColumnMapper {

  def mapColumns(df: DataFrame, config: ReconciliationConfig): DataFrame = {
    val columnMappings = config.columnMappings
    if (columnMappings.isEmpty) {
      df
    } else {
      val selectExprs = df.columns.map { col =>
        if (columnMappings.contains(col)) {
          df(col).as(columnMappings(col))
        } else {
          df(col)
        }
      }
      df.select(selectExprs: _*)
    }
  }
}
