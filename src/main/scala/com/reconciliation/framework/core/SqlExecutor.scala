package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class SqlExecutor(spark: SparkSession, config: ReconciliationConfig) {

  def execute(): DataFrame = {
    config.reconSql.map(spark.sql)
      .getOrElse(throw new IllegalStateException("Reconciliation SQL is not defined for this job"))
  }
}
