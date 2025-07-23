package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {

  def writeSummary(summary: DataFrame): Unit = {
    summary.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)
  }

  def writeMismatches(mismatches: DataFrame): Unit = {
    mismatches.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.mismatchHiveTable)
  }
}
