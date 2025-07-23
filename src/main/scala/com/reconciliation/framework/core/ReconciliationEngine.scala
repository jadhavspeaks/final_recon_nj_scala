package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.SparkSession

class ReconciliationEngine(spark: SparkSession, config: ReconciliationConfig) {

  def run(): Unit = {
    // This is the main orchestration logic
    // In a real implementation, you would:
    // 1. Load source and target data (or execute SQL)
    // 2. Trigger the ReconciliationTypeExecutor based on config flags
    // 3. Collect results
    // 4. Call AuditWriter to log the summary
    // 5. Call HtmlMailGenerator to send the report
    println(s"Running reconciliation job: ${config.jobName}")
  }
}
