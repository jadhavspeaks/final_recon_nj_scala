package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

case class AuditDetails(
  schema_drift: Option[SchemaDriftResult],
  extra_missing: Option[ExtraMissingResult],
  column_comparison: Seq[ColumnComparisonResult],
  threshold_validation: Seq[ThresholdValidationResult],
  business_rule_validation: Option[DataFrame]
)

case class AuditRecord(
  job_id: Long,
  job_name: String,
  execution_timestamp: Timestamp,
  status: String,
  source_count: Long,
  target_count: Long,
  count_match: Boolean,
  execution_time_in_seconds: Long,
  details: String
)

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def writeAuditLog(result: ReconciliationResult): Unit = {
    val executionTimestamp = Timestamp.from(Instant.now())

    val auditDetails = AuditDetails(
      schema_drift = result.schemaDriftResults,
      extra_missing = result.extraMissingResult,
      column_comparison = result.columnComparisonResults,
      threshold_validation = result.thresholdValidationResults,
      business_rule_validation = result.businessRuleValidationResult
    )

    val detailsJson = new com.google.gson.Gson().toJson(auditDetails)

    val auditRecord = AuditRecord(
      job_id = config.jobId,
      job_name = config.jobName,
      execution_timestamp = executionTimestamp,
      status = result.status,
      source_count = result.sourceCount,
      target_count = result.targetCount,
      count_match = result.countMatch,
      execution_time_in_seconds = (result.endTime - result.startTime) / 1000,
      details = detailsJson
    )

    val auditData = spark.createDataset(Seq(auditRecord))
    auditData.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)
  }
}
