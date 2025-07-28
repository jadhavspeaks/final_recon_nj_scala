package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

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

    val detailsJson = s"""
      {
        "schema_drift": ${result.schemaDriftResults.map(r => s"""{"missing_in_target": [${r.missingInTarget.map(s => s""""$s"""").mkString(",")}],"extra_in_target": [${r.extraInTarget.map(s => s""""$s"""").mkString(",")}],"type_mismatches": [${r.typeMismatches.map(s => s""""$s"""").mkString(",")}]}""").getOrElse("null")},
        "extra_missing": {
          "missing_in_target_count": ${result.extraMissingResult.map(_.missingInTarget.count()).getOrElse(0)},
          "extra_in_source_count": ${result.extraMissingResult.map(_.extraInSource.count()).getOrElse(0)}
        },
        "column_comparison": [
          ${result.columnComparisonResults.map(r => s"""{"source_column": "${r.sourceColumn}","target_column": "${r.targetColumn}","mismatch_count": ${r.mismatches.count()}}""").mkString(",")}
        ],
        "threshold_validation": [
          ${result.thresholdValidationResults.map(r => s"""{"column_name": "${r.columnName}","breach_count": ${r.breaches.count()}}""").mkString(",")}
        ],
        "business_rule_validation_mismatch_count": ${result.businessRuleValidationResult.map(_.count()).getOrElse(0)}
      }
    """

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
