package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant
import com.google.gson.Gson

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def writeAuditLog(result: ReconciliationResult): Unit = {
    val executionTimestamp = Timestamp.from(Instant.now())

    val detailsMap = Map(
      "schema_drift" -> result.schemaDriftResults.map(r => Map(
        "missing_in_target" -> r.missingInTarget,
        "extra_in_target" -> r.extraInTarget,
        "type_mismatches" -> r.typeMismatches
      )),
      "extra_missing" -> result.extraMissingResult.map(r => Map(
        "missing_in_target_count" -> r.missingInTarget.count(),
        "extra_in_source_count" -> r.extraInSource.count()
      )),
      "column_comparison" -> result.columnComparisonResults.map(r => Map(
        "source_column" -> r.sourceColumn,
        "target_column" -> r.targetColumn,
        "mismatch_count" -> r.mismatches.count()
      )),
      "threshold_validation" -> result.thresholdValidationResults.map(r => Map(
        "column_name" -> r.columnName,
        "breach_count" -> r.breaches.count()
      )),
      "business_rule_validation_mismatch_count" -> result.businessRuleValidationResult.map(_.count())
    )

    val detailsJson = new Gson().toJson(detailsMap)

    val summary = spark.createDataFrame(Seq(
      (config.jobId, config.jobName, executionTimestamp, result.status, result.sourceCount, result.targetCount, result.countMatch, (result.endTime - result.startTime) / 1000, detailsJson)
    )).toDF("job_id", "job_name", "execution_timestamp", "status", "source_count", "target_count", "count_match", "execution_time_in_seconds", "details")

    summary.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)
  }
}
