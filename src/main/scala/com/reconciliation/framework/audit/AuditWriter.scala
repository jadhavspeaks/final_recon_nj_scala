package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

case class AuditSummary(
  job_id: String,
  job_name: String,
  execution_timestamp: Timestamp,
  audit_type: String,
  status: String,
  sourceCount: Long,
  targetCount: Long,
  countMatch: Boolean
)

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def writeAuditLog(result: ReconciliationResult): Unit = {
    val executionTimestamp = Timestamp.from(Instant.now())

    // Write summary record
    val summary = Seq(
      AuditSummary(
        job_id = config.jobId,
        job_name = config.jobName,
        execution_timestamp = executionTimestamp,
        audit_type = "SUMMARY",
        status = result.status,
        sourceCount = result.sourceCount,
        targetCount = result.targetCount,
        countMatch = result.countMatch
      )
    ).toDF()

    summary.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)

    // Write mismatch records
    val mismatchDfs = (
      result.extraMissingResult.map(r => Seq(
        r.missingInTarget.withColumn("mismatch_type", lit("missing_in_target")),
        r.extraInSource.withColumn("mismatch_type", lit("extra_in_source"))
      )).getOrElse(Seq.empty) ++
      result.columnComparisonResults.map(r => r.mismatches.withColumn("mismatch_type", lit(s"column_mismatch_${r.sourceColumn}"))) ++
      result.thresholdValidationResults.map(r => r.breaches.withColumn("mismatch_type", lit(s"threshold_breach_${r.columnName}"))) ++
      result.businessRuleValidationResult.map(df => Seq(df.withColumn("mismatch_type", lit("business_rule_mismatch")))).getOrElse(Seq.empty)
    )

    mismatchDfs.foreach { df =>
      if (!df.isEmpty) {
        val auditDf = df.withColumn("job_id", lit(config.jobId))
                        .withColumn("job_name", lit(config.jobName))
                        .withColumn("execution_timestamp", lit(executionTimestamp))
                        .withColumn("audit_type", lit("MISMATCH"))
                        .withColumn("status", lit(result.status))

        auditDf.write
          .mode("append")
          .format("hive")
          .saveAsTable(config.auditHiveTable)
      }
    }
  }
}
