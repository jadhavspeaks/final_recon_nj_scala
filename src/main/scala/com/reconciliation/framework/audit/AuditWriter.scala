package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def writeAuditLog(result: ReconciliationResult): Unit = {
    val executionTimestamp = Timestamp.from(Instant.now())

    // Write summary record
    val summary = Seq(result).toDF()
      .withColumn("job_id", lit(config.jobId))
      .withColumn("job_name", lit(config.jobName))
      .withColumn("execution_timestamp", lit(executionTimestamp))
      .withColumn("audit_type", lit("SUMMARY"))
      .select("job_id", "job_name", "execution_timestamp", "audit_type", "status", "sourceCount", "targetCount", "countMatch")

    summary.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)

    // Write mismatch records
    val mismatchDfs = Seq(
      result.extraMissingResult.map(_.missingInTarget.withColumn("mismatch_type", lit("missing_in_target"))),
      result.extraMissingResult.map(_.extraInSource.withColumn("mismatch_type", lit("extra_in_source"))),
      Some(result.columnComparisonResults.map(r => r.mismatches.withColumn("mismatch_type", lit(s"column_mismatch_${r.sourceColumn}"))).reduceOption(_ union _).getOrElse(spark.emptyDataFrame)),
      Some(result.thresholdValidationResults.map(r => r.breaches.withColumn("mismatch_type", lit(s"threshold_breach_${r.columnName}"))).reduceOption(_ union _).getOrElse(spark.emptyDataFrame)),
      result.businessRuleValidationResult.map(_.withColumn("mismatch_type", lit("business_rule_mismatch")))
    ).flatten

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
