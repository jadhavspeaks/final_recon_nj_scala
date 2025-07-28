package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

case class AuditRecord(
  job_id: Long,
  job_name: String,
  execution_timestamp: Timestamp,
  audit_type: String,
  status: String,
  sourceCount: Option[Long],
  targetCount: Option[Long],
  countMatch: Option[Boolean],
  execution_time_in_seconds: Option[Long],
  mismatch_type: Option[String],
  mismatch_details: Option[String]
)

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def writeAuditLog(result: ReconciliationResult): Unit = {
    val executionTimestamp = Timestamp.from(Instant.now())

    val summaryRecord = AuditRecord(
      job_id = config.jobId,
      job_name = config.jobName,
      execution_timestamp = executionTimestamp,
      audit_type = "SUMMARY",
      status = result.status,
      sourceCount = Some(result.sourceCount),
      targetCount = Some(result.targetCount),
      countMatch = Some(result.countMatch),
      execution_time_in_seconds = Some((result.endTime - result.startTime) / 1000),
      mismatch_type = None,
      mismatch_details = None
    )

    val mismatchRecords = (
      result.extraMissingResult.map(r => Seq(
        r.missingInTarget.withColumn("mismatch_type", lit("missing_in_target")),
        r.extraInSource.withColumn("mismatch_type", lit("extra_in_source"))
      )).getOrElse(Seq.empty) ++
      result.columnComparisonResults.map(r => r.mismatches.withColumn("mismatch_type", lit(s"column_mismatch_${r.sourceColumn}"))) ++
      result.thresholdValidationResults.map(r => r.breaches.withColumn("mismatch_type", lit(s"threshold_breach_${r.columnName}"))) ++
      result.businessRuleValidationResult.map(df => Seq(df.withColumn("mismatch_type", lit("business_rule_mismatch")))).getOrElse(Seq.empty) ++
      result.schemaDriftResults.map { r =>
        val missingInTarget = spark.createDataset(r.missingInTarget.toSeq).toDF("mismatch_details").withColumn("mismatch_type", lit("schema_drift_missing_in_target"))
        val extraInTarget = spark.createDataset(r.extraInTarget.toSeq).toDF("mismatch_details").withColumn("mismatch_type", lit("schema_drift_extra_in_target"))
        val typeMismatches = spark.createDataset(r.typeMismatches.toSeq).toDF("mismatch_details").withColumn("mismatch_type", lit("schema_drift_type_mismatch"))
        Seq(missingInTarget, extraInTarget, typeMismatches)
      }.getOrElse(Seq.empty)
    ).flatMap { df =>
      if (!df.isEmpty) {
        val auditDf = df.withColumn("job_id", lit(config.jobId))
                        .withColumn("job_name", lit(config.jobName))
                        .withColumn("execution_timestamp", lit(executionTimestamp))
                        .withColumn("audit_type", lit("MISMATCH"))
                        .withColumn("status", lit(result.status))
                        .withColumn("mismatch_details", to_json(struct(df.columns.map(c => col(c).cast("string")): _*)))

        Some(auditDf.select(
          col("job_id"),
          col("job_name"),
          col("execution_timestamp"),
          col("audit_type"),
          col("status"),
          lit(null).cast("long").as("sourceCount"),
          lit(null).cast("long").as("targetCount"),
          lit(null).cast("boolean").as("countMatch"),
          lit(null).cast("long").as("execution_time_in_seconds"),
          col("mismatch_type"),
          col("mismatch_details")
        Some(auditDf.select(
          col("job_id"),
          col("job_name"),
          col("execution_timestamp"),
          col("audit_type"),
          col("status"),
          lit(null).cast("long").as("sourceCount"),
          lit(null).cast("long").as("targetCount"),
          lit(null).cast("boolean").as("countMatch"),
          lit(null).cast("long").as("execution_time_in_seconds"),
          col("mismatch_type"),
          col("mismatch_details")
        ).as[AuditRecord])
      } else {
        None
      }
    }

    val auditData = Seq(summaryRecord) ++ mismatchRecords
    spark.createDataset(auditData)
      .write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)
  }
}
