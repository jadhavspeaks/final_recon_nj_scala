package com.reconciliation.framework.audit

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Timestamp
import java.time.Instant

class AuditWriter(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def writeAuditSummary(result: ReconciliationResult): Unit = {
    val summary = Seq(
      (
        config.jobId,
        config.jobName,
        Timestamp.from(Instant.now()),
        result.sourceCount,
        result.targetCount,
        if(result.countMatch) "Y" else "N",
        result.status
      )
    ).toDF("JOB_ID", "JOB_NAME", "EXECUTION_TIMESTAMP", "SOURCE_COUNT", "TARGET_COUNT", "COUNT_MATCH", "STATUS")

    summary.write
      .mode("append")
      .format("hive")
      .saveAsTable(config.auditHiveTable)
  }

  def writeMismatchDetails(result: ReconciliationResult): Unit = {
    result.extraMissingResult.foreach { res =>
      writeMismatches(res.missingInTarget, "missing_in_target")
      writeMismatches(res.extraInSource, "extra_in_source")
    }
    result.columnComparisonResults.foreach { res =>
      writeMismatches(res.mismatches, s"column_mismatch_${res.sourceColumn}")
    }
    result.thresholdValidationResults.foreach { res =>
      writeMismatches(res.breaches, s"threshold_breach_${res.columnName}")
    }
    result.businessRuleValidationResult.foreach { res =>
      writeMismatches(res, "business_rule_mismatch")
    }
  }

  private def writeMismatches(df: DataFrame, mismatchType: String): Unit = {
    if (!df.isEmpty) {
      val finalDf = df.withColumn("job_id", lit(config.jobId))
        .withColumn("mismatch_type", lit(mismatchType))
        .withColumn("execution_timestamp", lit(Timestamp.from(Instant.now())))

      finalDf.write
        .mode("append")
        .format("hive")
        .saveAsTable(config.mismatchHiveTable)
    }
  }
}
