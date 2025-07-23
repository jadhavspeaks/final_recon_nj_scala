package com.reconciliation.framework.config

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ReconciliationConfig(
    jobId: Long,
    jobName: String,
    reconMode: String,
    sourceType: String,
    sourcePath: Option[String],
    sourceJdbcUrl: Option[String],
    sourceJdbcUser: Option[String],
    sourceJdbcPassword: Option[String],
    sourceJdbcDriver: Option[String],
    sourceHiveTable: Option[String],
    sourcePrimaryKeys: Seq[String],
    targetType: Option[String],
    targetPath: Option[String],
    targetHiveTable: Option[String],
    targetPrimaryKeys: Seq[String],
    reconSql: Option[String],
    doColumnComparison: Boolean,
    doBusinessRuleValidation: Boolean,
    doCountReconciliation: Boolean,
    doExtraMissingCheck: Boolean,
    doThresholdValidation: Boolean,
    doSchemaDriftDetection: Boolean,
    columnMappings: Map[String, String],
    thresholdSettings: Map[String, (String, Double)],
    emailRecipients: Seq[String],
    emailSubject: String,
    auditHiveTable: String,
    mismatchHiveTable: String,
    isActive: Boolean
)

class OracleConfigLoader(spark: SparkSession, jdbcUrl: String, dbProperties: Properties) {

  def loadConfig(jobName: String): ReconciliationConfig = {
    val query = s"(SELECT * FROM RECON_CONFIG WHERE JOB_NAME = '$jobName' AND IS_ACTIVE = 'Y') t"
    val df = spark.read.jdbc(jdbcUrl, query, dbProperties)

    if (df.isEmpty) {
      throw new IllegalArgumentException(s"No active configuration found for job: $jobName")
    }

    import spark.implicits._
    val configRow = df.as[RawReconConfig].first()

    // In a real implementation, you would parse the raw config into the structured ReconciliationConfig
    // For now, this is a placeholder
    ???
  }

  // Raw representation of the table row
  private case class RawReconConfig(
      JOB_ID: Long,
      JOB_NAME: String,
      RECON_MODE: String,
      SOURCE_TYPE: String,
      SOURCE_PATH: String,
      SOURCE_JDBC_URL: String,
      SOURCE_JDBC_USER: String,
      SOURCE_JDBC_PASSWORD: String,
      SOURCE_JDBC_DRIVER: String,
      SOURCE_HIVE_TABLE: String,
      SOURCE_PRIMARY_KEYS: String,
      TARGET_TYPE: String,
      TARGET_PATH: String,
      TARGET_HIVE_TABLE: String,
      TARGET_PRIMARY_KEYS: String,
      RECON_SQL: String,
      DO_COLUMN_COMPARISON: String,
      DO_BUSINESS_RULE_VALIDATION: String,
      DO_COUNT_RECONCILIATION: String,
      DO_EXTRA_MISSING_CHECK: String,
      DO_THRESHOLD_VALIDATION: String,
      DO_SCHEMA_DRIFT_DETECTION: String,
      COLUMN_MAPPINGS: String,
      THRESHOLD_SETTINGS: String,
      EMAIL_RECIPIENTS: String,
      EMAIL_SUBJECT: String,
      AUDIT_HIVE_TABLE: String,
      MISMATCH_HIVE_TABLE: String,
      IS_ACTIVE: String
  )
}
