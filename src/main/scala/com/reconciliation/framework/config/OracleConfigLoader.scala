package com.reconciliation.framework.config

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

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
    sourceDelimiter: Option[String],
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

class OracleConfigLoader(jdbcUrl: String, dbProperties: Properties) {

  def loadConfig(jobName: String): ReconciliationConfig = {
    var conn: Connection = null
    try {
      Class.forName(dbProperties.getProperty("driver"))
      conn = DriverManager.getConnection(jdbcUrl, dbProperties.getProperty("user"), dbProperties.getProperty("password"))
      val stmt = conn.prepareStatement("SELECT * FROM RECON_CONFIG WHERE JOB_NAME = ? AND IS_ACTIVE = 'Y'")
      stmt.setString(1, jobName)
      val rs = stmt.executeQuery()

      if (rs.next()) {
        buildConfig(rs)
      } else {
        throw new IllegalArgumentException(s"No active configuration found for job: $jobName")
      }
    } finally {
      if (conn != null) conn.close()
    }
  }

  private def buildConfig(rs: ResultSet): ReconciliationConfig = {
    ReconciliationConfig(
      jobId = rs.getLong("JOB_ID"),
      jobName = rs.getString("JOB_NAME"),
      reconMode = rs.getString("RECON_MODE"),
      sourceType = rs.getString("SOURCE_TYPE"),
      sourcePath = Option(rs.getString("SOURCE_PATH")),
      sourceJdbcUrl = Option(rs.getString("SOURCE_JDBC_URL")),
      sourceJdbcUser = Option(rs.getString("SOURCE_JDBC_USER")),
      sourceJdbcPassword = Option(rs.getString("SOURCE_JDBC_PASSWORD")),
      sourceJdbcDriver = Option(rs.getString("SOURCE_JDBC_DRIVER")),
      sourceHiveTable = Option(rs.getString("SOURCE_HIVE_TABLE")),
      sourcePrimaryKeys = Option(rs.getString("SOURCE_PRIMARY_KEYS")).map(_.split(",").toSeq).getOrElse(Seq.empty),
      sourceDelimiter = Option(rs.getString("SOURCE_DELIMITER")),
      targetType = Option(rs.getString("TARGET_TYPE")),
      targetPath = Option(rs.getString("TARGET_PATH")),
      targetHiveTable = Option(rs.getString("TARGET_HIVE_TABLE")),
      targetPrimaryKeys = Option(rs.getString("TARGET_PRIMARY_KEYS")).map(_.split(",").toSeq).getOrElse(Seq.empty),
      reconSql = Option(rs.getString("RECON_SQL")),
      doColumnComparison = rs.getString("DO_COLUMN_COMPARISON") == "Y",
      doBusinessRuleValidation = rs.getString("DO_BUSINESS_RULE_VALIDATION") == "Y",
      doCountReconciliation = rs.getString("DO_COUNT_RECONCILIATION") == "Y",
      doExtraMissingCheck = rs.getString("DO_EXTRA_MISSING_CHECK") == "Y",
      doThresholdValidation = rs.getString("DO_THRESHOLD_VALIDATION") == "Y",
      doSchemaDriftDetection = rs.getString("DO_SCHEMA_DRIFT_DETECTION") == "Y",
      columnMappings = Option(rs.getString("COLUMN_MAPPINGS")).map(parseColumnMappings).getOrElse(Map.empty),
      thresholdSettings = Option(rs.getString("THRESHOLD_SETTINGS")).map(parseThresholdSettings).getOrElse(Map.empty),
      emailRecipients = Option(rs.getString("EMAIL_RECIPIENTS")).map(_.split(",").toSeq).getOrElse(Seq.empty),
      emailSubject = rs.getString("EMAIL_SUBJECT"),
      auditHiveTable = rs.getString("AUDIT_HIVE_TABLE"),
      mismatchHiveTable = rs.getString("MISMATCH_HIVE_TABLE"),
      isActive = rs.getString("IS_ACTIVE") == "Y"
    )
  }

  private def parseColumnMappings(mappings: String): Map[String, String] = {
    mappings.split(",").map { pair =>
      val parts = pair.split(":")
      parts(0) -> parts(1)
    }.toMap
  }

  private def parseThresholdSettings(settings: String): Map[String, (String, Double)] = {
    settings.split(",").map { setting =>
      val parts = setting.split(":")
      parts(0) -> (parts(1), parts(2).toDouble)
    }.toMap
  }
}
