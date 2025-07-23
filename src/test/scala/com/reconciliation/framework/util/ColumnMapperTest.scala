package com.reconciliation.framework.util

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ColumnMapperTest extends AnyFunSuite with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ColumnMapperTest")
    .getOrCreate()

  import spark.implicits._

  test("mapColumns should rename columns based on config") {
    val df = Seq((1, "a"), (2, "b")).toDF("id", "value")
    val config = ReconciliationConfig(
      jobId = 1, jobName = "test", reconMode = "SOURCE_TO_TARGET",
      sourceType = "TEST", sourcePath = None, sourceJdbcUrl = None, sourceJdbcUser = None, sourceJdbcPassword = None, sourceJdbcDriver = None,
      sourceHiveTable = None, sourcePrimaryKeys = Seq("id"), targetType = None, targetPath = None, targetHiveTable = None, targetPrimaryKeys = Seq("new_id"),
      reconSql = None, doColumnComparison = true, doBusinessRuleValidation = false, doCountReconciliation = false, doExtraMissingCheck = false,
      doThresholdValidation = false, doSchemaDriftDetection = false,
      columnMappings = Map("id" -> "new_id", "value" -> "new_value"),
      thresholdSettings = Map.empty, emailRecipients = Seq.empty, emailSubject = "", auditHiveTable = "", mismatchHiveTable = "", isActive = true
    )

    val result = ColumnMapper.mapColumns(df, config)

    result.columns should contain allOf ("new_id", "new_value")
  }
}
