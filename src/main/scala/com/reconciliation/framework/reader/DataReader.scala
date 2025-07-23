package com.reconciliation.framework.reader

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

object DataReader {

  def read(spark: SparkSession, config: ReconciliationConfig): (DataFrame, Option[DataFrame]) = {
    val sourceDF = readSource(spark, config)
    val targetDF = if (config.reconMode == "SOURCE_TO_TARGET") {
      Some(readTarget(spark, config))
    } else {
      None
    }
    (sourceDF, targetDF)
  }

  private def readSource(spark: SparkSession, config: ReconciliationConfig): DataFrame = {
    val reader = spark.read.option("header", "true")

    config.sourceType.toUpperCase match {
      case "CSV" | "TEXT" | "DAT" =>
        val delimiter = config.sourceDelimiter.getOrElse(",")
        reader.option("delimiter", delimiter).csv(config.sourcePath.get)
      case "EXCEL" =>
        reader.format("com.crealytics.spark.excel").load(config.sourcePath.get)
      case "PARQUET" => spark.read.parquet(config.sourcePath.get)
      case "ORC" => spark.read.orc(config.sourcePath.get)
      case "HIVE" => spark.table(config.sourceHiveTable.get)
      case "JDBC" =>
        val properties = new Properties()
        properties.setProperty("user", config.sourceJdbcUser.get)
        properties.setProperty("password", config.sourceJdbcPassword.get)
        properties.setProperty("driver", config.sourceJdbcDriver.get)
        spark.read.jdbc(config.sourceJdbcUrl.get, config.sourceHiveTable.get, properties)
      case _ => throw new IllegalArgumentException(s"Unsupported source type: ${config.sourceType}")
    }
  }

  private def readTarget(spark: SparkSession, config: ReconciliationConfig): DataFrame = {
    config.targetType.get.toUpperCase match {
      case "HIVE" => spark.table(config.targetHiveTable.get)
      case "PARQUET" => spark.read.parquet(config.targetPath.get)
      case "ORC" => spark.read.orc(config.targetPath.get)
      case _ => throw new IllegalArgumentException(s"Unsupported target type: ${config.targetType.get}")
    }
  }
}
