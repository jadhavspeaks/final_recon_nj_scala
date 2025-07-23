package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.reconciliation.framework.util.ColumnMapper

class ReconciliationTypeExecutor(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def execute(source: DataFrame, target: DataFrame): Unit = {
    if (config.doCountReconciliation) {
      countReconciliation(source, target)
    }
    if (config.doSchemaDriftDetection) {
      schemaDriftDetection(source, target)
    }
    if (config.doExtraMissingCheck) {
      extraMissingCheck(source, target)
    }
    if (config.doColumnComparison) {
      columnComparison(source, target)
    }
    if (config.doThresholdValidation) {
      thresholdValidation(source, target)
    }
  }

  def executeBusinessRuleValidation(source: DataFrame, derived: DataFrame): Unit = {
    if (config.doBusinessRuleValidation) {
      // In this mode, we compare the source with the derived dataset
      countReconciliation(source, derived)
      extraMissingCheck(source, derived)
      columnComparison(source, derived)
    }
  }

  private def countReconciliation(source: DataFrame, target: DataFrame): Unit = {
    val sourceCount = source.count()
    val targetCount = target.count()
    println(s"Source count: $sourceCount, Target count: $targetCount")
    if (sourceCount != targetCount) {
      println(s"Count mismatch: source has $sourceCount rows, target has $targetCount rows.")
    }
  }

  private def schemaDriftDetection(source: DataFrame, target: DataFrame): Unit = {
    val sourceSchema = source.schema.fields.map(f => (f.name, f.dataType)).toMap
    val targetSchema = target.schema.fields.map(f => (f.name, f.dataType)).toMap

    val missingInTarget = sourceSchema.keys.toSet -- targetSchema.keys
    val extraInTarget = targetSchema.keys.toSet -- sourceSchema.keys
    val commonCols = sourceSchema.keys.toSet.intersect(targetSchema.keys)
    val typeMismatches = commonCols.filter(c => sourceSchema(c) != targetSchema(c))

    if (missingInTarget.nonEmpty) println(s"Columns missing in target: ${missingInTarget.mkString(", ")}")
    if (extraInTarget.nonEmpty) println(s"Extra columns in target: ${extraInTarget.mkString(", ")}")
    if (typeMismatches.nonEmpty) println(s"Type mismatches: ${typeMismatches.mkString(", ")}")
  }

  private def extraMissingCheck(source: DataFrame, target: DataFrame): Unit = {
    val sourcePKs = config.sourcePrimaryKeys
    val targetPKs = if (config.reconMode == "SOURCE_TO_TARGET") config.targetPrimaryKeys else sourcePKs

    val sourceKeys = source.select(sourcePKs.map(col): _*).withColumn("pk_hash", sha2(concat_ws("||", sourcePKs.map(col): _*), 256))
    val targetKeys = target.select(targetPKs.map(col): _*).withColumn("pk_hash", sha2(concat_ws("||", targetPKs.map(col): _*), 256))

    val missingInTarget = sourceKeys.join(targetKeys, Seq("pk_hash"), "left_anti")
    val extraInSource = targetKeys.join(sourceKeys, Seq("pk_hash"), "left_anti")

    println(s"Records missing in target: ${missingInTarget.count()}")
    missingInTarget.show(false)
    println(s"Records extra in source (not in target): ${extraInSource.count()}")
    extraInSource.show(false)
  }

  private def columnComparison(source: DataFrame, target: DataFrame): Unit = {
    val mappedTarget = ColumnMapper.mapColumns(target, config)
    val joinKeys = config.sourcePrimaryKeys

    val joined = source.join(mappedTarget, joinKeys, "inner")

    config.columnMappings.foreach { case (sourceCol, targetCol) =>
      val mismatches = joined.filter(col(sourceCol) =!= col(targetCol))
      if (mismatches.count() > 0) {
        println(s"Mismatches found in column: $sourceCol -> $targetCol")
        mismatches.select(joinKeys.map(col) :+ col(sourceCol).as(s"${sourceCol}_source") :+ col(targetCol).as(s"${targetCol}_target"): _*).show(false)
      }
    }
  }

  private def thresholdValidation(source: DataFrame, target: DataFrame): Unit = {
    val mappedTarget = ColumnMapper.mapColumns(target, config)
    val joinKeys = config.sourcePrimaryKeys
    val joined = source.join(mappedTarget, joinKeys, "inner")

    config.thresholdSettings.foreach { case (colName, (thresholdType, thresholdValue)) =>
      val (sourceCol, targetCol) = if (config.columnMappings.contains(colName)) (colName, config.columnMappings(colName)) else (colName, colName)

      val diffExpr = abs(col(sourceCol).cast("double") - col(targetCol).cast("double"))
      val thresholdBreaches = thresholdType.toLowerCase match {
        case "absolute" => joined.filter(diffExpr > thresholdValue)
        case "percentage" => joined.filter((diffExpr / col(sourceCol).cast("double")) * 100 > thresholdValue)
        case _ => spark.emptyDataFrame
      }

      if (thresholdBreaches.count() > 0) {
        println(s"Threshold breaches found for column $colName:")
        thresholdBreaches.show(false)
      }
    }
  }
}
