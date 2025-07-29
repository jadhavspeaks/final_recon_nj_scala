package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.reconciliation.framework.util.ColumnMapper

class ReconciliationTypeExecutor(spark: SparkSession, config: ReconciliationConfig) {
  import spark.implicits._

  def execute(source: DataFrame, target: DataFrame): ReconciliationResult = {
    var result = ReconciliationResult(jobName = config.jobName, reconMode = config.reconMode)

    if (config.doCountReconciliation) {
      val (sourceCount, targetCount, matchStatus) = countReconciliation(source, target)
      result = result.copy(sourceCount = sourceCount, targetCount = targetCount, countMatch = matchStatus)
    }
    if (config.doSchemaDriftDetection) {
      val schemaDriftResult = schemaDriftDetection(source, target)
      if (schemaDriftResult.missingInTarget.nonEmpty || schemaDriftResult.extraInTarget.nonEmpty || schemaDriftResult.typeMismatches.nonEmpty) {
        result = result.copy(status = "FAILURE")
      }
      result = result.copy(schemaDriftResults = Some(schemaDriftResult))
    }
    if (config.doExtraMissingCheck) {
      val extraMissingResult = extraMissingCheck(source, target)
      if (extraMissingResult.missingInTarget.count() > 0 || extraMissingResult.extraInSource.count() > 0) {
        result = result.copy(status = "FAILURE")
      }
      result = result.copy(extraMissingResult = Some(extraMissingResult))
    }
    if (config.doColumnComparison) {
      val columnComparisonResults = columnComparison(source, target)
      if (columnComparisonResults.exists(_.mismatches.count() > 0)) {
        result = result.copy(status = "FAILURE")
      }
      result = result.copy(columnComparisonResults = columnComparisonResults)
    }
    if (config.doThresholdValidation) {
      val thresholdValidationResults = thresholdValidation(source, target)
      if (thresholdValidationResults.exists(_.breaches.count() > 0)) {
        result = result.copy(status = "FAILURE")
      }
      result = result.copy(thresholdValidationResults = thresholdValidationResults)
    }
    result
  }

  def executeBusinessRuleValidation(source: DataFrame, derived: DataFrame): ReconciliationResult = {
    var result = ReconciliationResult(jobName = config.jobName, reconMode = config.reconMode)
    if (config.doBusinessRuleValidation) {
      val mismatches = source.except(derived)
      result = result.copy(businessRuleValidationResult = Some(mismatches))
    }
    result
  }

  private def countReconciliation(source: DataFrame, target: DataFrame): (Long, Long, Boolean) = {
    val sourceCount = source.count()
    val targetCount = target.count()
    (sourceCount, targetCount, sourceCount.toString.equalsIgnoreCase(targetCount.toString))
  }

  private def schemaDriftDetection(source: DataFrame, target: DataFrame): SchemaDriftResult = {
    val sourceSchema = source.schema.fields.map(f => (f.name, f.dataType.simpleString)).toMap
    val targetSchema = target.schema.fields.map(f => (f.name, f.dataType.simpleString)).toMap
    val missingInTarget = sourceSchema.keys.toSet -- targetSchema.keys
    val extraInTarget = targetSchema.keys.toSet -- sourceSchema.keys
    val commonCols = sourceSchema.keys.toSet.intersect(targetSchema.keys.toSet)
    val typeMismatches = commonCols.filter(c => sourceSchema(c) != targetSchema(c))
    SchemaDriftResult(missingInTarget, extraInTarget, typeMismatches)
  }

  private def extraMissingCheck(source: DataFrame, target: DataFrame): ExtraMissingResult = {
    val sourcePKs = config.sourcePrimaryKeys
    val targetPKs = if (config.reconMode == "SOURCE_TO_TARGET") config.targetPrimaryKeys else sourcePKs

    val sourceKeys = source.select(sourcePKs.map(col): _*).withColumn("pk_hash", sha2(concat_ws("||", sourcePKs.map(col): _*), 256))
    val targetKeys = target.select(targetPKs.map(col): _*).withColumn("pk_hash", sha2(concat_ws("||", targetPKs.map(col): _*), 256))

    val missingInTarget = sourceKeys.join(targetKeys, Seq("pk_hash"), "left_anti").drop("pk_hash")
    val extraInSource = targetKeys.join(sourceKeys, Seq("pk_hash"), "left_anti").drop("pk_hash")
    ExtraMissingResult(missingInTarget, extraInSource)
  }

  private def columnComparison(source: DataFrame, target: DataFrame): Seq[ColumnComparisonResult] = {
    val joinKeys = config.sourcePrimaryKeys

    // Create a map for all column renames (PKs and data columns) from target to source names
    val pkRenames = config.targetPrimaryKeys.zip(config.sourcePrimaryKeys).toMap
    val colRenames = config.columnMappings.map { case (source, target) => (target, source) }
    val allRenames = pkRenames ++ colRenames

    // Rename all necessary columns in the target DataFrame in one pass
    val aliasedTarget = allRenames.foldLeft(target) { case (df, (oldName, newName)) =>
      if (df.columns.contains(oldName)) {
        df.withColumnRenamed(oldName, newName)
      } else {
        df
      }
    }

    // Combine join expressions with &&
    val joinExpr = joinKeys.map(c => source(c) === aliasedTarget(c)).reduce(_ && _)
    val joined = source.as("s").join(aliasedTarget.as("t"), joinExpr, "outer")

    config.columnMappings.flatMap { case (sourceCol, targetCol) =>
      // All columns on the target DataFrame are now aliased to source column names
      val sCol = col(s"s.$sourceCol")
      val tCol = col(s"t.$sourceCol")

      val mismatchExpr = (sCol.isNull and tCol.isNotNull) or
        (sCol.isNotNull and tCol.isNull) or
        (sCol =!= tCol)

      val mismatches = joined.filter(mismatchExpr)

      if (mismatches.head(1).isEmpty) {
        None
      } else {
        Some(
          ColumnComparisonResult(
            sourceColumn = sourceCol,
            targetColumn = targetCol, // Reporting original target column name
            mismatches = mismatches.select(
              concat_ws(",", joinKeys.map(c => col(s"s.$c")): _*).as("primary_key"),
              lit(sourceCol).as("mismatched_column"),
              sCol.cast("string").as("source_value"),
              tCol.cast("string").as("target_value")
            )
          )
        )
      }
    }.toSeq
  }

  private def thresholdValidation(source: DataFrame, target: DataFrame): Seq[ThresholdValidationResult] = {
    val mappedTarget = ColumnMapper.mapColumns(target, config)
    val joinKeys = config.sourcePrimaryKeys
    val aliasedMappedTarget = config.columnMappings.foldLeft(mappedTarget) {
      case (df, (sourceCol, targetCol)) => df.withColumnRenamed(targetCol, sourceCol)
    }
    val joined = source.join(aliasedMappedTarget, joinKeys, "inner")

    config.thresholdSettings.map { case (colName, (thresholdType, thresholdValue)) =>
      val (sourceCol, targetCol) = if (config.columnMappings.contains(colName)) (colName, config.columnMappings(colName)) else (colName, colName)

      val diffExpr = abs(col(sourceCol).cast("double") - col(targetCol).cast("double"))
      val breaches = thresholdType.toLowerCase match {
        case "absolute" => joined.filter(diffExpr > thresholdValue)
        case "percentage" => joined.filter((diffExpr / col(sourceCol).cast("double")) * 100 > thresholdValue)
        case _ => spark.emptyDataFrame
      }
      ThresholdValidationResult(colName, thresholdType, thresholdValue, breaches)
    }.toSeq
  }
}
