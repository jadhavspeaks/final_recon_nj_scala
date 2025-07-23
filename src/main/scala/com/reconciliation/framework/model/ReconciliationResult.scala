package com.reconciliation.framework.model

import org.apache.spark.sql.DataFrame

case class ReconciliationResult(
    jobName: String,
    reconMode: String,
    sourceCount: Long = 0,
    targetCount: Long = 0,
    countMatch: Boolean = true,
    schemaDriftResults: Option[SchemaDriftResult] = None,
    extraMissingResult: Option[ExtraMissingResult] = None,
    columnComparisonResults: Seq[ColumnComparisonResult] = Seq.empty,
    thresholdValidationResults: Seq[ThresholdValidationResult] = Seq.empty,
    businessRuleValidationResult: Option[DataFrame] = None, // Mismatches from business rule
    status: String = "SUCCESS"
)

case class SchemaDriftResult(
    missingInTarget: Set[String],
    extraInTarget: Set[String],
    typeMismatches: Set[String]
)

case class ExtraMissingResult(
    missingInTarget: DataFrame,
    extraInSource: DataFrame
)

case class ColumnComparisonResult(
    sourceColumn: String,
    targetColumn: String,
    mismatches: DataFrame
)

case class ThresholdValidationResult(
    columnName: String,
    thresholdType: String,
    thresholdValue: Double,
    breaches: DataFrame
)
