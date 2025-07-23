package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReconciliationTypeExecutor(spark: SparkSession, config: ReconciliationConfig) {

  def execute(source: DataFrame, target: DataFrame): Unit = {
    if (config.doCountReconciliation) {
      // Implement count reconciliation
    }
    if (config.doSchemaDriftDetection) {
      // Implement schema drift detection
    }
    if (config.doExtraMissingCheck) {
      // Implement extra/missing record check
    }
    if (config.doColumnComparison) {
      // Implement column-level comparison
    }
    if (config.doThresholdValidation) {
      // Implement threshold validation
    }
  }

  def executeBusinessRuleValidation(source: DataFrame): Unit = {
    if (config.doBusinessRuleValidation) {
      // Implement business rule validation using reconSql
    }
  }
}
