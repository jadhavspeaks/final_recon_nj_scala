package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import org.apache.spark.sql.SparkSession
import com.reconciliation.framework.reader.DataReader
import com.reconciliation.framework.audit.AuditWriter
import com.reconciliation.framework.email.HtmlMailGenerator
import com.reconciliation.framework.util.Logging

class ReconciliationEngine(spark: SparkSession, config: ReconciliationConfig) extends Logging {

  def run(): Unit = {
    log.info(s"Starting reconciliation job: ${config.jobName}")
    var result: ReconciliationResult = null

    try {
      val reconExecutor = new ReconciliationTypeExecutor(spark, config)

      result = if (config.reconMode == "SOURCE_TO_TARGET") {
        val (sourceDF, targetDF) = DataReader.read(spark, config)
        reconExecutor.execute(sourceDF, targetDF.get)
      } else { // SOURCE_TO_SQL
        val (sourceDF, _) = DataReader.read(spark, config)
        val sqlExecutor = new SqlExecutor(spark, config)
        val derivedDF = sqlExecutor.execute()
        reconExecutor.executeBusinessRuleValidation(sourceDF, derivedDF)
      }

      val finalStatus = if (result.countMatch && result.schemaDriftResults.forall(r => r.missingInTarget.isEmpty && r.extraInTarget.isEmpty && r.typeMismatches.isEmpty) && result.extraMissingResult.forall(r => r.missingInTarget.isEmpty && r.extraInSource.isEmpty) && result.columnComparisonResults.forall(_.mismatches.isEmpty) && result.thresholdValidationResults.forall(_.breaches.isEmpty)) "SUCCESS" else "FAILURE"
      result = result.copy(status = finalStatus, endTime = System.currentTimeMillis())

      log.info(s"Reconciliation finished with status: ${result.status}")

      val auditWriter = new AuditWriter(spark, config)
      auditWriter.writeAuditLog(result)

      val mailGenerator = new HtmlMailGenerator(config)
      mailGenerator.sendEmail(result)

    } catch {
      case e: Exception =>
        log.error(s"Error running reconciliation job: ${config.jobName}", e)
        // Optionally, send a failure email
    } finally {
      log.info(s"Finished reconciliation job: ${config.jobName}")
    }
  }
}
