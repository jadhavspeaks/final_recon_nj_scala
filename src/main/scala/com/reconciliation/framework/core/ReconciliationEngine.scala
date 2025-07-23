package com.reconciliation.framework.core

import com.reconciliation.framework.config.ReconciliationConfig
import org.apache.spark.sql.SparkSession
import com.reconciliation.framework.reader.DataReader
import com.reconciliation.framework.audit.AuditWriter
import com.reconciliation.framework.email.HtmlMailGenerator

class ReconciliationEngine(spark: SparkSession, config: ReconciliationConfig) {

  def run(): Unit = {
    println(s"Running reconciliation job: ${config.jobName}")

    val reconExecutor = new ReconciliationTypeExecutor(spark, config)

    if (config.reconMode == "SOURCE_TO_TARGET") {
      val (sourceDF, targetDF) = DataReader.read(spark, config)
      reconExecutor.execute(sourceDF, targetDF.get)
    } else { // SOURCE_TO_SQL
      val (sourceDF, _) = DataReader.read(spark, config)
      val sqlExecutor = new SqlExecutor(spark, config)
      val derivedDF = sqlExecutor.execute()
      reconExecutor.executeBusinessRuleValidation(sourceDF, derivedDF)
    }

    // In a real implementation, you would collect results and pass them to the audit writer and email generator.
    // val auditWriter = new AuditWriter(spark, config)
    // auditWriter.writeSummary(...)
    // auditWriter.writeMismatches(...)

    // val mailGenerator = new HtmlMailGenerator(config)
    // mailGenerator.sendEmail(...)

    println(s"Finished reconciliation job: ${config.jobName}")
  }
}
