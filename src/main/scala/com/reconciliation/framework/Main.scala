package com.reconciliation.framework

import org.apache.spark.sql.SparkSession
import com.reconciliation.framework.config.OracleConfigLoader
import com.reconciliation.framework.core.ReconciliationEngine
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: spark-submit --class com.reconciliation.framework.Main <jar_file> <job_name>")
      System.exit(1)
    }
    val jobName = args(0)

    val spark = SparkSession.builder()
      .appName(s"Reconciliation Framework: $jobName")
      .enableHiveSupport()
      .getOrCreate()

    // These should be passed securely, e.g., via command line args or a secrets manager
    val jdbcUrl = "jdbc:oracle:thin:@//host:port/service"
    val dbProperties = new Properties()
    dbProperties.setProperty("user", "user")
    dbProperties.setProperty("password", "password")
    dbProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")

    val configLoader = new OracleConfigLoader(jdbcUrl, dbProperties)
    val config = configLoader.loadConfig(jobName)

    val engine = new ReconciliationEngine(spark, config)
    engine.run()

    spark.stop()
  }
}
