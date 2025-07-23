package com.reconciliation.framework

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: spark-submit --class com.reconciliation.framework.Main <jar_file> <job_name>")
      System.exit(1)
    }
    val jobName = args(0)
    // In a real implementation, you would trigger the ReconciliationEngine here
    println(s"Starting reconciliation job: $jobName")
  }
}
