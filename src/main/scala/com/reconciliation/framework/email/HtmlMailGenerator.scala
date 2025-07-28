package com.reconciliation.framework.email

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import com.reconciliation.framework.util.DataFrameConverter
import java.util.Properties
import javax.mail._
import javax.mail.internet._

class HtmlMailGenerator(config: ReconciliationConfig) {

  def sendEmail(result: ReconciliationResult): Unit = {
    val props = new Properties()
    props.put("mail.smtp.host", "your_smtp_host")
    props.put("mail.smtp.port", "your_smtp_port")
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")

    val session = Session.getInstance(props, null)

    try {
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress("no-reply@reconciliation.framework"))
      val recipients = config.emailRecipients.map(new InternetAddress(_)).toArray[Address]
      msg.setRecipients(Message.RecipientType.TO, recipients)
      msg.setSubject(s"${config.emailSubject} - ${result.status}")
      msg.setContent(buildHtmlBody(result), "text/html")

      Transport.send(msg)
      println("Email report generated and sent.")

    } catch {
      case e: MessagingException => e.printStackTrace()
    }
  }

  private def buildHtmlBody(result: ReconciliationResult): String = {
    val statusColor = if (result.status == "SUCCESS") "green" else "red"
    s"""
    <html>
      <head><style>
        table { font-family: arial, sans-serif; border-collapse: collapse; width: 100%; }
        td, th { border: 1px solid #dddddd; text-align: left; padding: 8px; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .status { color: $statusColor; }
      </style></head>
      <body>
        <h2>Reconciliation Report for ${result.jobName}</h2>
        <p><strong>Status: <span class="status">${result.status}</span> | Execution Time (seconds): ${(result.endTime - result.startTime) / 1000}</strong></p>
        ${buildDetailsTable(result)}
      </body>
    </html>
    """
  }

  private def buildDetailsTable(result: ReconciliationResult): String = {
    val sb = new StringBuilder
    sb.append("<table>")
    sb.append("<tr><th>Check</th><th>Result</th></tr>")

    // Count Reconciliation
    sb.append(s"<tr><td>Count Reconciliation</td><td>${if (result.countMatch) "Matched" else "Mismatched"} (Source: ${result.sourceCount}, Target: ${result.targetCount})</td></tr>")

    // Schema Drift
    if (config.doSchemaDriftDetection) {
      val driftResult = result.schemaDriftResults.map { res =>
        if (res.missingInTarget.isEmpty && res.extraInTarget.isEmpty && res.typeMismatches.isEmpty) {
          "No drift detected"
        } else {
          s"Missing in Target: ${res.missingInTarget.mkString(", ")}<br>" +
          s"Extra in Target: ${res.extraInTarget.mkString(", ")}<br>" +
          s"Type Mismatches: ${res.typeMismatches.mkString(", ")}"
        }
      }.getOrElse("Not Performed")
      sb.append(s"<tr><td>Schema Drift</td><td>$driftResult</td></tr>")
    }

    // Extra/Missing Check
    if (config.doExtraMissingCheck) {
      val extraMissingResult = result.extraMissingResult.map { res =>
        s"Missing in Target: ${res.missingInTarget.count()}<br>" +
        s"Extra in Source: ${res.extraInSource.count()}"
      }.getOrElse("Not Performed")
      sb.append(s"<tr><td>Extra/Missing Records</td><td>$extraMissingResult</td></tr>")
    }

    // Column Comparison
    if (config.doColumnComparison) {
      val columnComparisonResult = result.columnComparisonResults.map { res =>
        s"${res.sourceColumn} vs ${res.targetColumn}: ${res.mismatches.count()} mismatches"
      }.mkString("<br>")
      sb.append(s"<tr><td>Column Comparison</td><td>${if (columnComparisonResult.isEmpty) "No mismatches" else columnComparisonResult}</td></tr>")
    }

    // Threshold Validation
    if (config.doThresholdValidation) {
      val thresholdResult = result.thresholdValidationResults.map { res =>
        s"${res.columnName}: ${res.breaches.count()} breaches"
      }.mkString("<br>")
      sb.append(s"<tr><td>Threshold Validation</td><td>${if (thresholdResult.isEmpty) "No breaches" else thresholdResult}</td></tr>")
    }

    // Business Rule Validation
    if (config.doBusinessRuleValidation) {
      val businessRuleResult = result.businessRuleValidationResult.map { res =>
        s"${res.count()} mismatches"
      }.getOrElse("Not Performed")
      sb.append(s"<tr><td>Business Rule Validation</td><td>$businessRuleResult</td></tr>")
    }

    sb.append("</table>")
    sb.toString()
  }
}
