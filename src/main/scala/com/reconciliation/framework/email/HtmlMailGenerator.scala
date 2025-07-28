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
        <p><strong>Status: <span class="status">${result.status}</span></strong></p>
        <h3>Summary</h3>
        <table>
          <tr><th>Metric</th><th>Value</th></tr>
          <tr><td>Source Count</td><td>${result.sourceCount}</td></tr>
          <tr><td>Target Count</td><td>${result.targetCount}</td></tr>
          <tr><td>Count Match</td><td>${if(result.countMatch) "Yes" else "No"}</td></tr>
          <tr><td>Execution Time (seconds)</td><td>${(result.endTime - result.startTime) / 1000}</td></tr>
        </table>
        ${buildMismatchDetails(result)}
      </body>
    </html>
    """
  }

  private def buildMismatchDetails(result: ReconciliationResult): String = {
    val sb = new StringBuilder

    if (config.doSchemaDriftDetection) {
      sb.append("<h3>Schema Drift</h3>")
      result.schemaDriftResults.foreach { res =>
        sb.append(s"<p>Missing in Target: ${res.missingInTarget.mkString(", ")}</p>")
        sb.append(s"<p>Extra in Target: ${res.extraInTarget.mkString(", ")}</p>")
        sb.append(s"<p>Type Mismatches: ${res.typeMismatches.mkString(", ")}</p>")
      }
    }

    if (config.doExtraMissingCheck) {
      sb.append("<h3>Extra/Missing Records</h3>")
      result.extraMissingResult.foreach { res =>
        sb.append(s"<p>Missing in Target: ${res.missingInTarget.count()}</p>")
        if (res.missingInTarget.count() > 0 && res.missingInTarget.count() < 10) {
          sb.append(DataFrameConverter.toHtml(res.missingInTarget.limit(10)))
        }
        sb.append(s"<p>Extra in Source: ${res.extraInSource.count()}</p>")
        if (res.extraInSource.count() > 0 && res.extraInSource.count() < 10) {
          sb.append(DataFrameConverter.toHtml(res.extraInSource.limit(10)))
        }
      }
    }

    if (config.doColumnComparison) {
      sb.append("<h3>Column Comparison</h3>")
      result.columnComparisonResults.filterNot(_.mismatches.isEmpty).foreach { res =>
        sb.append(s"<h4>Mismatches for ${res.sourceColumn} vs ${res.targetColumn}</h4>")
        val mismatchCount = res.mismatches.count()
        sb.append(s"<p>Count: $mismatchCount</p>")
        if (mismatchCount > 0 && mismatchCount < 10) {
          sb.append(DataFrameConverter.toHtml(res.mismatches.limit(10)))
        }
      }
    }

    if (config.doThresholdValidation) {
      sb.append("<h3>Threshold Validation</h3>")
      result.thresholdValidationResults.filterNot(_.breaches.isEmpty).foreach { res =>
        sb.append(s"<h4>Breaches for ${res.columnName}</h4>")
        val breachCount = res.breaches.count()
        sb.append(s"<p>Count: $breachCount</p>")
        if (breachCount > 0 && breachCount < 10) {
          sb.append(DataFrameConverter.toHtml(res.breaches.limit(10)))
        }
      }
    }

    if (config.doBusinessRuleValidation) {
      sb.append("<h3>Business Rule Validation</h3>")
      result.businessRuleValidationResult.foreach { res =>
        val mismatchCount = res.count()
        sb.append(s"<p>Mismatches: $mismatchCount</p>")
        if (mismatchCount > 0 && mismatchCount < 10) {
          sb.append(DataFrameConverter.toHtml(res.limit(10)))
        }
      }
    }

    sb.toString()
  }
}
