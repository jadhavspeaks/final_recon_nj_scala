package com.reconciliation.framework.email

import com.reconciliation.framework.config.ReconciliationConfig
import com.reconciliation.framework.model.ReconciliationResult
import java.util.Properties
import javax.mail._
import javax.mail.internet._

class HtmlMailGenerator(config: ReconciliationConfig) {

  def sendEmail(result: ReconciliationResult): Unit = {
    val props = new Properties()
    // Configure mail server properties here (e.g., mail.smtp.host)

    val session = Session.getInstance(props, null)

    try {
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress("no-reply@reconciliation.framework"))
      val recipients = config.emailRecipients.map(new InternetAddress(_)).toArray
      msg.setRecipients(Message.RecipientType.TO, recipients)
      msg.setSubject(s"${config.emailSubject} - ${result.status}")
      msg.setContent(buildHtmlBody(result), "text/html")

      // Transport.send(msg) // Uncomment to send
      println("Email report generated. (Sending is disabled in this skeleton).")

    } catch {
      case e: MessagingException => e.printStackTrace()
    }
  }

  private def buildHtmlBody(result: ReconciliationResult): String = {
    s"""
    <html>
      <head><style>
        table { font-family: arial, sans-serif; border-collapse: collapse; width: 100%; }
        td, th { border: 1px solid #dddddd; text-align: left; padding: 8px; }
        tr:nth-child(even) { background-color: #f2f2f2; }
      </style></head>
      <body>
        <h2>Reconciliation Report for ${result.jobName}</h2>
        <p><strong>Status: ${result.status}</strong></p>
        <h3>Summary</h3>
        <table>
          <tr><th>Metric</th><th>Value</th></tr>
          <tr><td>Source Count</td><td>${result.sourceCount}</td></tr>
          <tr><td>Target Count</td><td>${result.targetCount}</td></tr>
          <tr><td>Count Match</td><td>${if(result.countMatch) "Yes" else "No"}</td></tr>
        </table>
        ${buildMismatchDetails(result)}
      </body>
    </html>
    """
  }

  private def buildMismatchDetails(result: ReconciliationResult): String = {
    // In a real implementation, you would iterate through the results and build detailed tables
    // for each type of mismatch. For brevity, this is a simplified example.
    val sb = new StringBuilder
    result.extraMissingResult.foreach { res =>
      sb.append("<h3>Extra/Missing Records</h3>")
      sb.append(s"<p>Missing in Target: ${res.missingInTarget.count()}</p>")
      sb.append(s"<p>Extra in Source: ${res.extraInSource.count()}</p>")
    }
    result.columnComparisonResults.filterNot(_.mismatches.isEmpty).foreach { res =>
      sb.append(s"<h3>Column Mismatches: ${res.sourceColumn}</h3>")
      sb.append(s"<p>Count: ${res.mismatches.count()}</p>")
    }
    sb.toString()
  }
}
