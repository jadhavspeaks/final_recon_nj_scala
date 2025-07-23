package com.reconciliation.framework.email

import com.reconciliation.framework.config.ReconciliationConfig
import java.util.Properties
import javax.mail._
import javax.mail.internet._

class HtmlMailGenerator(config: ReconciliationConfig) {

  def sendEmail(reportBody: String): Unit = {
    val props = new Properties()
    // Configure mail server properties here (e.g., mail.smtp.host)

    val session = Session.getInstance(props, null)

    try {
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress("no-reply@reconciliation.framework"))
      val recipients = config.emailRecipients.map(new InternetAddress(_)).toArray
      msg.setRecipients(Message.RecipientType.TO, recipients)
      msg.setSubject(config.emailSubject)
      msg.setContent(reportBody, "text/html")

      // Transport.send(msg) // Uncomment to send
      println("Email report generated. (Sending is disabled in this skeleton).")

    } catch {
      case e: MessagingException => e.printStackTrace()
    }
  }
}
