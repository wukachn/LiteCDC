package io.github.wukachn.litecdc.engine;

import io.github.wukachn.litecdc.util.EnvironmentVariableHandler;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmailHandler {
  private final Session session;
  private final String senderEmail;
  private final List<String> recipientList;

  public EmailHandler(String senderEmail, String senderPassword, List<String> recipientList) {
    var properties = new Properties();
    properties.setProperty("mail.smtp.host", "smtp.gmail.com");
    properties.setProperty("mail.smtp.port", "587");
    properties.setProperty("mail.smtp.auth", "true");
    properties.setProperty("mail.smtp.starttls.enable", "true");
    this.session =
        Session.getInstance(
            properties,
            new Authenticator() {
              protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(
                    senderEmail, EnvironmentVariableHandler.get(senderPassword));
              }
            });
    this.senderEmail = senderEmail;
    this.recipientList = recipientList;
  }

  public void sendEmail(Exception triggerEx) {
    log.info("Attempting to send failure notification emails.");
    try {
      var message = new MimeMessage(session);
      message.setFrom(new InternetAddress(senderEmail));
      for (String recipient : recipientList) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
      }
      message.setSubject("Change Data Capture Pipeline Failure");
      message.setText(getEmailContent(triggerEx));
      Transport.send(message);
      log.info("Pipeline failure notification email(s) sent successfully.");
    } catch (Exception e) {
      log.error("Failed to send failure notification emails.", e);
    }
  }

  private String getEmailContent(Exception e) {
    var stringBuilder = new StringBuilder();
    stringBuilder.append("Your pipeline has stopped.\n\nSee overview below:\n");
    stringBuilder.append(e.getMessage() + " ");
    for (var cause = e.getCause(); cause != null; cause = cause.getCause()) {
      stringBuilder.append(cause.getMessage() + " ");
    }
    stringBuilder.append("\n\nView logs for more details.");
    return stringBuilder.toString();
  }
}
