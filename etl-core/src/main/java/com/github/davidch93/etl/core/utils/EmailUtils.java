package com.github.davidch93.etl.core.utils;

import com.sun.mail.smtp.SMTPTransport;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

/**
 * Utility class for sending emails using SMTP.
 * Configurations are expected to be provided via system properties.
 *
 * @author david.christianto
 */
public final class EmailUtils {

    private static final Logger logger = LoggerFactory.getLogger(EmailUtils.class);

    private static final String DEFAULT_PROTOCOL = "smtp";
    private static final String PREFIX = "spark.yarn.appMasterEnv";
    private static final String SMTP_HOST = System.getProperty(PREFIX + ".SMTP_HOST");
    private static final String SMTP_PORT = System.getProperty(PREFIX + ".SMTP_PORT");
    private static final String SMTP_STARTTLS = System.getProperty(PREFIX + ".SMTP_STARTTLS");
    private static final String SMTP_SSL = System.getProperty(PREFIX + ".SMTP_SSL");
    private static final String SMTP_MAIL_SENDER = System.getProperty(PREFIX + ".SMTP_MAIL_SENDER");
    private static final String SMTP_PASSWORD = System.getProperty(PREFIX + ".SMTP_PASSWORD");
    private static final String SMTP_USER = System.getProperty(PREFIX + ".SMTP_USER");
    private static final String MAILING_LIST = System.getProperty(PREFIX + ".MAILING_LIST");

    /**
     * Sends an email using the configured SMTP settings.
     *
     * @param subject the subject of the email.
     * @param message the body of the email.
     */
    public static void sendEmail(String subject, String message) {
        Properties properties = constructProperties();
        Session session = Session.getInstance(properties);

        try (SMTPTransport transport = (SMTPTransport) session.getTransport(DEFAULT_PROTOCOL)) {
            MimeMessage mimeMessage = createMimeMessage(session, subject, message);
            transport.connect(SMTP_HOST, SMTP_USER, decodePassword());
            transport.sendMessage(mimeMessage, mimeMessage.getAllRecipients());
            logger.info("[ETL-CORE] Email sent successfully.");
        } catch (MessagingException e) {
            logger.error("[ETL-CORE] Failed to send email!", e);
        }
    }

    /**
     * Constructs SMTP configuration properties.
     *
     * @return a {@link Properties} object containing SMTP settings.
     */
    static Properties constructProperties() {
        Properties properties = new Properties();
        properties.put("mail.smtp.host", SMTP_HOST);
        properties.put("mail.smtp.port", SMTP_PORT);
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", SMTP_STARTTLS);
        properties.put("mail.smtp.ssl.enable", SMTP_SSL);
        return properties;
    }

    /**
     * Creates a MIME message with the specified subject and message body.
     *
     * @param session the mail session.
     * @param subject the subject of the email.
     * @param message the body of the email.
     * @return a {@link MimeMessage} instance.
     * @throws MessagingException if an error occurs during message creation.
     */
    static MimeMessage createMimeMessage(Session session, String subject, String message) throws MessagingException {
        MimeMessage mimeMessage = new MimeMessage(session);
        mimeMessage.setFrom(new InternetAddress(SMTP_MAIL_SENDER));
        mimeMessage.setRecipients(Message.RecipientType.TO, MAILING_LIST);
        mimeMessage.setSubject(subject);
        mimeMessage.setText(message);
        return mimeMessage;
    }

    /**
     * Decodes the base64-encoded SMTP password.
     *
     * @return the decoded SMTP password as a plain string.
     */
    static String decodePassword() {
        byte[] decodedBytes = Base64.getDecoder().decode(SMTP_PASSWORD);
        return new String(decodedBytes, StandardCharsets.UTF_8);
    }
}