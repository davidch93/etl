package com.github.davidch93.etl.core.utils;

import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class EmailUtilsTest {

    @BeforeAll
    static void setup() {
        System.setProperty("spark.yarn.appMasterEnv.SMTP_HOST", "smtp.example.com");
        System.setProperty("spark.yarn.appMasterEnv.SMTP_PORT", "587");
        System.setProperty("spark.yarn.appMasterEnv.SMTP_STARTTLS", "true");
        System.setProperty("spark.yarn.appMasterEnv.SMTP_SSL", "false");
        System.setProperty("spark.yarn.appMasterEnv.SMTP_MAIL_SENDER", "sender@example.com");
        System.setProperty("spark.yarn.appMasterEnv.MAILING_LIST", "recipient@example.com");
        System.setProperty("spark.yarn.appMasterEnv.SMTP_USER", "user@example.com");
        System.setProperty("spark.yarn.appMasterEnv.SMTP_PASSWORD",
            Base64.getEncoder().encodeToString("test-password".getBytes()));
    }

    @Test
    void testConstructProperties() {
        Properties properties = EmailUtils.constructProperties();

        assertThat(properties.get("mail.smtp.host")).isEqualTo("smtp.example.com");
        assertThat(properties.get("mail.smtp.port")).isEqualTo("587");
        assertThat(properties.get("mail.smtp.starttls.enable")).isEqualTo("true");
        assertThat(properties.get("mail.smtp.ssl.enable")).isEqualTo("false");
    }

    @Test
    void testCreateMimeMessage() throws Exception {
        Properties properties = new Properties();
        Session session = Session.getInstance(properties);

        MimeMessage mimeMessage = EmailUtils.createMimeMessage(session, "Test Subject", "Test Body");

        assertThat(mimeMessage.getFrom()[0].toString()).isEqualTo("sender@example.com");
        assertThat(mimeMessage.getAllRecipients()[0].toString()).isEqualTo("recipient@example.com");
        assertThat(mimeMessage.getSubject()).isEqualTo("Test Subject");
        assertThat(mimeMessage.getContent().toString()).isEqualTo("Test Body");
    }

    @Test
    void testDecodePassword() {
        String decodedPassword = EmailUtils.decodePassword();
        assertThat(decodedPassword).isEqualTo("test-password");
    }
}