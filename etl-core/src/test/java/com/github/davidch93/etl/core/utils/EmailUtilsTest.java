package com.github.davidch93.etl.core.utils;

import com.sun.mail.smtp.SMTPTransport;
import jakarta.mail.Address;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Base64;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EmailUtilsTest {

    private Session mockSession;
    private SMTPTransport mockTransport;

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

    @BeforeEach
    void setUp() {
        mockSession = mock(Session.class);
        mockTransport = mock(SMTPTransport.class);
    }

    @Test
    void testSendEmailSuccess() throws Exception {
        try (MockedStatic<Session> mockedSession = mockStatic(Session.class)) {
            mockedSession.when(() -> Session.getInstance(any(Properties.class))).thenReturn(mockSession);

            when(mockSession.getTransport(eq("smtp"))).thenReturn(mockTransport);
            when(mockSession.getProperties()).thenReturn(new Properties());

            EmailUtils.sendEmail("Test Subject", "Test Message");

            verify(mockTransport).connect("smtp.example.com", "user@example.com", "test-password");
            verify(mockTransport).sendMessage(any(MimeMessage.class), any(Address[].class));
            verify(mockTransport).close();
        }
    }

    @Test
    void testSendEmailFailure() throws Exception {
        try (MockedStatic<Session> mockedSession = mockStatic(Session.class)) {
            mockedSession.when(() -> Session.getInstance(any(Properties.class))).thenReturn(mockSession);

            when(mockSession.getTransport(eq("smtp"))).thenReturn(mockTransport);
            when(mockSession.getProperties()).thenReturn(new Properties());
            doThrow(new MessagingException("SMTP Connection Failed"))
                .when(mockTransport)
                .connect(anyString(), anyString(), anyString());

            EmailUtils.sendEmail("Test Subject", "Test Message");

            verify(mockTransport).connect("smtp.example.com", "user@example.com", "test-password");
            verify(mockTransport, never()).sendMessage(any(), any());
            verify(mockTransport).close();
        }
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