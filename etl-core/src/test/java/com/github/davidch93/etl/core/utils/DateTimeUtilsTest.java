package com.github.davidch93.etl.core.utils;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DateTimeUtilsTest {

    @Test
    void testParseIsoLocalDateTime_withValidInput() {
        String input = "2024-12-01T12:34:56";
        LocalDateTime expected = LocalDateTime.of(2024, 12, 1, 12, 34, 56);
        assertThat(DateTimeUtils.parseIsoLocalDateTime(input)).isEqualTo(expected);
    }

    @Test
    void testParseIsoLocalDateTime_withNullInput_expectThrowsException() {
        assertThatThrownBy(() -> DateTimeUtils.parseIsoLocalDateTime(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("ISO date-time string cannot be null!");
    }

    @Test
    void testGetStartOfDay_withValidInput() {
        String input = "2024-12-01T12:34:56";
        ZoneId zoneId = ZoneId.of("Asia/Jakarta");

        ZonedDateTime startOfDay = DateTimeUtils.getStartOfDay(input, zoneId);
        assertThat(startOfDay.toString()).isEqualTo("2024-12-01T00:00+07:00[Asia/Jakarta]");
    }

    @Test
    void testGetStartOfDay_withNullZoneId_expectThrowsException() {
        assertThatThrownBy(() -> DateTimeUtils.getStartOfDay("2024-12-01T12:34:56", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("ZoneId cannot be null!");
    }

    @Test
    void testFormatInstant_withDefaultZone() {
        Instant instant = Instant.parse("2024-12-10T12:34:56Z");
        String format = "yyyy-MM-dd HH:mm:ss";

        String result = DateTimeUtils.formatInstant(instant, format);
        assertThat(result).isEqualTo("2024-12-10 12:34:56");
    }

    @Test
    void testFormatInstant_withCustomZone() {
        Instant instant = Instant.parse("2024-12-10T12:34:56Z");
        String format = "yyyy-MM-dd HH:mm:ss";
        ZoneId zoneId = ZoneId.of("Asia/Jakarta");

        String result = DateTimeUtils.formatInstant(instant, format, zoneId);
        assertThat(result).isEqualTo("2024-12-10 19:34:56");
    }

    @Test
    void testFormatInstant_withNullInstant_expectThrowsException() {
        String format = "yyyy-MM-dd HH:mm:ss";
        ZoneId zoneId = ZoneId.of("UTC");

        assertThatThrownBy(() -> DateTimeUtils.formatInstant(null, format, zoneId))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Instant cannot be null!");
    }

    @Test
    void testFormatInstant_withNullFormat_expectThrowsException() {
        Instant instant = Instant.now();
        ZoneId zoneId = ZoneId.of("UTC");

        assertThatThrownBy(() -> DateTimeUtils.formatInstant(instant, null, zoneId))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Format cannot be null!");
    }

    @Test
    void testFormatInstant_withNullZoneId_expectThrowsException() {
        Instant instant = Instant.now();
        String format = "yyyy-MM-dd HH:mm:ss";

        assertThatThrownBy(() -> DateTimeUtils.formatInstant(instant, format, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("ZoneId cannot be null!");
    }
}