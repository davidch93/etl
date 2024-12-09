package com.github.davidch93.etl.core.utils;

import org.junit.jupiter.api.Test;

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
    void testFormatEpochMillis_withDefaultZone() {
        long epochMillis = 1733056496000L; // 2024-12-01T12:34:56 UTC
        String format = "yyyy-MM-dd HH:mm:ss";
        String result = DateTimeUtils.formatEpochMillis(epochMillis, format);
        assertThat(result).isEqualTo("2024-12-01 12:34:56");
    }

    @Test
    void testFormatEpochMillis_withCustomZone() {
        long epochMillis = 1733056496000L; // 2024-12-01T12:34:56 UTC
        String format = "yyyy-MM-dd HH:mm:ss";
        ZoneId zoneId = ZoneId.of("Asia/Jakarta");
        String result = DateTimeUtils.formatEpochMillis(epochMillis, format, zoneId);
        assertThat(result).isEqualTo("2024-12-01 19:34:56");
    }

    @Test
    void testFormatEpochMillis_withNullFormat_expectThrowsException() {
        assertThatThrownBy(() -> DateTimeUtils.formatEpochMillis(1704065696000L, null, ZoneId.of("UTC")))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("Format cannot be null!");
    }

    @Test
    void testFormatEpochMillis_withNullZoneId_expectThrowsException() {
        assertThatThrownBy(() -> DateTimeUtils.formatEpochMillis(1704065696000L, "yyyy-MM-dd", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("ZoneId cannot be null!");
    }
}