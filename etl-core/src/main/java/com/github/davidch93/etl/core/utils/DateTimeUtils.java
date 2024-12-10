package com.github.davidch93.etl.core.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Utility class for handling various date and time operations.
 * <p>
 * This class provides methods for parsing, formatting, and manipulating date-time values
 * using Java's {@link java.time} API.
 * </p>
 *
 * <p><b>Note:</b> This class is immutable and thread-safe.</p>
 * <p>
 * Example usage:
 * <pre>{@code
 * // Convert ISO 8601 string to LocalDateTime
 * LocalDateTime dateTime = DateTimeUtils.parseIsoLocalDateTime("2024-12-01T12:34:56");
 *
 * // Get the start of the day in a specific time zone
 * ZonedDateTime startOfDay = DateTimeUtils.getStartOfDay("2024-12-01T12:34:56", ZoneId.of("Asia/Jakarta"));
 *
 * // Format epoch milliseconds into a custom date-time string
 * String formatted = DateTimeUtils.formatEpochMillis(1704065696000L, "yyyy-MM-dd HH:mm:ss", ZoneId.of("UTC"));
 * }</pre>
 *
 * @author david.christianto
 */
public final class DateTimeUtils {

    /**
     * Parses an ISO 8601 date-time string into a {@link LocalDateTime}.
     *
     * @param isoDateTimeString the ISO 8601 date-time string (e.g., "2024-12-01T12:34:56")
     * @return the corresponding {@link LocalDateTime}
     * @throws NullPointerException                    if {@code isoDateTimeString} is null
     * @throws java.time.format.DateTimeParseException if the string cannot be parsed
     */
    public static LocalDateTime parseIsoLocalDateTime(String isoDateTimeString) {
        Objects.requireNonNull(isoDateTimeString, "ISO date-time string cannot be null!");
        return LocalDateTime.parse(isoDateTimeString, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    /**
     * Calculates the start of the day for the given date-time string in the specified time zone.
     *
     * @param isoDateTimeString the ISO 8601 date-time string
     * @param zoneId            the time zone to consider
     * @return the start of the day as a {@link ZonedDateTime}
     * @throws NullPointerException if {@code isoDateTimeString} or {@code zoneId} is null
     */
    public static ZonedDateTime getStartOfDay(String isoDateTimeString, ZoneId zoneId) {
        Objects.requireNonNull(zoneId, "ZoneId cannot be null!");
        LocalDateTime localDateTime = parseIsoLocalDateTime(isoDateTimeString);
        return localDateTime.toLocalDate().atStartOfDay(zoneId);
    }

    /**
     * Formats a given {@link Instant} into a formatted date-time string
     * in UTC with the specified format.
     *
     * @param instant the {@link Instant} to format
     * @param format  the desired date-time format (e.g., "yyyy-MM-dd HH:mm:ss")
     * @return the formatted date-time string
     * @throws NullPointerException if {@code instant} or {@code format} is null
     */
    public static String formatInstant(Instant instant, String format) {
        return formatInstant(instant, format, ZoneId.of("UTC"));
    }

    /**
     * Formats a given {@link Instant} into a formatted date-time string
     * in the specified time zone and format.
     *
     * @param instant the {@link Instant} to format
     * @param format  the desired date-time format (e.g., "yyyy-MM-dd HH:mm:ss")
     * @param zoneId  the time zone to consider
     * @return the formatted date-time string
     * @throws NullPointerException if {@code instant}, {@code format}, or {@code zoneId} is null
     */
    public static String formatInstant(Instant instant, String format, ZoneId zoneId) {
        Objects.requireNonNull(instant, "Instant cannot be null!");
        Objects.requireNonNull(format, "Format cannot be null!");
        Objects.requireNonNull(zoneId, "ZoneId cannot be null!");

        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return zonedDateTime.format(formatter);
    }
}