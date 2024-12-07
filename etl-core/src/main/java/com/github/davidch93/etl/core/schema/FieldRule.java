package com.github.davidch93.etl.core.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a validation rule applied to a field.
 * <p>
 * This class defines a rule for validating field values and allows for additional hints
 * and assertions to guide data processing and ensure data quality.
 * </p>
 *
 * <p><strong>Rules:</strong></p>
 * <ul>
 *   <li>HAS_COMPLETENESS: Validates the completeness of the field.</li>
 *   <li>IS_COMPLETE: Ensures the field is complete.</li>
 *   <li>IS_CONTAINED_IN: Checks if the field value is contained in a predefined list.</li>
 *   <li>IS_NON_NEGATIVE: Validates that the field value is non-negative.</li>
 *   <li>IS_PRIMARY_KEY: Ensures the field is a primary key.</li>
 *   <li>IS_UNIQUE: Validates the uniqueness of the field value.</li>
 * </ul>
 *
 * @author david.christianto
 */
public class FieldRule implements Serializable {

    /**
     * Enumeration of validation rules.
     */
    public enum Rule {
        HAS_COMPLETENESS,
        IS_COMPLETE,
        IS_CONTAINED_IN,
        IS_NON_NEGATIVE,
        IS_PRIMARY_KEY,
        IS_UNIQUE
    }

    @JsonProperty(value = "rule", required = true)
    private Rule rule;

    @JsonProperty(value = "hint", required = true)
    private String hint;

    @JsonProperty(value = "assertion")
    private Double assertion;

    @JsonProperty(value = "values")
    private List<String> values;

    /**
     * Retrieves the rule applied to the field.
     *
     * @return the {@link Rule} enum value representing the type of rule applied (e.g., IS_PRIMARY_KEY, IS_UNIQUE).
     */
    public Rule getRule() {
        return rule;
    }

    /**
     * Retrieves the hint or message associated with the rule.
     *
     * @return the hint as a {@link String}, providing context or guidance about the rule.
     */
    public String getHint() {
        return hint;
    }

    /**
     * Retrieves the assertion value associated with the rule.
     *
     * @return a {@link Double} representing the threshold or value required for rule validation, or {@code null} if not applicable.
     */
    public Double getAssertion() {
        return assertion;
    }

    /**
     * Retrieves the list of allowed or expected values for the rule.
     *
     * @return a list of {@link String} values representing the valid options or constraints for the rule, or {@code null} if not applicable.
     */
    public List<String> getValues() {
        return values;
    }
}
