package org.openphc.cce.collector.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventTypeValidator — strict org.openphc.cce.&lt;resource&gt; pattern validation.
 */
class EventTypeValidatorTest {

    private final EventTypeValidator validator = new EventTypeValidator();

    @ParameterizedTest
    @ValueSource(strings = {
            "org.openphc.cce.encounter",
            "org.openphc.cce.observation",
            "org.openphc.cce.condition",
            "org.openphc.cce.medicationrequest",
            "org.openphc.cce.medicationdispense",
            "org.openphc.cce.servicerequest",
            "org.openphc.cce.procedure",
            "org.openphc.cce.episodeofcare",
            "org.openphc.cce.diagnosticreport",
            "org.openphc.cce.immunization",
            "org.openphc.cce.allergyintolerance",
            "org.openphc.cce.careplan",
            "org.openphc.cce.patient"
    })
    void shouldAcceptValidEventTypes(String type) {
        assertTrue(validator.isValid(type), "Expected valid: " + type);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "cce.encounter.created",
            "cce.observation.updated",
            "custom.event.type",
            "org.openphc.cce.",
            "org.openphc.cce.Encounter",
            "org.openphc.cce.encounter.created",
            "encounter",
            "org.openphc.encounter"
    })
    void shouldRejectInvalidEventTypes(String type) {
        assertFalse(validator.isValid(type), "Expected invalid: " + type);
    }

    @Test
    void shouldRejectNullType() {
        assertFalse(validator.isValid(null));
    }

    @Test
    void shouldRejectBlankType() {
        assertFalse(validator.isValid(""));
        assertFalse(validator.isValid("   "));
    }
}
