package org.openphc.cce.collector.api.exception;

import lombok.Getter;

import java.util.List;

/**
 * Exception thrown when CloudEvents envelope validation fails.
 * Holds all aggregated validation errors.
 */
@Getter
public class CloudEventValidationException extends RuntimeException {

    private final List<String> errors;

    public CloudEventValidationException(String message, List<String> errors) {
        super(message);
        this.errors = errors;
    }
}
