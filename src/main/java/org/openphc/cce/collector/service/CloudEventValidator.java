package org.openphc.cce.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates CloudEvents v1.0 envelope — mandatory fields, specversion, format constraints.
 * Aggregates all validation errors before throwing.
 */
@Component
@Slf4j
public class CloudEventValidator {

    private static final String REQUIRED_SPEC_VERSION = "1.0";
    private static final int MAX_ID_LENGTH = 256;

    /**
     * Validate the CloudEvents envelope. Throws CloudEventValidationException
     * with all aggregated errors on failure.
     */
    public void validate(EventIngestionRequest request) {
        List<String> errors = new ArrayList<>();

        // specversion must be "1.0"
        if (request.getSpecversion() == null || request.getSpecversion().isBlank()) {
            errors.add("Missing required CloudEvents field: 'specversion'");
        } else if (!REQUIRED_SPEC_VERSION.equals(request.getSpecversion())) {
            errors.add("specversion must be '1.0', got '" + request.getSpecversion() + "'");
        }

        // id — required, non-empty, max 256 chars
        if (request.getId() == null || request.getId().isBlank()) {
            errors.add("Missing required CloudEvents field: 'id'");
        } else if (request.getId().length() > MAX_ID_LENGTH) {
            errors.add("CloudEvents 'id' exceeds max length of " + MAX_ID_LENGTH + " characters");
        }

        // source — required, non-empty
        if (request.getSource() == null || request.getSource().isBlank()) {
            errors.add("Missing required CloudEvents field: 'source'");
        }

        // type — required, non-empty
        if (request.getType() == null || request.getType().isBlank()) {
            errors.add("Missing required CloudEvents field: 'type'");
        }

        // subject — required by CCE (patient UPID)
        if (request.getSubject() == null || request.getSubject().isBlank()) {
            errors.add("Missing required CloudEvents field: 'subject' (patient UPID required by CCE)");
        }

        // data — required by CCE
        if (request.getData() == null || request.getData().isEmpty()) {
            errors.add("Missing required CloudEvents field: 'data'");
        }

        if (!errors.isEmpty()) {
            throw new CloudEventValidationException(
                    "CloudEvents envelope validation failed: " + String.join("; ", errors), errors);
        }

        log.debug("CloudEvents envelope validation passed for event id={}", request.getId());
    }
}
