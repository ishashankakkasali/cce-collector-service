package org.openphc.cce.collector.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.fhir.FhirResourceValidator;
import org.openphc.cce.collector.fhir.FhirResourceValidator.ValidationResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Validates event payloads based on datacontenttype.
 * - application/fhir+json: Full FHIR R4 structural validation via HAPI
 * - application/json: JSON validity check (must be non-empty valid JSON object)
 * - Any other value: rejected with UNSUPPORTED_CONTENT_TYPE
 */
@Component
@Slf4j
public class FhirPayloadValidator {

    private static final String FHIR_CONTENT_TYPE = "application/fhir+json";
    private static final String JSON_CONTENT_TYPE = "application/json";

    private final FhirResourceValidator fhirResourceValidator;
    private final boolean fhirValidationEnabled;
    private final boolean strictMode;

    public FhirPayloadValidator(
            FhirResourceValidator fhirResourceValidator,
            @Value("${cce.collector.fhir-validation.enabled:true}") boolean fhirValidationEnabled,
            @Value("${cce.collector.fhir-validation.strict-mode:false}") boolean strictMode) {
        this.fhirResourceValidator = fhirResourceValidator;
        this.fhirValidationEnabled = fhirValidationEnabled;
        this.strictMode = strictMode;
    }

    /**
     * Validate payload based on the effective datacontenttype.
     * Returns a PayloadValidationResult indicating success or failure with the appropriate
     * rejection reason (INVALID_FHIR, INVALID_JSON, or UNSUPPORTED_CONTENT_TYPE).
     *
     * @param request         the inbound event request
     * @param dataContentType the effective (enriched) data content type
     * @return validation result
     */
    public PayloadValidationResult validate(EventIngestionRequest request, String dataContentType) {
        if (FHIR_CONTENT_TYPE.equals(dataContentType)) {
            return validateFhirPayload(request);
        } else if (JSON_CONTENT_TYPE.equals(dataContentType)) {
            return validateJsonPayload(request);
        } else {
            return PayloadValidationResult.failure(
                    RejectionReason.UNSUPPORTED_CONTENT_TYPE,
                    "Unsupported datacontenttype: '" + dataContentType + "'",
                    List.of("datacontenttype must be 'application/fhir+json' or 'application/json'"));
        }
    }

    /**
     * Validate FHIR R4 payload via HAPI FHIR.
     */
    private PayloadValidationResult validateFhirPayload(EventIngestionRequest request) {
        if (!fhirValidationEnabled) {
            log.debug("FHIR validation disabled, skipping for event id={}", request.getId());
            return PayloadValidationResult.success();
        }

        ValidationResult result = fhirResourceValidator.validate(request.getData(), request.getSubject());

        if (!result.valid()) {
            return PayloadValidationResult.failure(
                    RejectionReason.INVALID_FHIR,
                    "FHIR R4 payload validation failed",
                    result.errors());
        }

        if (!result.warnings().isEmpty()) {
            if (strictMode) {
                return PayloadValidationResult.failure(
                        RejectionReason.INVALID_FHIR,
                        "FHIR R4 payload validation warnings (strict mode)",
                        result.warnings());
            }
            result.warnings().forEach(w ->
                    log.warn("FHIR validation warning for event id={}: {}", request.getId(), w));
        }

        log.debug("FHIR payload validation passed for event id={}", request.getId());
        return PayloadValidationResult.success();
    }

    /**
     * Validate that the data is a non-empty valid JSON object.
     */
    private PayloadValidationResult validateJsonPayload(EventIngestionRequest request) {
        Map<String, Object> data = request.getData();
        if (data == null || data.isEmpty()) {
            return PayloadValidationResult.failure(
                    RejectionReason.INVALID_JSON,
                    "JSON payload validation failed",
                    List.of("data must be a non-empty JSON object"));
        }
        log.debug("JSON payload validation passed for event id={}", request.getId());
        return PayloadValidationResult.success();
    }

    /**
     * Result of payload validation with the appropriate rejection reason.
     */
    @Getter
    public static class PayloadValidationResult {
        private final boolean valid;
        private final RejectionReason rejectionReason;
        private final String message;
        private final List<String> errors;

        private PayloadValidationResult(boolean valid, RejectionReason rejectionReason,
                                         String message, List<String> errors) {
            this.valid = valid;
            this.rejectionReason = rejectionReason;
            this.message = message;
            this.errors = errors;
        }

        public static PayloadValidationResult success() {
            return new PayloadValidationResult(true, null, null, List.of());
        }

        public static PayloadValidationResult failure(RejectionReason reason, String message, List<String> errors) {
            return new PayloadValidationResult(false, reason, message, errors);
        }
    }
}
