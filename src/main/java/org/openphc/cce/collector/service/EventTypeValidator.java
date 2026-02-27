package org.openphc.cce.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

/**
 * Strictly validates that the event type matches org.openphc.cce.&lt;resource&gt; pattern.
 * The Collector does NOT normalize event types — normalization is the emitter adaptor's responsibility.
 */
@Component
@Slf4j
public class EventTypeValidator {

    private static final Pattern CCE_EVENT_TYPE_PATTERN =
            Pattern.compile("^org\\.openphc\\.cce\\.[a-z]+$");

    /**
     * Validate that the event type matches the required pattern.
     *
     * @param type the event type to validate
     * @return true if valid, false otherwise
     */
    public boolean isValid(String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        boolean valid = CCE_EVENT_TYPE_PATTERN.matcher(type).matches();
        if (!valid) {
            log.warn("Invalid event type '{}' — must match org.openphc.cce.<resource>", type);
        }
        return valid;
    }
}
