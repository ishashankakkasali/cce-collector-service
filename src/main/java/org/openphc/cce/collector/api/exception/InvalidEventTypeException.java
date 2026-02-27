package org.openphc.cce.collector.api.exception;

import lombok.Getter;

/**
 * Exception thrown when the event type does not match the required org.openphc.cce.&lt;resource&gt; pattern.
 */
@Getter
public class InvalidEventTypeException extends RuntimeException {

    private final String eventType;

    public InvalidEventTypeException(String eventType) {
        super("Invalid event type '" + eventType + "' — must match pattern org.openphc.cce.<resource>");
        this.eventType = eventType;
    }
}
