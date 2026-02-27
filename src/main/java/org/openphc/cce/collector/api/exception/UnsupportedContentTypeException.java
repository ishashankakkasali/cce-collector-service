package org.openphc.cce.collector.api.exception;

import lombok.Getter;

import java.util.List;

/**
 * Exception thrown when the datacontenttype is not supported.
 */
@Getter
public class UnsupportedContentTypeException extends RuntimeException {

    private final String contentType;
    private final List<String> errors;

    public UnsupportedContentTypeException(String contentType, String message, List<String> errors) {
        super(message);
        this.contentType = contentType;
        this.errors = errors;
    }
}
