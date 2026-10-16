package com.example.paymentevent.exception;

/**
 * A request that is valid but could not be completed because a row it needs was locked for
 * longer than lock_timeout (Postgres SQLState 55P03). Nothing was written; the same request
 * can simply be sent again. Mapped to 503 service-busy with a Retry-After header.
 */
public class ServiceBusyException extends RuntimeException {

    public ServiceBusyException(String message, Throwable cause) {
        super(message, cause);
    }
}
