package com.example.paymentevent.exception;

/**
 * POST /payments referenced an account that doesn't exist (foreign key violation on the
 * Payment insert). Unlike AccountNotFoundException, this is raised synchronously at the
 * REST layer and mapped to 422, not routed through Kafka/the DLT.
 */
public class UnknownAccountException extends RuntimeException {

    public UnknownAccountException(String accountId) {
        super("Unknown account: " + accountId);
    }

    public UnknownAccountException(String fromAccount, String toAccount) {
        super("Unknown account: " + fromAccount + " and/or " + toAccount);
    }
}
