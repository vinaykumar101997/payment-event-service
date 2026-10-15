package com.example.paymentevent.api;

/**
 * Every problem "type" the API returns (besides Spring MVC's own about:blank ones). The full
 * type URI is payments.problem-base-uri + slug; with the default base each slug is a heading
 * in docs/problems.md, and ProblemTypesDocumentedTest keeps the two in step.
 */
public enum ProblemType {
    VALIDATION_FAILED("validation-failed", "Validation failed"),
    MALFORMED_REQUEST("malformed-request", "Malformed request body"),
    INVALID_REQUEST("invalid-request", "Invalid payment request"),
    INVALID_AMOUNT("invalid-amount", "Invalid amount"),
    NOT_FOUND("not-found", "Resource not found"),
    DUPLICATE_PAYMENT("duplicate-payment", "Duplicate payment"),
    PAYMENT_ID_REUSED("payment-id-reused", "Payment id reused with a different payload"),
    UNKNOWN_ACCOUNT("unknown-account", "Unknown account");

    private final String slug;
    private final String title;

    ProblemType(String slug, String title) {
        this.slug = slug;
        this.title = title;
    }

    public String slug() {
        return slug;
    }

    public String title() {
        return title;
    }
}
