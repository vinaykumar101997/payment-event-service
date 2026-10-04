package com.example.paymentevent.api;

import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.InvalidAmountException;
import com.example.paymentevent.exception.InvalidPaymentRequestException;
import com.example.paymentevent.exception.PaymentIdReusedException;
import com.example.paymentevent.exception.ResourceNotFoundException;
import com.example.paymentevent.exception.UnknownAccountException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Every API error is an RFC 9457 problem (application/problem+json). "type" is the stable,
 * machine-readable identifier clients should branch on; "title" names the kind of problem,
 * "detail" explains this occurrence, "instance" is the request path. The mapping:
 *  - 400 validation-failed  field validation (with an "errors" member: field -> message)
 *  - 400 malformed-request  body isn't parseable JSON for the request type
 *  - 400 invalid-request    well-formed but invalid as a whole (e.g. fromAccount == toAccount)
 *  - 400 invalid-amount     breaks a money rule (decimal places for the currency, maximum amount)
 *  - 404 not-found          unknown payment or account id
 *  - 409 duplicate-payment  identical replay of an existing paymentId (with a "payment" member)
 *  - 422 payment-id-reused  existing paymentId, different payload
 *  - 422 unknown-account    fromAccount/toAccount doesn't exist
 * Extending ResponseEntityExceptionHandler also turns Spring MVC's own errors (405, 415,
 * unknown routes, ...) into problems, with type about:blank.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    static final String PROBLEM_TYPE_BASE = "https://example.com/problems/";

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            fieldErrors.put(error.getField(), error.getDefaultMessage());
        }
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, "validation-failed", "Validation failed",
                "One or more fields are invalid", pathOf(request));
        problem.setProperty("errors", fieldErrors);
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
                                                                  HttpHeaders headers, HttpStatusCode status,
                                                                  WebRequest request) {
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, "malformed-request", "Malformed request body",
                "The request body could not be read as JSON of the expected shape", pathOf(request));
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @ExceptionHandler(InvalidAmountException.class)
    public ProblemDetail handleInvalidAmount(InvalidAmountException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "invalid-amount", "Invalid amount", ex.getMessage(),
                request.getRequestURI());
    }

    @ExceptionHandler(InvalidPaymentRequestException.class)
    public ProblemDetail handleInvalidRequest(InvalidPaymentRequestException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "invalid-request", "Invalid payment request",
                ex.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail handleNotFound(ResourceNotFoundException ex, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "not-found", "Resource not found", ex.getMessage(),
                request.getRequestURI());
    }

    /**
     * The idempotent-replay answer: 409 with the existing payment in a "payment" member, so a
     * client retrying after a timeout learns the outcome of its original request. Without
     * that member it's the fallback for when the existing row couldn't be read back.
     */
    @ExceptionHandler(DuplicatePaymentException.class)
    public ProblemDetail handleDuplicate(DuplicatePaymentException ex, HttpServletRequest request) {
        ProblemDetail problem = problem(HttpStatus.CONFLICT, "duplicate-payment", "Duplicate payment",
                ex.getMessage(), request.getRequestURI());
        ex.existing().ifPresent(existing -> problem.setProperty("payment", PaymentResponse.from(existing)));
        return problem;
    }

    @ExceptionHandler(PaymentIdReusedException.class)
    public ProblemDetail handlePaymentIdReused(PaymentIdReusedException ex, HttpServletRequest request) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "payment-id-reused", "Payment id reused with a different payload",
                ex.getMessage(), request.getRequestURI());
    }

    @ExceptionHandler(UnknownAccountException.class)
    public ProblemDetail handleUnknownAccount(UnknownAccountException ex, HttpServletRequest request) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "unknown-account", "Unknown account", ex.getMessage(),
                request.getRequestURI());
    }

    private static ProblemDetail problem(HttpStatus status, String typeSlug, String title, String detail,
                                         String path) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create(PROBLEM_TYPE_BASE + typeSlug));
        problem.setTitle(title);
        if (path != null) {
            problem.setInstance(URI.create(path));
        }
        return problem;
    }

    private static String pathOf(WebRequest request) {
        return request instanceof ServletWebRequest servletRequest
                ? servletRequest.getRequest().getRequestURI()
                : null;
    }
}
