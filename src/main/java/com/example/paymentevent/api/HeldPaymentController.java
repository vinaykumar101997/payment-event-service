package com.example.paymentevent.api;

import com.example.paymentevent.exception.InvalidQueryException;
import com.example.paymentevent.screening.HeldPaymentQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * GET /payments?status=HELD[&overdue=true|false][&cursor=...][&limit=1..200]: the officers' work
 * list (see HeldPaymentQuery for ordering and pagination). Only status=HELD is supported; any
 * other value, a bad limit or an unknown cursor is a 400 invalid-query problem.
 *
 * The parameters are taken as strings and checked here, so every malformed one gets the same
 * invalid-query problem instead of Spring's generic type-mismatch response.
 */
@RestController
public class HeldPaymentController {

    static final int DEFAULT_LIMIT = 50;
    static final int MAX_LIMIT = 200;

    private final HeldPaymentQuery heldPaymentQuery;

    public HeldPaymentController(HeldPaymentQuery heldPaymentQuery) {
        this.heldPaymentQuery = heldPaymentQuery;
    }

    @GetMapping("/payments")
    public HeldPaymentQuery.Page list(@RequestParam(required = false) String status,
                                      @RequestParam(required = false) String overdue,
                                      @RequestParam(required = false) String cursor,
                                      @RequestParam(required = false) String limit) {
        if (!"HELD".equals(status)) {
            throw new InvalidQueryException("status is required and must be HELD (the only listable status)");
        }
        return heldPaymentQuery.find(parseOverdue(overdue), blankToNull(cursor), parseLimit(limit));
    }

    private static Boolean parseOverdue(String overdue) {
        if (overdue == null) {
            return null;
        }
        return switch (overdue) {
            case "true" -> Boolean.TRUE;
            case "false" -> Boolean.FALSE;
            default -> throw new InvalidQueryException("overdue must be true or false");
        };
    }

    private static int parseLimit(String limit) {
        if (limit == null) {
            return DEFAULT_LIMIT;
        }
        try {
            int value = Integer.parseInt(limit);
            if (value >= 1 && value <= MAX_LIMIT) {
                return value;
            }
        } catch (NumberFormatException ignored) {
            // falls through to the same message
        }
        throw new InvalidQueryException("limit must be a whole number from 1 to " + MAX_LIMIT);
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
