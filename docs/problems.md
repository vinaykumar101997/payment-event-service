# Problem types

Every error from the API is an [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem
(`Content-Type: application/problem+json`). The `type` field is
`payments.problem-base-uri` + a slug. With the default base
`https://github.com/vinaykumar101997/payment-event-service/blob/main/docs/problems.md#`,
each `type` links to the section of this page with the same name.

`type` values used to be `https://example.com/problems/<slug>`; clients that matched on
those must switch to the new URIs.

Branch on `type`, not on `title` or `detail`: `title` names the kind of problem and `detail`
explains this occurrence in words that may change. `instance` is the request path.

Errors produced by Spring MVC itself (unknown route, 405 method not allowed, 415 unsupported
media type) are also problem details, with `type` `about:blank`, and are not listed here.

In the examples, `{base}` stands for the configured base URI.

## validation-failed

- **HTTP status:** 400 Bad Request
- **When it occurs:** one or more fields of `POST /payments` fail validation: a blank
  `paymentId`, `fromAccount` or `toAccount`; a missing, zero or negative `amount`, or one with
  more than 17 integer digits or 2 decimal places; a `currency` that is not three upper-case
  letters. The `errors` member maps each invalid field to its message.
- **Example response:**

  ```json
  {
    "type": "{base}validation-failed",
    "title": "Validation failed",
    "status": 400,
    "detail": "One or more fields are invalid",
    "instance": "/payments",
    "errors": {
      "amount": "must be greater than 0",
      "currency": "must be a 3-letter ISO currency code"
    }
  }
  ```

- **What the client should do:** fix the fields listed in `errors` and send the request
  again. Retrying unchanged will fail the same way.

## malformed-request

- **HTTP status:** 400 Bad Request
- **When it occurs:** the request body is not valid JSON, or a value has the wrong JSON type
  (for example `"amount": "ten"`).
- **Example response:**

  ```json
  {
    "type": "{base}malformed-request",
    "title": "Malformed request body",
    "status": 400,
    "detail": "The request body could not be read as JSON of the expected shape",
    "instance": "/payments"
  }
  ```

- **What the client should do:** fix the request body. This is a client bug; do not retry
  unchanged.

## invalid-request

- **HTTP status:** 400 Bad Request
- **When it occurs:** every field is valid on its own but the request as a whole is not;
  today, `fromAccount` and `toAccount` are the same account.
- **Example response:**

  ```json
  {
    "type": "{base}invalid-request",
    "title": "Invalid payment request",
    "status": 400,
    "detail": "fromAccount and toAccount must differ",
    "instance": "/payments"
  }
  ```

- **What the client should do:** correct the request. Do not retry unchanged.

## invalid-amount

- **HTTP status:** 400 Bad Request
- **When it occurs:** the amount breaks a money rule: more decimal places than the currency
  allows (JPY allows none), an unknown currency or one with more than 2 decimal places, or an
  amount above the currency's configured maximum (`payments.max-amount-by-currency`, falling
  back to `payments.max-amount`).
- **Example response:**

  ```json
  {
    "type": "{base}invalid-amount",
    "title": "Invalid amount",
    "status": 400,
    "detail": "Amount 100.5 has more decimal places than JPY allows (0)",
    "instance": "/payments"
  }
  ```

- **What the client should do:** correct the amount or currency. An amount above the maximum
  needs to be split or handled outside this API. Do not retry unchanged.

## not-found

- **HTTP status:** 404 Not Found
- **When it occurs:** `GET /payments/{id}` or `GET /accounts/{id}` with an id that does not
  exist.
- **Example response:**

  ```json
  {
    "type": "{base}not-found",
    "title": "Resource not found",
    "status": 404,
    "detail": "Payment not found: p-404",
    "instance": "/payments/p-404"
  }
  ```

- **What the client should do:** check the id. A payment that was just created with
  `201 Created` is readable immediately, so a 404 for it means the create did not happen.

## duplicate-payment

- **HTTP status:** 409 Conflict
- **When it occurs:** `POST /payments` repeats a `paymentId` that already exists with exactly
  the same payload (amounts compared by value, so `100` and `100.00` are the same). This is
  the idempotent replay answer: no second payment is created. The `payment` member carries
  the existing payment and its current status.
- **Example response:**

  ```json
  {
    "type": "{base}duplicate-payment",
    "title": "Duplicate payment",
    "status": 409,
    "detail": "Payment already exists: p-1",
    "instance": "/payments",
    "payment": {
      "paymentId": "p-1",
      "fromAccount": "ACC-1001",
      "toAccount": "ACC-1002",
      "amount": 25.00,
      "currency": "USD",
      "status": "PROCESSED",
      "failureReason": null,
      "createdAt": "2026-10-15T18:30:00Z",
      "processedAt": "2026-10-15T18:30:01Z"
    }
  }
  ```

- **What the client should do:** treat it as success of the original request and read the
  outcome from `payment`. Safe to receive after a timeout-and-retry.

## payment-id-reused

- **HTTP status:** 422 Unprocessable Content
- **When it occurs:** `POST /payments` uses a `paymentId` that already exists but with a
  different payload (another amount, account or currency). The original payment is untouched.
- **Example response:**

  ```json
  {
    "type": "{base}payment-id-reused",
    "title": "Payment id reused with a different payload",
    "status": 422,
    "detail": "paymentId p-1 already exists with a different payload",
    "instance": "/payments"
  }
  ```

- **What the client should do:** use a new `paymentId` for a new payment. If this was meant to
  be a retry, the client is not sending the same payload as the first time, which is a bug.

## unknown-account

- **HTTP status:** 422 Unprocessable Content
- **When it occurs:** `POST /payments` names a `fromAccount` or `toAccount` that does not exist.
  No payment is created.
- **Example response:**

  ```json
  {
    "type": "{base}unknown-account",
    "title": "Unknown account",
    "status": 422,
    "detail": "Unknown account: ACC-9999",
    "instance": "/payments"
  }
  ```

- **What the client should do:** check the account ids. Do not retry unchanged.

## service-busy

- **HTTP status:** 503 Service Unavailable, with a `Retry-After: 1` header (seconds).
- **When it occurs:** `POST /payments` could not insert the payment because one of its accounts
  was locked by another database transaction for longer than the lock timeout (Postgres
  `lock_timeout`, 5 seconds by default; SQLState 55P03). Normal payment processing does not
  cause this; a long-running script or manual change holding the account row can. Nothing was
  saved.
- **Example response:**

  ```json
  {
    "type": "{base}service-busy",
    "title": "Service busy",
    "status": 503,
    "detail": "An account in this payment is busy; retry shortly",
    "instance": "/payments"
  }
  ```

- **What the client should do:** wait at least the `Retry-After` seconds and send the same
  request again, with the same `paymentId`. It is safe: if an earlier attempt did get through,
  the retry is answered with `duplicate-payment` (409) and the existing payment.

## payment-not-held

- **HTTP status:** 409 Conflict
- **When it occurs:** `POST /payments/{id}/release` or `POST /payments/{id}/reject` for a
  payment that is not HELD and has no earlier officer decision of the same kind: the opposite
  officer decision was already made, a screening verdict already released or rejected it, or
  it was never held. Nothing is changed or recorded. (Repeating the same decision is not an
  error: it returns 200 with the payment.) The `payment` member carries the current state.
- **Example response:**

  ```json
  {
    "type": "{base}payment-not-held",
    "title": "Payment not held",
    "status": 409,
    "detail": "Cannot release payment p-1: it is REJECTED, not HELD",
    "instance": "/payments/p-1/release",
    "payment": {
      "paymentId": "p-1",
      "fromAccount": "ACC-1001",
      "toAccount": "ACC-1002",
      "amount": 25.00,
      "currency": "USD",
      "status": "REJECTED",
      "failureReason": "Rejected by officer carol: customer confirmed fraud",
      "createdAt": "2026-10-15T18:30:00Z",
      "processedAt": "2026-10-15T18:42:10Z"
    }
  }
  ```

- **What the client should do:** don't retry. Read `payment.status`: the payment has already
  been decided, and that decision stands.

## unauthorized

- **HTTP status:** 401 Unauthorized
- **When it occurs:** any request (except `/actuator/health` and `/actuator/info`) without an `X-API-Key` header,
  or with one that doesn't exactly match the key the service was started with
  (`PAYMENTS_API_KEY`). The response is the same for a missing and a wrong key. Nothing is
  read or changed.
- **Example response:**

  ```json
  {
    "type": "{base}unauthorized",
    "title": "Unauthorized",
    "status": 401,
    "detail": "Missing or invalid X-API-Key header",
    "instance": "/payments"
  }
  ```

- **What the client should do:** send the correct key in `X-API-Key`. Retrying without fixing
  the key won't help.

## invalid-query

- **HTTP status:** 400 Bad Request
- **When it occurs:** `GET /payments` without `status=HELD` (it is the only listable status),
  with `limit` outside 1 to 200 or not a number, `overdue` other than `true` or `false`, or a
  `cursor` that wasn't returned by this endpoint. `detail` names the parameter.
- **Example response:**

  ```json
  {
    "type": "{base}invalid-query",
    "title": "Invalid query",
    "status": 400,
    "detail": "limit must be a whole number from 1 to 200",
    "instance": "/payments"
  }
  ```

- **What the client should do:** fix the parameter. Use `nextCursor` from the previous page
  exactly as returned; don't build cursors yourself.
