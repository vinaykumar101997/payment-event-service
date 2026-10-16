# Screening contracts

JSON Schemas (draft 2020-12) for the events exchanged with the screening service. The version
is part of the file name; a breaking change gets a new file (`.v2`), never an edit to a
published one.

| Schema | Topic | Producer | Consumer |
|---|---|---|---|
| `payment.screening.requested.v1.schema.json` | `payment.screening.requested` | payment-event-service | screening service |
| `payment.screening.completed.v1.schema.json` | `payment.screening.completed` | screening service | payment-event-service |

Both messages are keyed by `paymentId`. `additionalProperties` is `false`: an unknown field
is a contract break, not something to ignore. A `payment.screening.completed` event that
does not validate goes to `payment.screening.completed.DLT` and is never applied.

`examples/` holds one valid sample per event type and verdict. `ScreeningContractsTest`
validates them and checks that a list of contract breaks is rejected.
