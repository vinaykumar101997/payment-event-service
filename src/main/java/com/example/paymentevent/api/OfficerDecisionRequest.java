package com.example.paymentevent.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** Body of POST /payments/{id}/release and /reject. Both fields are stored in officer_decisions. */
public record OfficerDecisionRequest(
        @NotBlank @Size(max = 100) String officer,
        @NotBlank @Size(max = 1000) String note) {
}
