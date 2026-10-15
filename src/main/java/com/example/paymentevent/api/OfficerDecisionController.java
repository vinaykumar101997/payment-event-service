package com.example.paymentevent.api;

import com.example.paymentevent.screening.OfficerDecision;
import com.example.paymentevent.screening.OfficerDecisionService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Compliance officer decisions on HELD payments. 200 with the payment on success, also when
 * the same decision is repeated; 409 payment-not-held otherwise (see OfficerDecisionService).
 */
@RestController
@RequestMapping("/payments/{id}")
public class OfficerDecisionController {

    private final OfficerDecisionService officerDecisionService;

    public OfficerDecisionController(OfficerDecisionService officerDecisionService) {
        this.officerDecisionService = officerDecisionService;
    }

    @PostMapping("/release")
    public PaymentResponse release(@PathVariable String id, @Valid @RequestBody OfficerDecisionRequest request) {
        return PaymentResponse.from(
                officerDecisionService.decide(id, OfficerDecision.RELEASE, request.officer(), request.note()));
    }

    @PostMapping("/reject")
    public PaymentResponse reject(@PathVariable String id, @Valid @RequestBody OfficerDecisionRequest request) {
        return PaymentResponse.from(
                officerDecisionService.decide(id, OfficerDecision.REJECT, request.officer(), request.note()));
    }
}
