package com.example.paymentevent.api;

import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.service.PaymentService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * payments.problem-base-uri is per environment: whatever base is configured is what clients
 * see in "type". A web slice (no database or Kafka) so the property can differ from the one
 * the shared integration-test context uses. The mocked repository returns Optional.empty(),
 * so GET /payments/{id} produces a not-found problem.
 */
@WebMvcTest(controllers = PaymentController.class, properties = "payments.problem-base-uri=https://errors.internal.test/payments/")
class ProblemBaseUriTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PaymentService paymentService;

    @MockitoBean
    private PaymentRepository paymentRepository;

    @Test
    void configuredBaseUriIsUsedForTheProblemType() throws Exception {
        mockMvc.perform(get("/payments/{id}", "missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.type").value("https://errors.internal.test/payments/not-found"));
    }
}
