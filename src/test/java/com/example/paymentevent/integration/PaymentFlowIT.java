package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.api.PaymentResponse;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentProducer;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PaymentFlowIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentProducer paymentProducer;

    @Test
    void processesAPaymentEndToEnd() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        ResponseEntity<PaymentResponse> createResponse = post(
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));
        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(getPayment(paymentId).getBody().status()).isEqualTo(PaymentStatus.PROCESSED));

        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("900.00");
        assertThat(accountRepository.findById(to.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("100.00");
    }

    @Test
    void duplicatePostIsRejectedAndDoesNotDoubleDebit() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        PaymentRequest request = new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD");

        ResponseEntity<PaymentResponse> first = post(request);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        // 409 bodies are problem details (with the existing payment inside), not a PaymentResponse.
        ResponseEntity<String> second = restTemplate.postForEntity("/payments", request, String.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(getPayment(paymentId).getBody().status()).isEqualTo(PaymentStatus.PROCESSED));

        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("900.00");
    }

    /**
     * Idempotency is keyed on paymentId, but only an identical retry is a replay. The same
     * paymentId with a different payload is a client bug (or a reused id) - silently
     * answering with the *original* payment would let the client believe its new request
     * was accepted. That's 422; the original payment is untouched.
     */
    @Test
    void reusedPaymentIdWithDifferentPayloadIsRejectedWith422() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        assertThat(post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"))
                .getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> differentAmount = restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("999.00"), "USD"), String.class);
        assertThat(differentAmount.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(differentAmount.getBody()).contains(paymentId);

        // Same values, different scale: still the identical request, so still the 409 replay.
        assertThat(restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100"), "USD"), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(getPayment(paymentId).getBody().status()).isEqualTo(PaymentStatus.PROCESSED));
        assertThat(getPayment(paymentId).getBody().amount()).isEqualByComparingTo("100.00");
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("900.00");
    }

    /**
     * Insufficient funds is a business outcome, not a fault: the payment ends FAILED and a
     * payment.failed event is published (via the outbox). It used to be thrown out of the
     * listener and land on payments.DLT, mixing expected outcomes in with real faults.
     */
    @Test
    void insufficientFundsFailsThePaymentAndPublishesPaymentFailedNotTheDlt() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("10.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        ResponseEntity<PaymentResponse> createResponse = post(
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));
        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(getPayment(paymentId).getBody().status()).isEqualTo(PaymentStatus.FAILED));

        assertThat(getPayment(paymentId).getBody().failureReason()).containsIgnoringCase("insufficient funds");
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("10.00");
        assertThat(observeRecordKey("payment.failed", paymentId, Duration.ofSeconds(15))).isTrue();
        // By now the listener has long since returned; had it thrown, the DLT record would be there.
        assertThat(observeRecordKey("payments.DLT", paymentId, Duration.ofSeconds(5))).isFalse();
    }

    @Test
    void postWithUnknownFromAccountIsRejectedWith422() {
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String missing = id("MISSING");
        String paymentId = id("PAY");

        ResponseEntity<String> response = restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, missing, to.getId(), new BigDecimal("10.00"), "USD"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody()).contains("problems/unknown-account").contains(missing);
        assertThat(restTemplate.getForEntity("/payments/{id}", String.class, paymentId).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void postWithUnknownToAccountIsRejectedWith422() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        String missing = id("MISSING");

        ResponseEntity<String> response = restTemplate.postForEntity("/payments",
                new PaymentRequest(id("PAY"), from.getId(), missing, new BigDecimal("10.00"), "USD"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody()).contains("problems/unknown-account").contains(missing);
    }

    /**
     * Bypasses POST so the stored row and the Kafka event can disagree: the row says 100.00,
     * the event published straight onto "payments" says 500.00. No money may move, the
     * payment must end FAILED, and the message must land on the DLT.
     */
    @Test
    void eventThatDoesNotMatchStoredPaymentMovesNoMoneyAndGoesToDlt() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        paymentRepository.saveAndFlush(new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));

        paymentProducer.publishPaymentEvent(
                new PaymentEvent(paymentId, from.getId(), to.getId(), new BigDecimal("500.00"), "USD"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(getPayment(paymentId).getBody().status())
                        .isIn(PaymentStatus.PROCESSED, PaymentStatus.FAILED));

        assertThat(getPayment(paymentId).getBody().status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(getPayment(paymentId).getBody().failureReason()).contains("does not match");
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("1000.00");
        assertThat(accountRepository.findById(to.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("0.00");
        assertThat(observeDltRecordKey(paymentId)).isTrue();
        assertThat(observeRecordKey("payment.failed", paymentId, Duration.ofSeconds(15))).isTrue();
    }

    /**
     * The DLT topic is shared across the whole test JVM (see AbstractIntegrationTest) -
     * ConcurrencyIT's partial-funds test alone puts 40 other messages on it. Reading from
     * "earliest" and grabbing the first record would just observe whichever unrelated
     * message happens to be oldest, so this scans every record for the specific key
     * we're looking for instead of assuming it's first.
     */
    private boolean observeDltRecordKey(String expectedKey) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.List.of("payments.DLT"));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (expectedKey.equals(record.key())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private ResponseEntity<PaymentResponse> post(PaymentRequest request) {
        return restTemplate.postForEntity("/payments", request, PaymentResponse.class);
    }

    private ResponseEntity<PaymentResponse> getPayment(String paymentId) {
        return restTemplate.getForEntity("/payments/{id}", PaymentResponse.class, paymentId);
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
