package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.api.PaymentResponse;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
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

        ResponseEntity<PaymentResponse> second = post(request);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(getPayment(paymentId).getBody().status()).isEqualTo(PaymentStatus.PROCESSED));

        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("900.00");
    }

    @Test
    void insufficientFundsFailsThePaymentAndPublishesToTheDlt() {
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
        assertThat(observeDltRecordKey(paymentId)).isTrue();
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
