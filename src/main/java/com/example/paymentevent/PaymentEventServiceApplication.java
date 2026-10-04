package com.example.paymentevent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @EnableScheduling activates the @Scheduled methods of OutboxRelay and PaymentSweeper.
 */
@SpringBootApplication
@EnableScheduling
public class PaymentEventServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentEventServiceApplication.class, args);
    }
}
