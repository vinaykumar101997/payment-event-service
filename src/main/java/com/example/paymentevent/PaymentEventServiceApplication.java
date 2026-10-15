package com.example.paymentevent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @EnableScheduling activates the @Scheduled methods of OutboxRelay and PaymentSweeper.
 * @ConfigurationPropertiesScan registers the @ConfigurationProperties records (MoneyLimitsProperties).
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class PaymentEventServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentEventServiceApplication.class, args);
    }
}
