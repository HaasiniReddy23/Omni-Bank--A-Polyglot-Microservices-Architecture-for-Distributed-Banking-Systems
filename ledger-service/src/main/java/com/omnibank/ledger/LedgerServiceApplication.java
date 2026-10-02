package com.omnibank.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LedgerServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
        System.out.println("=================================================");
        System.out.println(" OmniBank Ledger Service is running on port 8081");
        System.out.println(" Listening for messages on queue: transfer-requests");
        System.out.println("=================================================");
    }
}