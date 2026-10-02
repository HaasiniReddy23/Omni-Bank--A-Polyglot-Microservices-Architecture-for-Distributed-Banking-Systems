package com.omnibank.ledger;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    public static final String TRANSFER_QUEUE = "transfer-requests";

    public static final String TRANSFER_EXCHANGE = "transfer-events";

    @Bean
    public Queue transferQueue() {
        return new Queue(TRANSFER_QUEUE, true);
    }

    @Bean
    public FanoutExchange transferExchange() {
        return new FanoutExchange(TRANSFER_EXCHANGE, true, false);
    }
}