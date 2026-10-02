package com.omnibank.ledger;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransferListener {

    private final RabbitTemplate rabbitTemplate;

    public TransferListener(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queues = RabbitConfig.TRANSFER_QUEUE)
    public void handleTransferMessage(String message) {

        System.out.println(">>> RECEIVED MESSAGE FROM RABBITMQ: " + message);

        // Forward the transfer event to the notification service
        rabbitTemplate.convertAndSend(
                RabbitConfig.TRANSFER_EXCHANGE,
                "",
                message
        );

        System.out.println(">>> TRANSFER EVENT PUBLISHED FOR NOTIFICATIONS");
    }
}