package org.apache.pulsar.client;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

public class ConsumerTest {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        try{

        }catch (Exception e){

        }
        try (PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build()) {
            Consumer consumer = client.newConsumer()
                    .topic("test1")
                    .subscriptionName("sb1")
                    .subscribe();

            while (true) {
                // Wait for a message

                try {
                    Message msg = consumer.receive(1000, TimeUnit.MILLISECONDS);

                    // Do something with the message
                    System.out.println("Message received: " + (msg == null ? "null" : new String(msg.getData())));

                    // Acknowledge the message so that it can be deleted by the message broker
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    System.out.println(e.getMessage());
                }
            }

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}
