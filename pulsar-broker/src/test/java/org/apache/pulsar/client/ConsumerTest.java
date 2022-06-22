package org.apache.pulsar.client;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

public class ConsumerTest {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {

        try (PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build()) {
            Consumer consumer = client.newConsumer()
                    .topic("test9")
                    //允许乱序
                    .subscriptionName("sb1")
                    .subscribe();
            consumer.seek(MessageId.earliest);

            while (true) {
                // Wait for a message
                try {
                    Message msg = consumer.receive(1000, TimeUnit.MILLISECONDS);

                    if(msg!=null){
                        // Do something with the message
                        System.out.println("Message received: " + (msg == null ? "null" : new String(msg.getData())));

                        // Acknowledge the message so that it can be deleted by the message broker
                        consumer.acknowledge(msg);
                    }else{
                        break;
                    }
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    System.out.println(e.getMessage());
                }
            }
            consumer.close();
            client.close();

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}
