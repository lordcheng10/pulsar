package org.apache.pulsar.client;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class ProducerTest {

    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();

            Producer<byte[]> producer = client.newProducer().producerName("chenlin-producer1").topic("test1").create();

            producer.sendAsync("wocaonimaxx123".getBytes());
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        Thread.sleep(1000);
    }
}
