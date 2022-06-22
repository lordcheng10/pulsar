package org.apache.pulsar.client;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.ExecutionException;

public class ProducerTest {


    public static void main(String[] args) throws InterruptedException {
        try {
            //设置配置，并构建PulsarClient对象实例
            //1.PulsarClient.builder():构建出ClientBuilder实例对象；
            //2.调用ClientBuilder的serviceUrl方法，设置对应conf配置，然后返回ClientBuilder对象；
            //3.通过调用ClientBuilder的build方法(通过之前set的conf配置，来实例化PulsarClient类)，构建PulsarClient对象实例
            //所以在build之前，其实都是类似setProperty的动作，最后build才是new PulsarClient(propConf)的动作
            PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();
            //这里和上面一样，client.newProducer()只是创建一个ProducerBuilder对象，
            // 然后通过后面类似producerName的方法进行设置配置(类似Conf conf = new Conf后，conf.setConfig(key,value))，
            // 最后再通过create方法来真正构建producer对象：new Producer(conf)
            //这里回发CONNECTE -> PARTITIONED_METADATA -> LOOKUP -> PRODUCER 三个请求，目的是把producer注册上
            //如果这里的schema填的是Schema.AUTO_PRODUCE_BYTES()，那么在CONNECTED后立马就会发送getSchema请求来获取schema.
            //newProducer\producerName\topic都没有什么逻辑，发送请求这些逻辑都在create中
            Producer<byte[]> producer = client.newProducer(Schema.AUTO_PRODUCE_BYTES()).producerName("chenlin-producer1").topic("test9").create();

            //这里发send请求
            for(int i =0;i<10;i++){
                byte[] buffer = ("" +i).getBytes();
                long length = buffer.length;
                System.out.println(length);
                producer.sendAsync(buffer).get();
            }
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        Thread.sleep(1000);
    }
}