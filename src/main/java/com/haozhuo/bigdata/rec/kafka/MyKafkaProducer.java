package com.haozhuo.bigdata.rec.kafka;

import com.haozhuo.bigdata.rec.Props;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;


public class MyKafkaProducer {
    private String topicName = Props.get("kafka.topic.name");
    public Producer<String, String> producerInstance;
    public  MyKafkaProducer() {
        Properties props = new Properties();
        props.put("client.id","112");
        props.put("bootstrap.servers",Props.get("kafka.bootstrap.servers"));
        props.put("acks","all");
        props.put("retries", 0);
        props.put("batch.size", Props.get("kafka.batch.size"));
        props.put("linger.ms", Props.get("kafka.linger.ms"));
        props.put("buffer.memory", Props.get("kafka.buffer.memory"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerInstance = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public void send(String msg) {
        producerInstance.send(new ProducerRecord<>(topicName, null, msg));
    }

    public void close() {
        producerInstance.close();
    }

}
