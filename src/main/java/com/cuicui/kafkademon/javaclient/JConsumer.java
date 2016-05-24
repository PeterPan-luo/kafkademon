package com.cuicui.kafkademon.javaclient;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 
 * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality for
 * automatic consumer load balancing and failover. This example assumes that the offsets are stored in Kafka and are
 * automatically committed periodically, as controlled by the auto.commit.interval.ms config
 * 
 * 
 * @author <a href="mailto:leicui001@126.com">崔磊</a>
 * @date 2015年11月5日 下午2:31:53
 */
public class JConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("group.id", "test");
        props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.subscribe("foo", "bar");
        boolean isRunning = true;
        while (isRunning) {
            Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
            process(records);
        }
        consumer.close();
    }

    private static void process(Map<String, ConsumerRecords<byte[], byte[]>> records) {

    }
}
