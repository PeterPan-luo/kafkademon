package com.cuicui.kafkademon.javaclient;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality along with
 * custom offset storage. In this example, the assumption made is that the user chooses to store the consumer offsets
 * outside Kafka. This requires the user to plugin logic for retrieving the offsets from a custom store and provide the
 * offsets to the consumer in the ConsumerRebalanceCallback callback. The onPartitionsAssigned callback is invoked after
 * the consumer is assigned a new set of partitions on rebalance <i>and</i> before the consumption restarts post
 * rebalance. This is the right place to supply offsets from a custom store to the consumer.
 * 
 * Similarly, the user would also be required to plugin logic for storing the consumer's offsets to a custom store. The
 * onPartitionsRevoked callback is invoked right after the consumer has stopped fetching data and before the partition
 * ownership changes. This is the right place to commit the offsets for the current set of partitions owned by the
 * consumer.
 * 
 * 
 * @author <a href="mailto:leicui001@126.com">崔磊</a>
 * @date 2015年11月7日 上午11:52:17
 */
public class JConsumer4 {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("group.id", "test");
        props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "false"); // since enable.auto.commit only applies to Kafka based offset storage
        KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<byte[], byte[]>(props, new ConsumerRebalanceCallback() {
                    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                        Map<TopicPartition, Long> lastCommittedOffsets =
                                getLastCommittedOffsetsFromCustomStore(partitions);
                        consumer.seek(lastCommittedOffsets);
                    }

                    public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                        Map<TopicPartition, Long> offsets = getLastConsumedOffsets(partitions);
                        commitOffsetsToCustomStore(offsets);
                    }

                    // following APIs should be implemented by the user for custom offset management
                    private Map<TopicPartition, Long> getLastCommittedOffsetsFromCustomStore(
                            Collection<TopicPartition> partitions) {
                        return null;
                    }

                    private Map<TopicPartition, Long> getLastConsumedOffsets(Collection<TopicPartition> partitions) {
                        return null;
                    }

                    private void commitOffsetsToCustomStore(Map<TopicPartition, Long> offsets) {
                    }
                });
        consumer.subscribe("foo", "bar");
        int commitInterval = 100;
        int numRecords = 0;
        boolean isRunning = true;
        Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
        while (isRunning) {
            Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
            Map<TopicPartition, Long> lastConsumedOffsets = process(records);
            consumedOffsets.putAll(lastConsumedOffsets);
            numRecords += records.size();
            // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
            if (numRecords % commitInterval == 0)
                commitOffsetsToCustomStore(consumedOffsets);
        }
        consumer.commit(true);
        consumer.close();
    }

    private static Map<TopicPartition, Long> process(Map<String, ConsumerRecords<byte[], byte[]>> records) {
        // TODO Auto-generated method stub
        return null;
    }

    private static void commitOffsetsToCustomStore(Map<TopicPartition, Long> consumedOffsets) {
        // TODO Auto-generated method stub

    }

}
