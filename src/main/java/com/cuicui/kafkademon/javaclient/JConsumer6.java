package com.cuicui.kafkademon.javaclient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 
 This example demonstrates how the consumer can be used to subscribe to specific partitions of certain topics and
 * consume upto the latest available message for each of those partitions before shutting down. When used to subscribe
 * to specific partitions, the user foregoes the group management functionality and instead relies on manually
 * configuring the consumer instances to subscribe to a set of partitions. This example assumes that the user chooses to
 * use custom offset storage.
 * 
 * 
 * @author <a href="mailto:leicui001@126.com">崔磊</a>
 * @date 2015年11月7日 上午11:55:21
 */
public class JConsumer6 {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
        // subscribe to some partitions of topic foo
        TopicPartition partition0 = new TopicPartition("foo", 0);
        TopicPartition partition1 = new TopicPartition("foo", 1);
        TopicPartition[] partitions = new TopicPartition[2];
        partitions[0] = partition0;
        partitions[1] = partition1;
        consumer.subscribe(partitions);
        Map<TopicPartition, Long> lastCommittedOffsets = getLastCommittedOffsetsFromCustomStore();
        // seek to the last committed offsets to avoid duplicates
        consumer.seek(lastCommittedOffsets);
        // find the offsets of the latest available messages to know where to stop consumption
        Map<TopicPartition, Long> latestAvailableOffsets = consumer.offsetsBeforeTime(-2, Arrays.asList(partitions));
        boolean isRunning = true;
        Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
        while (isRunning) {
            Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
            Map<TopicPartition, Long> lastConsumedOffsets = process(records);
            consumedOffsets.putAll(lastConsumedOffsets);
            // commit offsets for partitions 0,1 for topic foo to custom store
            commitOffsetsToCustomStore(consumedOffsets);
            for (TopicPartition partition : partitions) {
                if (consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
                    isRunning = false;
                else
                    isRunning = true;
            }
        }
        commitOffsetsToCustomStore(consumedOffsets);
        consumer.close();
    }

    private static void commitOffsetsToCustomStore(Map<TopicPartition, Long> consumedOffsets) {
        // TODO Auto-generated method stub

    }

    private static Map<TopicPartition, Long> process(Map<String, ConsumerRecords<byte[], byte[]>> records) {
        // TODO Auto-generated method stub
        return null;
    }

    private static Map<TopicPartition, Long> getLastCommittedOffsetsFromCustomStore() {
        // TODO Auto-generated method stub
        return null;
    }

}
