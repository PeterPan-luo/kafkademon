package com.cuicui.kafkademon.javaclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 
 * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality for
 * automatic consumer load balancing and failover. This example assumes that the offsets are stored in Kafka and are
 * manually committed using the commit(boolean) API. This example also demonstrates rewinding the consumer's offsets if
 * processing of the consumed messages fails. Note that this method of rewinding offsets using {@link #seek(Map)
 * seek(offsets)} is only useful for rewinding the offsets of the current consumer instance. As such, this will not
 * trigger a rebalance or affect the fetch offsets for the other consumer instances.
 * 
 * 
 * @author <a href="mailto:leicui001@126.com">崔磊</a>
 * @date 2015年11月7日 上午11:44:27
 */
public class JConsumer2 {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("group.id", "test");
        props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "false");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.subscribe("foo", "bar");
        int commitInterval = 100;
        int numRecords = 0;
        boolean isRunning = true;
        Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
        while (isRunning) {
            Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(100);
            try {
                Map<TopicPartition, Long> lastConsumedOffsets = process(records);
                consumedOffsets.putAll(lastConsumedOffsets);
                numRecords += records.size();
                // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
                if (numRecords % commitInterval == 0)
                    consumer.commit(false);
            } catch (Exception e) {
                try {
                    // rewind consumer's offsets for failed partitions
                    // assume failedPartitions() returns the list of partitions for which the processing of the last
                    // batch of messages failed
                    List<TopicPartition> failedPartitions = failedPartitions();
                    Map<TopicPartition, Long> offsetsToRewindTo = new HashMap<TopicPartition, Long>();
                    for (TopicPartition failedPartition : failedPartitions) {
                        // rewind to the last consumed offset for the failed partition. Since process() failed for this
                        // partition, the consumed offset
                        // should still be pointing to the last successfully processed offset and hence is the right
                        // offset to rewind consumption to.
                        offsetsToRewindTo.put(failedPartition, consumedOffsets.get(failedPartition));
                    }
                    // seek to new offsets only for partitions that failed the last process()
                    consumer.seek(offsetsToRewindTo);
                } catch (Exception e1) {
                    break;
                } // rewind failed
            }
        }
        consumer.close();
    }

    private static List<TopicPartition> failedPartitions() {
        // TODO Auto-generated method stub
        return null;
    }

    private static Map<TopicPartition, Long> process(Map<String, ConsumerRecords<byte[], byte[]>> records) {
        // TODO Auto-generated method stub
        return null;
    }
}
