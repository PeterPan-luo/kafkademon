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
 * 
 * This example demonstrates how to rewind the offsets of the entire consumer group. It is assumed that the user has
 * chosen to use Kafka's group management functionality for automatic consumer load balancing and failover. This example
 * also assumes that the offsets are stored in Kafka. If group management is used, the right place to systematically
 * rewind offsets for <i>every</i> consumer instance is inside the ConsumerRebalanceCallback. The onPartitionsAssigned
 * callback is invoked after the consumer is assigned a new set of partitions on rebalance <i>and</i> before the
 * consumption restarts post rebalance. This is the right place to supply the newly rewound offsets to the consumer. It
 * is recommended that if you foresee the requirement to ever reset the consumer's offsets in the presence of group
 * management, that you always configure the consumer to use the ConsumerRebalanceCallback with a flag that protects
 * whether or not the offset rewind logic is used. This method of rewinding offsets is useful if you notice an issue
 * with your message processing after successful consumption and offset commit. And you would like to rewind the offsets
 * for the entire consumer group as part of rolling out a fix to your processing logic. In this case, you would
 * configure each of your consumer instances with the offset rewind configuration flag turned on and bounce each
 * consumer instance in a rolling restart fashion. Each restart will trigger a rebalance and eventually all consumer
 * instances would have rewound the offsets for the partitions they own, effectively rewinding the offsets for the
 * entire consumer group.
 * 
 * 
 * @author <a href="mailto:leicui001@126.com">崔磊</a>
 * @date 2015年11月7日 上午11:47:16
 */
public class JConsumer3 {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("group.id", "test");
        props.put("session.timeout.ms", "1000");
        props.put("enable.auto.commit", "false");
        KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<byte[], byte[]>(props, new ConsumerRebalanceCallback() {
                    boolean rewindOffsets = true; // should be retrieved from external application config

                    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                        Map<TopicPartition, Long> latestCommittedOffsets = consumer.committed(partitions);
                        if (rewindOffsets) {
                            Map<TopicPartition, Long> newOffsets = rewindOffsets(latestCommittedOffsets, 100);
                            consumer.seek(newOffsets);
                        }
                    }

                    public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                        consumer.commit(true);
                    }

                    // this API rewinds every partition back by numberOfMessagesToRewindBackTo messages
                    private Map<TopicPartition, Long> rewindOffsets(Map<TopicPartition, Long> currentOffsets,
                            long numberOfMessagesToRewindBackTo) {
                        Map<TopicPartition, Long> newOffsets = new HashMap<TopicPartition, Long>();
                        for (Map.Entry<TopicPartition, Long> offset : currentOffsets.entrySet())
                            newOffsets.put(offset.getKey(), offset.getValue() - numberOfMessagesToRewindBackTo);
                        return newOffsets;
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
                consumer.commit(consumedOffsets, true);
        }
        consumer.commit(true);
        consumer.close();
    }

    private static Map<TopicPartition, Long> process(Map<String, ConsumerRecords<byte[], byte[]>> records) {
        // TODO Auto-generated method stub
        return null;
    }

}
