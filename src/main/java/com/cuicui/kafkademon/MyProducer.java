package com.cuicui.kafkademon;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author <a href="mailto:leicui001@126.com">崔磊</a>
 * @date 2015年11月4日 上午11:44:15
 */
public class MyProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", KafkaProperties.BROKER_CONNECT);
        props.put("partitioner.class", "com.cuicui.kafkademon.MyPartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // 单个发送
        for (int i = 0; i <= 1000000; i++) {
            KeyedMessage<String, String> message =
                    new KeyedMessage<String, String>(KafkaProperties.TOPIC, i + "", "Message" + i);
            producer.send(message);
            Thread.sleep(5000);
        }

        // 批量发送
        List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>(100);
        for (int i = 0; i <= 10000; i++) {
            KeyedMessage<String, String> message =
                    new KeyedMessage<String, String>(KafkaProperties.TOPIC, i + "", "Message" + i);
            messages.add(message);
            if (i % 100 == 0) {
                producer.send(messages);
                messages.clear();
            }
        }
        producer.send(messages);
    }
}
