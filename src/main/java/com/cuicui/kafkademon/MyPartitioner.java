package com.cuicui.kafkademon;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MyPartitioner implements Partitioner {
    public MyPartitioner(VerifiableProperties props) {

    }

    /*
     * @see kafka.producer.Partitioner#partition(java.lang.Object, int)
     */
    @Override
    public int partition(Object key, int partitionCount) {
        return Integer.valueOf((String) key) % partitionCount;
    }
}
