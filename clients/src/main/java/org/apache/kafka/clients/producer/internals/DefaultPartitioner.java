/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * NOTE this partitioner is deprecated and shouldn't be used.  To use default partitioning logic
 * remove partitioner.class configuration setting.  See KIP-794 for more info.
 *
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose the sticky partition that changes when the batch is full.
 * 
 * See KIP-480 for details about sticky partitioning.
 *
 *
 org.apache.kafka.clients.producer.internals @Deprecated
 public class DefaultPartitioner
 extends Object
 implements Partitioner
 注意：此分区程序已弃用，不应使用。要使用默认分区逻辑，请删除分区程序.class配置设置。有关更多信息，请参阅 KIP-794。默认分区策略：
 如果在记录中指定了分区，请使用它
 如果未指定分区但存在键，则根据键的哈希值选择分区
 如果不存在分区或键，请选择批处理已满时更改的粘性分区。有关粘性分区的详细信息，请参阅 KIP-480。
 kafka.clients.main
 *
 */
@Deprecated
public class DefaultPartitioner implements Partitioner {

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    /**
     * configure()方法主要用来获取配置信息及初始化数据。
     * @param configs
     */
    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return partition(topic, key, keyBytes, value, valueBytes, cluster, cluster.partitionsForTopic(topic).size());
    }

    /**
     * 计算当前 消息record 的 分区号
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param numPartitions The number of partitions of the given {@code topic}
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster,
                         int numPartitions) {
        if (keyBytes == null) {
            //这里 会 轮询发送 给所有的 topic 内 所有可用的 partition
            return stickyPartitionCache.partition(topic, cluster);
        }
        return BuiltInPartitioner.partitionForKey(keyBytes, numPartitions);
    }

    public void close() {}
    
    /**
     * If a batch completed for the current sticky partition, change the sticky partition. 
     * Alternately, if no sticky partition has been determined, set one.
     */
    @SuppressWarnings("deprecation")
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
