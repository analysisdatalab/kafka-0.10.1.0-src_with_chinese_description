/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 */
public class DefaultPartitioner implements Partitioner {

     //原子类
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

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
        //todo: 首先获取我们要发送消息对应的topic的分区消息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //todo: 计算topic的分区总数
        int numPartitions = partitions.size();
        //todo: 策略一：发送消息的时候 没有指定key
        if (keyBytes == null) {
            //定义一个计数器    每次执行实现加1的操作
            int nextValue = counter.getAndIncrement();
            //获取可用的分区信息
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                //一个自增的数对分区总数取模,来达到轮训的效果，达到负载均衡
                //6 % 3=0
                //7 % 3=1
                //8 % 3=2
                //9 % 3=0
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                //根据该值分配分区号
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            //todo: 策略二：指定key
            // hash the keyBytes to choose a partition
            // 直接对key取hashcode值 % 分区总数 =分区编号  这样如果是同一个key，最后肯定是发往同一个分区中
            //如果想要让消息发往同一个分区中，必须要指定对应的key
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {}

}
