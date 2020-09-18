/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.Iterator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final long lingerMs;
    private final long retryBackoffMs;
    private final BufferPool free;
    private final Time time;
    //TopicPartition 分区-------》  Deque<RecordBatch> 队列
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private int drainIndex;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;

        //todo: CopyOnWriteMap这样的一个数据类型
        //这个数据结构在jdk里面是没有，是kafka自己设计的
        //这个数据结构设计的很好，有了这个数据结构，整体的提升了封装批次的这个流程性能
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            /**
             * todo: 步骤一：
             * 先根据分区获取该消息所属的队列中
             *      如果有已经存在的队列，那么我们就使用存在队列
             *      如果队列不存在，那么我们新创建一个队列
             */
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                /**
                 * todo: 步骤二：
                 * 尝试向队列里面的批次中添加数据
                 * 这里需要说明一下：目前我们只是有了队列，数据是存储在批次中（批次对象是需要分配内存的）
                 * 目前还没有分配到内存，所以添加数据会失败
                 */
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                //第一次进来appendResult返回结果为null
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record ba tch try to allocate a new batch
            /**
             * todo: 步骤三：计算一个批次的大小
             * 在消息的大小和批次的大小之间取一个最大值，用这个值作为当前这个批次的大小
             *
             * 有可能一个消息比设定好的大小还要大
             * 默认一个批次的大小是16K。
             * 所以我们看到这段代码以后，应该有一个启发
             * 如果我们生产者发送数据的时候，如果我们的消息大小超过了16K, 那么消息就是一条一条的发送出去的，
             * 这样的话批次这个概念的设计也就没有意义了
             *  结论：所有大家一定要根据自己公司的数据大小来设置批次的大小
             */
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());

            /**
             * todo: 步骤四：根据批次大小分配内存
             */
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");

                /**
                 *  todo: 步骤五：再次尝试把数据写入到批次中
                 *  代码第一次执行到这里，依然还是失败的，
                 *  目前只是有内存了，目前还没有创建批次，还没有把内存分配给批次
                 */
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                //appendResult 还是等于null
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    //释放内存
                    free.deallocate(buffer);
                    return appendResult;
                }

                /**
                 * todo: 步骤六： 根据内存大小封装批次
                 */
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());

                //尝试往这个批次写数据，到这个时候，代码会执行成功
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));

                /**
                 * todo: 步骤七： 把这个批次放入到这个队列尾部
                 */
                dq.addLast(batch);
                incomplete.add(batch);
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        //todo: 获取到队列中最后一个批次
        RecordBatch last = deque.peekLast();
        //todo: 第一次进来是没有批次的，if不会执行，直接返回null
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null)
                last.records.close();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
        }
        //todo: 第一次直接返回null
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        //todo: 定义一个数组用来存储超时的批次
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        //todo: 遍历每个队列
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            //todo: 获取每个分区的队列
            Deque<RecordBatch> dq = entry.getValue();
            //todo：获取队列的分区信息
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            //todo: 没有正在处理的分区批次
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    //获取指定分区队列最后一个批次
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    //迭代队列中的每一个批次
                    while (batchIterator.hasNext()) {
                        //获取批次
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.records.isFull();
                        //todo: 判断批次是否超时
                        // check if the batch is expired
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            //todo: 保存超时的批次
                            expiredBatches.add(batch);
                            //todo: 超时批次计数累加1
                            count++;
                            //todo: 从数据结构中移除超时的批次
                            batchIterator.remove();
                            //todo: 释放内存资源
                            deallocate(batch);
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", count);

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long now) {
        //todo: 重试次数+1
        batch.attempts++;
        //todo: 上一次重试的时间等于当前时间
        batch.lastAttemptMs = now;
        //todo: 上一次添加到队列中的时间
        batch.lastAppendTime = now;
        batch.setRetry();

        //todo: 如果之前有队列就使用之前的队列，没有就重新创建一个
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            //todo: 添加批次到队列的头部
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();

        //todo: waiters队列里面有数据，说明内存池中的内存不够了。
        //如果exhausted为true，说明内存池中的内存不够了
        boolean exhausted = this.free.queued() > 0;

        //todo: 遍历所有的分区，一个分区对应一个队列
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            //获取分区
            TopicPartition part = entry.getKey();
            //获取分区对应的队列
            Deque<RecordBatch> deque = entry.getValue();

            //todo: 根据分区获取该分区的leader partition对应的主机
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                //todo: 如果没有找到对应的主机， 进行标识，下一次重新获取该topic的元数据信息
                if (leader == null && !deque.isEmpty()) {
                    // This is a partition for which leader is not known, but messages are available to send.
                    // Note that entries are currently not removed from batches when deque is empty.
                    unknownLeaderTopics.add(part.topic());

                } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                    //todo: 获取队列中第一个批次
                    RecordBatch batch = deque.peekFirst();

                    //todo: 批次不为null, 判断该批次是否可以发送
                    if (batch != null) {
                        /**
                         * batch.attempts:重试的次数
                         * batch.lastAttemptMs ： 上一次重试的时间
                         * retryBackoffMs ： 重试的时间间隔
                         *
                         * backingOff ：表示重新发送数据的时间到了。
                         */
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;

                        /**
                         * nowMs :当前时间
                         * batch.lastAttemptMs ： 上一次重试的时间
                         * waitedTimeMs ：该批次已经等待了多久
                         */
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;

                        /**
                         *  使用场景驱动的方式去分析，第一次发送数据，之前也没有消息发送过，也没有重试这一说
                         *  backingOff 应该为false
                         *
                         *  timeToWaitMs=lingerMs 默认就是0 ，表示不需要等待
                         *  如果默认是0的话，就是来一条消息就发送一条信息，很明显是不合适的，
                         *  所以我们在发送数据的时候，一定要记得去配置这个参数
                         *  假设我们配置的是100ms,表示消息最多等待100ms后，必须要发送出去
                         */
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;

                        /**
                         * timeToWaitMs :最多能等多久
                         * waitedTimeMs ：已经等待了多久
                         * timeLeftMs：还能等待多久
                         */
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);

                        /**
                         *  deque.size()： 队列中至少有2个批次，这里至少会出现1个批次是写满了
                         *  batch.records.isFull()：该批次已经写满了
                         *
                         *  有可能出现只有1个批次，该批次数据已经写满了，也需要把数据发送出去
                         *  full： 是否有写满的批次
                         */
                        boolean full = deque.size() > 1 || batch.records.isFull();

                        /**
                         * waitedTimeMs: 已经等待的时间
                         * timeToWaitMs：最多能等多久
                         *
                         * expired：是否达到了最大的等待时间
                         */
                        boolean expired = waitedTimeMs >= timeToWaitMs;

                        /**
                         * （1）full:    如果一个批次写满了，无论时间有没有到，都需要把消息发送出去
                         * （2）expired：时间到了，不管批次数据是否写满，也需要把消息发送出去
                         * （3）exhausted ：内存不够了，也需要把消息发送出去，最后释放内存
                         *  (4)(5) closed/flushInProgress :producer客户端线程关闭，也需要把消息发送出去
                         */
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();

                        /**
                         * 可以发送消息了
                         */
                        if (sendable && !backingOff) {
                            //todo：把可以发送该批次数据分区的leader主机添加到集合中
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     * 
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches.
                if (!muted.contains(tp)) {
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                if (!backoff) {
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        break;
                                    } else {
                                        RecordBatch batch = deque.pollFirst();
                                        batch.records.close();
                                        size += batch.records.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        //todo: 直接从batches里面获取当前分区对应的存储队列
        Deque<RecordBatch> d = this.batches.get(tp);
        //我们是以场景驱动的方式，代码第一次执行到这里的话，获取不到队列，该变量d为null
        if (d != null)
            return d;

        //创建出一个新的空队列
        d = new ArrayDeque<>();
        //把空的队列存入到batches这个map数据结构中
        //使用putIfAbsent方法添加键值对，如果map集合中没有该key对应的值，则直接添加，并返回null，
        // 如果已经存在对应的值，则依旧为原来的值。
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            //直接返回新的结果
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        //todo: 从IncompleteRecordBatches中移除成功处理的批次
        incomplete.remove(batch);

        //todo: 释放内存
        free.deallocate(batch.records.buffer(), batch.records.initialCapacity());
    }
    
    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }
    
    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.records.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
    
    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     */
    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }
        
        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }
        
        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }
        
        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
