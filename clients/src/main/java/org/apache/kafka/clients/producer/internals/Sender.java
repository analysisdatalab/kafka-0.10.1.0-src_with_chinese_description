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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /* the state of each nodes connection */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    private final Metadata metadata;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* param clientId of the client */
    private String clientId;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeout;

    public Sender(KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  Metrics metrics,
                  Time time,
                  String clientId,
                  int requestTimeout) {
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.clientId = clientId;
        this.sensors = new SenderMetrics(metrics);
        this.requestTimeout = requestTimeout;
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // main loop, runs until close is called
        //todo: 其实代码就是一个死循环，一直运行
        //所以我们应该知道了sender线程启动起来后就是一直在运行的
        while (running) {
            try {
                //todo: 调用run方法
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && (this.accumulator.hasUnsent() || this.client.inFlightRequestCount() > 0)) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        if (forceClose) {
            // We need to fail all the incomplete batches and wake up the threads waiting on
            // the futures.
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     * 
     * @param now
     *            The current POSIX time in milliseconds
     */
    void run(long now) {
        //todo: 1、获取元数据
        // 因为这里采用场景驱动的方式，由于代码第一次进来，目前还没有获取到元数据。 所以这个Cluster对象中是没有元数据的

        //todo: 注意: 如果没有获取到元数据，下面的一些代码逻辑不需要看了，因为下面的代码都需要依赖于这个元数据
        //todo: 我们直接看run方法，最后一行代码就可以了，就是通过该行代码去拉取的元数据


        /**
         * 现在我们的代码是第二次进来，这个时候就应该有对应的元数据了。
         * todo:步骤一：
         *      获取元数据
         */
        Cluster cluster = metadata.fetch();

        /***
         * todo:  步骤二：
         *      判断哪些partition有消息可以发送，获取到这个partition的leaderPartition对应的broker主机
         *
         */
        // get the list of partitions with data ready to send
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        /**
         * todo: 步骤三：
         *     标识还没有拉取到元数据的topic
         */
        // if there are any partitions whose leaders are not known yet, force metadata update
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic);
            this.metadata.requestUpdate();
        }

        // remove any nodes we aren't ready to send to
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            /**
             * todo: 步骤四：
             *      检查与要发送数据的主机网络是否建立好
             */
            if (!this.client.ready(node, now)) {
                //如果网络为建立好，这里返回的就是false， !false就可以进来了
                //移除这些主机
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        // create produce requests
        /**
         * todo: 步骤五：
         *        有可能要发送的partition有很多个，这些partition的leader分区可能在同一台节点上
         *        p0:leader ----> brokerId=0
         *        p1:leader ----> brokerId=0
         *        p2:leader ----> brokerId=1
         *        p3:leader ----> brokerId=2
         *   按照brokerId进行分区，同一个broker的partition为同一组
         *  key    value
         *  0      p0,p1
         *  1      p2
         *  2      p3
         */

        //如果网络没有建立好，这块代码也是不会执行的
        Map<Integer, List<RecordBatch>> batches = this.accumulator.drain(cluster,
                                                                         result.readyNodes,
                                                                         this.maxRequestSize,
                                                                         now);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            //如果网络没有建立好，batches为空，这块代码也不会执行
            for (List<RecordBatch> batchList : batches.values()) {
                for (RecordBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        /**
         * TODO: 步骤六：
         * todo: 放弃超时的batches
         * 超时批次的处理逻辑
         */
        List<RecordBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        // update sensors
        for (RecordBatch expiredBatch : expiredBatches)
            this.sensors.recordErrors(expiredBatch.topicPartition.topic(), expiredBatch.recordCount);

        sensors.updateProduceRequestMetrics(batches);

        /**
         *  TODO:步骤七：
         *       创建发送消息的request请求
         *
         * 向broker发送消息的时候，有些分区数据在同一台服务器上
         *  如果我们一个分区一个分区的发送网络请求，那么网络请求有一些频繁，我们要知道这个网络资源是比较珍贵的，
         *  这里需要把发送给同一个broker的分区数据组合成一个请求，然后统一一次发送出去，减少网络请求
         *
         *   按照brokerId进行分区，同一个broker的partition为同一组
         *  key    value
         *  0      p0,p1
         *  1      p2
         *  2      p3
         *
         */
        //todo:如果网络没有建立好，这块代码也是不会执行的
        List<ClientRequest> requests = createProduceRequests(batches, now);
        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout is determined by nodes that have partitions with data
        // that isn't yet sendable (e.g. lingering, backing off). Note that this specifically does not include nodes
        // with sendable data that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (result.readyNodes.size() > 0) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            log.trace("Created {} produce requests: {}", requests.size(), requests);
            pollTimeout = 0;
        }
        //todo: 遍历请求
        for (ClientRequest request : requests)
             //todo:发送请求
            client.send(request, now);

        // if some partitions are already ready to be sent, the select time would be 0;
        // otherwise if some partition already has some data accumulated but not ready yet,
        // the select time will be the time difference between now and its linger expiry time;
        // otherwise the select time will be the time difference between now and the metadata expiry time;

        //todo: 重点就是看这个方法，就是用这个方法拉取kafka集群的元数据
        /**
         * todo: 步骤八
         *      真正执行网络操作的都是这个NetworkClient组件,作用：发送请求，接受响应
         */

        //猜想这里可能会去建立好网络
        this.client.poll(pollTimeout, now);
    }

    /**
     * Start closing the sender (won't actual ly complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.

        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    /**
     * Handle a produce response
     *
     * todo: 执行回调函数的处理逻辑
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, RecordBatch> batches, long now) {
        int correlationId = response.request().request().header().correlationId();
        //todo: 发送请求时，broker可能失去连接，这是一个小概率事件
        if (response.wasDisconnected()) {
            log.trace("Cancelled request {} due to node {} being disconnected", response, response.request()
                                                                                                  .request()
                                                                                                  .destination());
            for (RecordBatch batch : batches.values())
                completeBatch(batch, Errors.NETWORK_EXCEPTION, -1L, Record.NO_TIMESTAMP, correlationId, now);
        } else {
            log.trace("Received produce response from node {} with correlation id {}",
                      response.request().request().destination(),
                      correlationId);
            // if we have a response, parse it

            //todo: 正常情况下，进入到该分支
            if (response.hasResponse()) {
                ProduceResponse produceResponse = new ProduceResponse(response.responseBody());
                //todo: 遍历每个分区的响应
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    //todo: 获取每一个分区对象TopicPartition
                    TopicPartition tp = entry.getKey();
                    //todo: 获取每一个分区的响应
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    //todo: 服务器处理失败了，返回异常信息
                    Errors error = Errors.forCode(partResp.errorCode);

                     //todo: 获取分区的数据
                    RecordBatch batch = batches.get(tp);

                     //todo: 对响应进行处理
                    completeBatch(batch, error, partResp.baseOffset, partResp.timestamp, correlationId, now);
                }
                this.sensors.recordLatency(response.request().request().destination(), response.requestLatencyMs());
                this.sensors.recordThrottleTime(response.request().request().destination(),
                                                produceResponse.getThrottleTime());
            } else {
                // this is the acks = 0 case, just complete all requests
                /**
                 * todo: acks=0  不需要返回响应
                 *
                 * 在实际的生产环境中一般不会设置acks=0
                 */
                for (RecordBatch batch : batches.values())
                    completeBatch(batch, Errors.NONE, -1L, Record.NO_TIMESTAMP, correlationId, now);
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     * 
     * @param batch The record batch
     * @param error The error (or null if none)
     * @param baseOffset The base offset assigned to the records if successful
     * @param timestamp The timestamp returned by the broker for this batch
     * @param correlationId The correlation id for the request
     * @param now The current POSIX time stamp in milliseconds
     */
    private void completeBatch(RecordBatch batch, Errors error, long baseOffset, long timestamp, long correlationId, long now) {
        /**
         * todo: 如果响应中带有异常，并且该请求时可以重试的
         */
        if (error != Errors.NONE && canRetry(batch, error)) {
            // retry
            log.warn("Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                     correlationId,
                     batch.topicPartition,
                     this.retries - batch.attempts - 1,
                     error);

            //todo: 把发送失败的批次重新加入到队列中
            this.accumulator.reenqueue(batch, now);
            this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
        } else {
            /**
             * todo: 如果带有异常 或者 不允许重试
             */
            RuntimeException exception;
            //todo: 带有权限的异常
            if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                //todo: 自定义权限异常
                exception = new TopicAuthorizationException(batch.topicPartition.topic());
            else
                //todo: 非权限的异常直接获取异常
                exception = error.exception();
            // tell the user the result of their request
            /**
             *  todo: 核心代码,对异常进行处理,里面调用了用户传进来的回调函数
             */
            batch.done(baseOffset, timestamp, exception);

            //todo: 回收内存资源
            this.accumulator.deallocate(batch);
            if (error != Errors.NONE)
                this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
        }
        if (error.exception() instanceof InvalidMetadataException) {
            if (error.exception() instanceof UnknownTopicOrPartitionException)
                log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                        "topic/partition may not exist or the user may not have Describe access to it", batch.topicPartition);
            metadata.requestUpdate();
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed
     */
    private boolean canRetry(RecordBatch batch, Errors error) {
        return batch.attempts < this.retries && error.exception() instanceof RetriableException;
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private List<ClientRequest> createProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
        List<ClientRequest> requests = new ArrayList<ClientRequest>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet())
            requests.add(produceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue()));
        return requests;
    }

    /**
     * Create a produce request from the given record batches
     * todo: 从批次中封装请求
     */
    private ClientRequest produceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, ByteBuffer> produceRecordsByPartition = new HashMap<TopicPartition, ByteBuffer>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<TopicPartition, RecordBatch>(batches.size());
        for (RecordBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            produceRecordsByPartition.put(tp, batch.records.buffer());
            recordsByPartition.put(tp, batch);
        }
        //todo:基于每一个批次封装request请求
        ProduceRequest request = new ProduceRequest(acks, timeout, produceRecordsByPartition);
        //封装发送数据的请求
        RequestSend send = new RequestSend(Integer.toString(destination),
                                           this.client.nextRequestHeader(ApiKeys.PRODUCE),
                                           request.toStruct());

        //todo: 定义回调函数
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                //todo: 回调函数处理的业务逻辑
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };

        //todo: 请求绑定回调函数
        return new ClientRequest(now, acks != 0, send, callback);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * A collection of sensors for the sender
     */
    private class SenderMetrics {

        private final Metrics metrics;
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor produceThrottleTimeSensor;

        public SenderMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = "producer-metrics";

            this.batchSizeSensor = metrics.sensor("batch-size");
            MetricName m = metrics.metricName("batch-size-avg", metricGrpName, "The average number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Avg());
            m = metrics.metricName("batch-size-max", metricGrpName, "The max number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            m = metrics.metricName("compression-rate-avg", metricGrpName, "The average compression rate of record batches.");
            this.compressionRateSensor.add(m, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            m = metrics.metricName("record-queue-time-avg", metricGrpName, "The average time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Avg());
            m = metrics.metricName("record-queue-time-max", metricGrpName, "The maximum time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            m = metrics.metricName("request-latency-avg", metricGrpName, "The average request latency in ms");
            this.requestTimeSensor.add(m, new Avg());
            m = metrics.metricName("request-latency-max", metricGrpName, "The maximum request latency in ms");
            this.requestTimeSensor.add(m, new Max());

            this.produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
            m = metrics.metricName("produce-throttle-time-avg", metricGrpName, "The average throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Avg());
            m = metrics.metricName("produce-throttle-time-max", metricGrpName, "The maximum throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            m = metrics.metricName("record-send-rate", metricGrpName, "The average number of records sent per second.");
            this.recordsPerRequestSensor.add(m, new Rate());
            m = metrics.metricName("records-per-request-avg", metricGrpName, "The average number of records per request.");
            this.recordsPerRequestSensor.add(m, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            m = metrics.metricName("record-retry-rate", metricGrpName, "The average per-second number of retried record sends");
            this.retrySensor.add(m, new Rate());

            this.errorSensor = metrics.sensor("errors");
            m = metrics.metricName("record-error-rate", metricGrpName, "The average per-second number of record sends that resulted in errors");
            this.errorSensor.add(m, new Rate());

            this.maxRecordSizeSensor = metrics.sensor("record-size-max");
            m = metrics.metricName("record-size-max", metricGrpName, "The maximum record size");
            this.maxRecordSizeSensor.add(m, new Max());
            m = metrics.metricName("record-size-avg", metricGrpName, "The average record size");
            this.maxRecordSizeSensor.add(m, new Avg());

            m = metrics.metricName("requests-in-flight", metricGrpName, "The current number of in-flight requests awaiting a response.");
            this.metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return client.inFlightRequestCount();
                }
            });
            m = metrics.metricName("metadata-age", metricGrpName, "The age in seconds of the current producer metadata being used.");
            metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return (now - metadata.lastSuccessfulUpdate()) / 1000.0;
                }
            });
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);
                String metricGrpName = "producer-topic-metrics";

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName m = this.metrics.metricName("record-send-rate", metricGrpName, metricTags);
                topicRecordCount.add(m, new Rate());

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                m = this.metrics.metricName("byte-rate", metricGrpName, metricTags);
                topicByteRate.add(m, new Rate());

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                m = this.metrics.metricName("compression-rate", metricGrpName, metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                m = this.metrics.metricName("record-retry-rate", metricGrpName, metricTags);
                topicRetrySensor.add(m, new Rate());

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                m = this.metrics.metricName("record-error-rate", metricGrpName, metricTags);
                topicErrorSensor.add(m, new Rate());
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<RecordBatch>> batches) {
            long now = time.milliseconds();
            for (List<RecordBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (RecordBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Utils.notNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Utils.notNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.records.sizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Utils.notNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.records.compressionRate());

                    // global metrics
                    this.batchSizeSensor.record(batch.records.sizeInBytes(), now);
                    this.queueTimeSensor.record(batch.drainedMs - batch.createdMs, now);
                    this.compressionRateSensor.record(batch.records.compressionRate());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        public void recordThrottleTime(String node, long throttleTimeMs) {
            this.produceThrottleTimeSensor.record(throttleTimeMs, time.milliseconds());
        }

    }

}
