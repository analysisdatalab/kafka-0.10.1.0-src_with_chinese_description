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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    //todo：更新元数据失败重试请求的最小时间间隔，默认值是100ms, 目的是减少网络的压力
    private final long refreshBackoffMs;
    //todo: 多久自动更新一次元数据，默认值是5分钟更新一次。
    private final long metadataExpireMs;
    //todo: 对于producer端来讲，元数据是有版本号,每次更新元数据，都会修改一下这个版本号。
    // version自增加1，主要用来判断metadata是否有更新
    private int version;
    //todo: 上一次更新元数据的时间。
    private long lastRefreshMs;
    //todo: 上一次成功更新元数据的时间。
    //如果正常情况下，如果每次都是更新成功的，那么lastRefreshMs和lastSuccessfulRefreshMs 应该是相同的。
    private long lastSuccessfulRefreshMs;
    //todo: Kafka集群本身的元数据对象。
    private Cluster cluster;
    //todo: 这是一个标识，用来判断是否更新元数据的标识之一。
    private boolean needUpdate;
    /* Topics with expiry time */
    //todo: 记录了当前已有的topics的过期时间
    private final Map<String, Long> topics;
    //todo:  事件监控者
    private final List<Listener> listeners;
    //todo: 当接收到 metadata 更新时, ClusterResourceListeners的列表
    private final ClusterResourceListeners clusterResourceListeners;
    //todo:  是否更新所有的 metadata
    private boolean needMetadataForAllTopics;
    //todo: 默认为 true, Producer会定时移除过期的topic
    private final boolean topicExpiryEnabled;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this(refreshBackoffMs, metadataExpireMs, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     * @param topicExpiryEnabled If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
     * will be reset on the next update.
     */
    public synchronized void add(String topic) {
        topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        //todo:获取当前时间
        long begin = System.currentTimeMillis();
        //todo 看剩余可以使用的时间，一开始是最大等待的时间。
        long remainingWaitMs = maxWaitMs;
        // todo: 如果元数据的版本号小于等于上一次的version，说明元数据还没有更新成功
        // 因为如果sender线程那儿 更新元数据，如果更新成功了，sender线程肯定回去累加这个version。
        while (this.version <= lastVersion) {
            //还有剩余时间
            if (remainingWaitMs != 0)
                //让当前线程阻塞等待。
                //这里虽然还没有去可能sender线程的源码，但是我们应该可以猜想它肯定会有这样的一个操作
                //如果更新元数据成功了，会唤醒该线程
                wait(remainingWaitMs);

            //如果代码执行到这儿 说明就要么就被唤醒了，要么就到时间了。
            //计算一下花了多少时间。
            long elapsed = System.currentTimeMillis() - begin;
            //超时了
            if (elapsed >= maxWaitMs)
                //报超时异常
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            //没有超时，就计算还剩的时间
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics))
            requestUpdate();
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     */
    //todo: 获取或者更新元数据
    public synchronized void update(Cluster cluster, long now) {
        Objects.requireNonNull(cluster, "cluster should not be null");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        //更新元数据的版本
        this.version += 1;

         //todo: 默认是true
        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            //todo:  第一次进来的topics为空，下面代码不会运行

            //todo: 如果代码第二次进来, 此时此刻进来，通过Producer的sender方法进来，这个时候已经有topic元数据信息
            //可以运行下面的代码逻辑了
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        for (Listener listener: listeners)
            listener.onMetadataUpdate(cluster);

        String previousClusterId = cluster.clusterResource().clusterId();
        //todo: 这个值默认是false，代码不会进入到该分支中
        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(cluster);
        } else {
            //todo: 直接把传进入来的Cluster对象赋值给cluster，内部仅仅只包含了kafka集群地址
             //所有初始化的时候，update这个方法并没有去服务端去拉取元数据
            this.cluster = cluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        if (!cluster.isBootstrapConfigured()) {
            String clusterId = cluster.clusterResource().clusterId();
            if (clusterId == null ? previousClusterId != null : !clusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", cluster.clusterResource().clusterId());
            clusterResourceListeners.onUpdate(cluster.clusterResource());
        }

        //todo: 该方法最最重要的作业就是唤醒上一讲中处于wait的线程,  metadata.awaitUpdate(version, remainingWaitMs);
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (partitionInfoList != null) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics);
    }
}
