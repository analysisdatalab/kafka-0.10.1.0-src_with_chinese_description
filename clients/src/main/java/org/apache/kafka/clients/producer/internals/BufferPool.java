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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public final class BufferPool {

    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock;
    //内存池就是一个队列，队列中存放的就是一块一块的内存。类似于一个连接池
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     * 
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        //todo: 如果要申请的内存大小超过了 32M 就报异常
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        //加锁
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            //todo: poolableSize就是一个批次的大小，默认就是16KB
            //如果size我们要申请的批次大小正好等于一个批次默认的大小，并且内存池不为空

            //todo:代码第一次进来的话，内存池中是没有内存的，这里是获取不到
            if (size == poolableSize && !this.free.isEmpty())
                //直接从内存池拿出一块内存就可以了
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            //todo: 内存块的个数* 批次的大小=内存池的内存大小
            int freeListSize = this.free.size() * this.poolableSize;

            /**
             * size是我们要申请的内存
             *  this.availableMemory + freeListSize 目前可用的总内存
             */
            if (this.availableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
                freeUp(size);
                //todo: 进行内存扣减
                this.availableMemory -= size;
                lock.unlock();
                //todo: 直接分配内存
                return ByteBuffer.allocate(size);
            } else {
                // we are out of memory and will have to block
                /**
                 * 还有一种情况是，比如内存池中还剩10K的内存，但是需要申请32K的内存
                 */

                //统计分配的内存
                int accumulated = 0;
                ByteBuffer buffer = null;
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                /**
                 * 等待别人释放内存
                 */
                this.waiters.addLast(moreMemory);
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                /**
                 *  总的分配内存思路:
                 *   可能一下子给不了这么大的内存，我可以先一点一点的分配
                 */

                //如果分配的内存的大小，还是没有达到我需要申请的内存大小，内存池就会一直不断的一点一点的去分配
                //等待别人释放内存
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //todo:等待别人释放内存，如果这儿的代码是等待wait操作
                        //假设其他线程归还了内存，那么这里的代码就可以被唤醒，代码就继续往下执行
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    //todo: 再次尝试看看内存池中是否有内存了，如果有，并且你申请的内存大小就是一个批次的大小
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        //todo: 这里就是可以直接获取到内存
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        freeUp(size - accumulated);
                        //todo: 可以给你分配的内存
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        //todo: 内存扣减
                        this.availableMemory -= got;
                        //todo: 统计已经分配的内存大小
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        //加锁
        lock.lock();
        try {
            //todo:需要申请的内存等于一个批次的内存大小，就把内存归还给内存池
            //该判断可以有效的提高内存的利用率，等于批次大小的内存直接归还内存池利用率才是最高的
            if (size == this.poolableSize && size == buffer.capacity()) {
                //todo: 清空数据
                buffer.clear();
                //todo:内存放入到内存池中
                this.free.add(buffer);
            } else {
            //todo:否则不相等，就直接归还给可用的内存中
                this.availableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                //todo: 内存释放后，需要唤醒处于wait等待内存的线程----> !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                //唤醒正在等待分配内存的线程
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            //获取waiters队列大小
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
