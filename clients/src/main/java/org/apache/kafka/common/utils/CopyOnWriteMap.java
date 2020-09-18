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
package org.apache.kafka.common.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 *
 *
 * (1) 这个数据结构在高并发的场景下是线程安全的
 * (2) 采用了读写分离的思想设计的数据结构
 *       每次插入数据的时候都开辟了新的内存空间，所以这里存在一个小缺点，写数据的时候会比较耗费内存空间
 *
 * (3) 这样的一个数据结构，适合写少读多的场景
 *     读数据的时候性能很高
 *
 *     batches这个对象存储数据的时候就是使用了这种数据结构
 *     对于batches来说，它面对的就是读多写少的场景
 *
 *     batches：
 *          读数据：
 *              每生产一条消息，都会从batches里面读取数据
 *               假如每秒中生产10w条数据，意味着要每秒读取10w次，这绝对是一个高并发的场景
 *
 *          写数据：
 *              假如有100个分区，那么就插入100次数据就可以了，并且新的队列只需要插入一次就可以了
 *               所以这是一个低频的操作
 *
 * 总结： kafka为了存储这些数据，设计出了这种读写分离的数据结构，很好的解决了高并发下读多写少的场景，
 *        这一点是很值得我们程序员去学习的
 */

public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {

    /**
     * 核心的变量就是一个map
     *
     *  这个map有个特点，它的修饰符是volatile关键字
     *  作用；在多线程的情况下，如果这个map的值发生变化，其他线程是可见的
     */
    private volatile Map<K, V> map;

    public CopyOnWriteMap() {
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map) {
        this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    /**
     *  没有加锁，读取数据的时候性能很高（高并发场景下，性能肯定很好）
     *  并且是线程安全的（由于采用了读写分离的思想）
     */
    @Override
    public V get(Object k) {
        return map.get(k);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public synchronized void clear() {
        this.map = Collections.emptyMap();
    }

    /**
     * (1) 整个方法是使用synchronized去修饰的，它是线程安全的
     *     即使加了锁，这段代码的性能依然很好，因为里面都是纯内存操作的
     *
     * (2) 读数据的流程和写数据的流程分离了，这里采用了读写分离的设计思想
     *     读操作和写操作是互不影响
     *     所以我们读数据的操作就是线程安全的
     *
     * (3) 最后把之赋值给了map，map是使用了volatile去修饰
     *     说明这个map是具有可见性的，如果map的值发生了变化，进行get操作的时候，是可以感知到的
     *
     */
    @Override
    public synchronized V put(K k, V v) {
        //开辟新的内存空间
        Map<K, V> copy = new HashMap<K, V>(this.map);
        //插入数据
        V prev = copy.put(k, v);
        //赋值给map
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    @Override
    public synchronized V remove(Object key) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized V putIfAbsent(K k, V v) {
        //如果k不存在，就调用内部的put方法
        if (!containsKey(k))
            return put(k, v);
        else
        //如果存在就返回结果
            return get(k);
    }

    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean replace(K k, V original, V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized V replace(K k, V v) {
        if (containsKey(k)) {
            return put(k, v);
        } else {
            return null;
        }
    }

}
