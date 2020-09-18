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
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    /**
     * todo: 构建方法，初始化生产者对象
     * @param topic
     * @param isAsync
     */
    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        // 指定kafka集群地址
        props.put("bootstrap.servers", "localhost:9092");
        //一般不做设置
        props.put("client.id", "DemoProducer");
        props.put("linger.ms",0);
        props.put("buffer.memory",32*1024*1024);
        props.put("batch.size",16*1024);
        //设置序列化的类。 数据传输的过程中需要进行序列化，消费者获取数据需要反序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //todo:初始化KafkaProducer对象
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        // todo: 一直会往kafka发送数据
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            //isAsync , kafka发送数据的时候，有两种方式
            //todo:1: 异步发送   isAsync=true
            //todo:2: 同步发送   isAsync=false
            if (isAsync) { // Send asynchronously
                //todo:异步发送
                //这样的方式，性能比较好，我们生产代码用的就是这种方式。
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    //todo:同步发送
                    //todo:发送一条消息，等这条消息所有的后续工作都完成以后才继续下一条消息的发送。
                    producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if(exception != null){
            System.out.println("有异常");
            //一般我们生产里面 还会有其它的备用的链路。
            //在进行消息的异步发送的过程中，如果出现了异常，可以在这里做一些补偿机制
        }else{
            System.out.println("说明没有异常信息，成功的！！");
        }

        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
