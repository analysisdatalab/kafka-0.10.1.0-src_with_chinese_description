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

package org.apache.kafka.common.network;


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;
//todo: 这个kafkaChannel就是对java NIO中的socketChannel进行了封装。
public class KafkaChannel {
    //一个broker对应一个kafkaChannel，这里就是brokerId
    private final String id;
    //封装了java NIO中的socketChannel
    private final TransportLayer transportLayer;
    //kafka安全机制认证
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    //接受到的响应
    private NetworkReceive receive;
    //发送出去的响应
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        //todo: 对当前的连接移除掉OP_READ事件
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        //todo: 重新注册OP_READ事件
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        //kafkaChannel中缓存发送请求
        this.send = send;
        //todo: 绑定OP_WRITE事件，一旦绑定了该事件，我们就可以向服务端发送请求了
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            //初始化对象NetworkReceive
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        //todo:不断接受数据
        receive(receive);

        //todo: 是否读完一个完整的消息，这里对拆包问题进行解决
        if (receive.complete()) {
            receive.payload().rewind();
            //赋值receive给result
            result = receive;
            //把receive置为null，方便一下次读取
            receive = null;
        }
        return result;
    }

    public Send write() throws IOException {
        Send result = null;
        //todo: 发送网络请求
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        //todo: 最终执行发送请求的代码
        send.writeTo(transportLayer);
        //todo: 已经完成请求的发送
        if (send.completed())
            //移除OP_WRITE事件
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

}
