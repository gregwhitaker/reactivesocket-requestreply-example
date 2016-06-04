/*
 * Copyright 2016 Greg Whitaker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.gregwhitaker.requestreply;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.netty.tcp.server.ReactiveSocketServerHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.net.InetSocketAddress;

/**
 * Server that receives and replies to messages.
 */
public class Server {
    private final InetSocketAddress bindAddress;

    /**
     * Main entry-point of the Server application.
     *
     * @param args command line arguments
     * @throws Exception
     */
    public static void main(String... args) throws Exception {
        Server server = new Server(new InetSocketAddress("localhost", 8080));
        server.start();
    }

    /**
     * Initializes this instance of {@link Server}.
     *
     * @param bindAddress ip address and port to listen for reactive socket connections
     */
    public Server(InetSocketAddress bindAddress) {
        this.bindAddress = bindAddress;
    }

    /**
     * Starts the server.
     *
     * @throws Exception
     */
    private void start() throws Exception {
        ServerBootstrap server = new ServerBootstrap();
        server.group(new NioEventLoopGroup(1), new NioEventLoopGroup(4))
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ReactiveSocketChannelInitializer());

        server.bind(bindAddress).sync();
    }

    /**
     * Initializes the netty channel and listens for reactive socket connections.
     */
    class ReactiveSocketChannelInitializer extends ChannelInitializer {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(ReactiveSocketServerHandler.create((setupPayload, reactiveSocket) -> {
                return new RequestHandler.Builder().withRequestResponse(payload -> {
                    ByteBuf buffer = Unpooled.buffer(payload.getData().capacity());
                    buffer.writeBytes(payload.getData());

                    byte[] bytes = new byte[buffer.capacity()];
                    buffer.readBytes(bytes);

                    System.out.println("Server Received: " + new String(bytes));

                    return new Publisher<Payload>() {
                        @Override
                        public void subscribe(Subscriber<? super Payload> s) {

                        }
                    };
                }).build();
            }));
        }
    }
}
