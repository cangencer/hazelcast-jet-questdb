package com.hazelcast.jet.contrib.influxdb;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        StreamStage<String> points = p.drawFrom(TestSources.itemStream(1_000_000))
                                      .withNativeTimestamps(0)
                                      .map(e -> Point.measurement("mem_usage")
//                        .time(e.timestamp(), TimeUnit.MILLISECONDS)
                                                     .addField("value", e.sequence())
                                                     .tag("tag1", "tagValue1")
                                                     .tag("tag2", "tagValue2")
                                                     .build().lineProtocol());

        points.drainTo(getQuestDbSink("127.0.0.1", 9009));

        points.window(WindowDefinition.tumbling(TimeUnit.SECONDS.toMillis(1)))
              .aggregate(AggregateOperations.counting())
              .drainTo(Sinks.logger());

        JetInstance jet = Jet.newJetInstance();

        jet.newJob(p).join();

    }

    private static Sink<String> getQuestDbSink(String host, int port) {
        return SinkBuilder.sinkBuilder("questDb", ctx -> new QuestDBSink(host, port)).receiveFn(QuestDBSink::receive)
                          .flushFn(QuestDBSink::send)
                          .build();
    }

    private static class QuestDBSink {

        private final DatagramSocket socket;
        private final DatagramPacket packet;
        private final ByteBuffer buffer = ByteBuffer.allocate(65507);
        private final byte[] delimiter = "\n".getBytes(StandardCharsets.UTF_8);

        public QuestDBSink(String host, int port) {
            try {
                this.packet = new DatagramPacket(buffer.array(), 0, InetAddress.getByName(host), port);
                this.socket = new DatagramSocket();
            } catch (SocketException | UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        public void receive(String item) {
            byte[] bytes = item.getBytes(StandardCharsets.UTF_8);
            if (buffer.remaining() < bytes.length + delimiter.length) {
                send();
            }
            buffer.put(bytes);
            buffer.put(delimiter);
        }

        public void send() {
            packet.setData(buffer.array(), 0, buffer.position());
            try {
                socket.send(packet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            buffer.clear();
        }
    }
}
