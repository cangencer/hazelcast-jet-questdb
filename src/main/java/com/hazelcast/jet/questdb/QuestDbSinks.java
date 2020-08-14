package com.hazelcast.jet.questdb;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cutlass.line.udp.LineProtoSender;
import io.questdb.cutlass.line.udp.LineTCPProtoSender;

public class QuestDbSinks {

    /**
     * A sink which writes to QuestDB using the {@link TableWriter} API. This requires
     * a write lock on the table and the database to be present locally on the Jet node.
     * This is the most performant option for writing.
     * <p>
     * The table and the schema must be created before any writing takes place.
     *
     * @param path Path to the data directory of questdb
     * @param table Name of the table
     * @param writeFn A function which creates a row and writes the item to the sink
     */
    public static <T> Sink<T> tableWriter(String path, String table, BiConsumerEx<Row, T> writeFn) {
        return SinkBuilder.sinkBuilder("questDbTableWriter(" + table + ")",
                ctx -> new TableWriterSink<>(path, table, writeFn))
                .<T>receiveFn(TableWriterSink::receive)
                .flushFn(TableWriterSink::flush)
                .destroyFn(TableWriterSink::destroy)
                .build();
    }

    /**
     * A sink which writes to QuestDB using the Influx Line Protocol format
     * over the network.
     * <p>
     * You can use {@link LineProtoSender} for UDP or {@link LineTCPProtoSender } for TCP.
     * For more information about the arguments, see the
     * <a href="https://questdb.io/docs/api/java#influxdb-sender-library">QuestDB docs</a>
     *
     * @param createSenderFn function to create the protocol sender
     * @param writeFn function to write the item to the sender
     * @param <T> type of the input record
     */
    public static <T> Sink<T> lineProtocol(
            SupplierEx<LineProtoSender> createSenderFn,
            BiConsumerEx<LineProtoSender, T> writeFn
    ) {
        return SinkBuilder.sinkBuilder("questDbLineProtocolWriter",
                ctx -> new LineProtocolSink<>(createSenderFn.get(), writeFn))
                .<T>receiveFn(LineProtocolSink::receive)
                .flushFn(LineProtocolSink::send)
                .destroyFn(LineProtocolSink::destroy)
                .build();
    }

    private static class LineProtocolSink<T> {

        private final LineProtoSender sender;
        private final BiConsumerEx<LineProtoSender, T> consumeFn;

        public LineProtocolSink(LineProtoSender sender,
                BiConsumerEx<LineProtoSender, T> consumeFn) {
            this.sender = sender;
            this.consumeFn = consumeFn;
        }

        public void receive(T item) {
            consumeFn.accept(sender, item);
        }

        public void send() {
            sender.flush();
        }

        public void destroy() {
            sender.close();
        }
    }

    private static class TableWriterSink<T> {

        private TableWriter writer;
        private final BiConsumerEx<Row, T> consumeFn;

        public TableWriterSink(
                String path,
                String table,
                BiConsumerEx<Row, T> consumeFn
        ) {
            this.consumeFn = consumeFn;
            CairoConfiguration configuration = new DefaultCairoConfiguration(path);
            this.writer = new TableWriter(configuration, table);
        }

        public void receive(T item) {
            Row row = writer.newRow();
            consumeFn.accept(row, item);
            row.append();
        }

        public void flush() {
            writer.commit();
        }

        public void destroy() {
            writer.close();
        }
    }
}
