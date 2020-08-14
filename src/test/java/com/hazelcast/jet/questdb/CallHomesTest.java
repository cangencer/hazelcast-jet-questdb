package com.hazelcast.jet.questdb;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.avro.AvroSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cutlass.line.udp.LineProtoSender;
import io.questdb.cutlass.line.udp.LineTCPProtoSender;
import io.questdb.network.Net;
import org.junit.Test;

import java.util.UUID;

import static com.hazelcast.jet.questdb.QuestDbSinks.lineProtocol;
import static com.hazelcast.jet.questdb.QuestDbSinks.tableWriter;

public class CallHomesTest {


    @Test
    public void callHomes_to_questDb_usingTableWriter() {
        JetInstance jet = Jet.bootstrappedInstance();

        Pipeline p = Pipeline.create();
        BatchStage<CallHome> s = p.readFrom(
                AvroSources.files(System.getenv("HOME") + "/tmp/callhome/avro", CallHome.class)
        );

        s.writeTo(tableWriter("/usr/local/var/questdb/db", "callhomes", CallHomesTest::writeRow));

        // s.writeTo(Sinks.map("callhomes", c -> UUID.randomUUID().toString(), c -> c));

        try {
            jet.newJob(p, new JobConfig().setStoreMetricsAfterJobCompletion(true)).join();
        } finally {
            jet.shutdown();
        }
    }

    @Test
    public void callHomes_to_questDb_usingLineProtocol() {
        JetInstance jet = Jet.bootstrappedInstance();

        Pipeline p = Pipeline.create();
        BatchStage<CallHome> s = p.readFrom(
                AvroSources.files(System.getenv("HOME") + "/tmp/callhome/avro", CallHome.class)
        );

        s.writeTo(lineProtocol(
                () -> new LineTCPProtoSender(Net.parseIPv4("127.0.0.1"), 9009, 65536),
                CallHomesTest::toMetric)
        );


        try {
            jet.newJob(p, new JobConfig().setStoreMetricsAfterJobCompletion(true)).join();
        } finally {
            jet.shutdown();
        }
    }

    public static void writeRow(Row row, CallHome r) {
        row.putStr(0, r.getIp());
        row.putSym(1, String.valueOf(r.getVersion()));
        row.putTimestamp(2, r.getPingTime());
        row.putStr(3, r.getMachineId());
        row.putBool(4, r.getEnterprise());
        row.putSym(5, String.valueOf(r.getLicense()));
        row.putSym(6, String.valueOf(r.getPardotId()));
        row.putStr(7, r.getClusterId());
        row.putChar(8, firstChar(r.getClusterSize()));
        row.putChar(9, firstChar(r.getClientsSize()));
        row.putSym(10, String.valueOf(r.getOrg()));
        row.putSym(11, String.valueOf(r.getCity()));
        row.putSym(12, String.valueOf(r.getCountry()));
        row.putDouble(13, toDouble(r.getLat()));
        row.putDouble(14, toDouble(r.getLong$()));
        row.putShort(15, toShort(r.getHDMemoryInGB()));
        row.putShort(16, toShort(r.getConnectedClientsCPP()));
        row.putShort(17, toShort(r.getConnectedClientsDotNet()));
        row.putShort(18, toShort(r.getConnectedClientsJava()));
        row.putLong(19, r.getClusterUpTime() == null ? 0 : r.getClusterUpTime());
        row.putLong(20, r.getNodeUptime() == null ? 0 : r.getNodeUptime());
        row.putSym(21, String.valueOf(r.getOsName()));
        row.putSym(22, String.valueOf(r.getOsArch()));
        row.putSym(23, String.valueOf(r.getOsVersion()));
        row.putSym(24, String.valueOf(r.getJvmName()));
        row.putSym(25, String.valueOf(r.getJvmVersion()));
        row.putShort(26, toShort(r.getConnectedClientsNodeJS()));
        row.putShort(27, toShort(r.getConnectedClientsPython()));
        row.putSym(28, String.valueOf(r.getState()));
        row.putSym(29, String.valueOf(r.getJetVersion()));
        row.putSym(30, String.valueOf(r.getMcVersion()));
        row.putSym(31, String.valueOf(r.getMcLicense()));
        row.putShort(32, toShort(r.getConnectedClientsGo()));
        row.putBool(33, r.getOem() == null ? false : r.getOem());
    }

    static void toMetric(LineProtoSender sender, CallHome r) {
        sender.metric("callhomes2")
              .tagEscaped("version", String.valueOf(r.getVersion()))
              .tagEscaped("org", String.valueOf(r.getOrg()))
              .tagEscaped("city", String.valueOf(r.getCity()))
              .tagEscaped("country", String.valueOf(r.getCountry()))
              .tagEscaped("osName", String.valueOf(r.getOsName()))
              .tagEscaped("osArch", String.valueOf(r.getOsArch()))
              .tagEscaped("osVersion", String.valueOf(r.getOsVersion()))
              .tagEscaped("jvmName", String.valueOf(r.getJvmName()))
              .tagEscaped("jvmVersion", String.valueOf(r.getJvmVersion()))
              .tagEscaped("state", String.valueOf(r.getState()))
              .tagEscaped("jetVersion", String.valueOf(r.getJetVersion()))
              .tagEscaped("mcVersion", String.valueOf(r.getMcVersion()))
              .tagEscaped("mcLicense", String.valueOf(r.getMcLicense()))
              .field("clusterSize", firstChar(r.getClusterSize()))
              .field("clientsSize", firstChar(r.getClientsSize()))
              .field("connectClientsGo", toShort(r.getConnectedClientsGo()))
              .field("connectedClientsCPP", toShort(r.getConnectedClientsCPP()))
              .field("connectClientsDotNet", toShort(r.getConnectedClientsDotNet()))
              .field("connectClientsJava", toShort(r.getConnectedClientsJava()))
              .field("connectClientsNodeJS", toShort(r.getConnectedClientsNodeJS()))
              .field("connectClientsPython", toShort(r.getConnectedClientsPython()))
              .field("ip", r.getIp())
              .field("machineId", String.valueOf(r.getMachineId()))
              .field("enterprise", r.getEnterprise())
              .field("license", String.valueOf(r.getLicense()))
              .field("pardotId", String.valueOf(r.getPardotId()))
              .field("clusterId", String.valueOf(r.getClusterId()))
              .field("lat", toDouble(r.getLat()))
              .field("long", toDouble(r.getLong$()))
              .field("HDMemoryInGB", toShort(r.getHDMemoryInGB()))
              .field("clusterUpTime", r.getClusterUpTime() == null ? 0 : r.getClusterUpTime())
              .field("nodeUpTime", r.getNodeUptime() == null ? 0 : r.getNodeUptime())
              .field("oem", r.getOem() == null ? false : r.getOem())
              .field("pingTime", r.getPingTime())
              .$();
    }

    public static char firstChar(CharSequence clusterSize) {
        return clusterSize == null || clusterSize.length() == 0 ? '?' : clusterSize.charAt(0);
    }

    public static short toShort(CharSequence str) {
        try {
            return str == null ? 0 : Short.parseShort(str.toString());
        } catch (NumberFormatException f) {
            return 0;
        }
    }

    public static double toDouble(CharSequence str) {
        try {
            return str == null ? 0.0d : Double.parseDouble(str.toString());
        } catch (NumberFormatException f) {
            return 0;
        }
    }


}
