package net.christophschubert.kafka.connect.smt;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class ArchiverTest {
    @Test
    public void stringValueWithSchemaGetsWrappedWith() {
        SinkRecord record = new SinkRecord("input.topic", 1, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "value", 100,  100_000L, TimestampType.CREATE_TIME, new ConnectHeaders());



        System.out.println(record);
        Archiver<SinkRecord> archiver = new Archiver<>();

        System.out.println(archiver.apply(record));

        UnArchiver<SinkRecord> unArchiver = new UnArchiver<>();


        Assertions.assertEquals(record, unArchiver.apply(archiver.apply(record)));
    }
}