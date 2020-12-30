package net.christophschubert.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Collections;
import java.util.List;
import java.util.Map;


//possible improvements:
// cache schemas
public class Archiver<R extends ConnectRecord<R>> implements Transformation<R> {

    final ConfigDef configDef = new ConfigDef();

    @Override
    public R apply(R record) {
        if (record.keySchema() == null || record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }

    }

    private R applySchemaless(R record) {

        final var v = Map.of(
                CommonConfig.KEY_FIELD, record.key(),
                CommonConfig.VALUE_FIELD, record.value(),
                CommonConfig.TOPIC_FIELD, record.topic(),
                CommonConfig.PARTITION_FIELD, record.kafkaPartition(),
                CommonConfig.OFFSET_FIELD, getOffset(record),
                CommonConfig.TIMESTAMP_FIELD, record.timestamp(),
                CommonConfig.HEADERS_FIELD, record.headers() // todo: implement proper handling
        );
        return record.newRecord(record.topic(), record.kafkaPartition(), null, null, null, v, record.timestamp());
    }

    private R applyWithSchema(R record) {

        final Schema schema = SchemaBuilder.struct().
                name(record.valueSchema().name() + "-wrapped").
                field(CommonConfig.KEY_FIELD, record.keySchema()).
                field(CommonConfig.VALUE_FIELD, record.valueSchema()).
                field(CommonConfig.TOPIC_FIELD, Schema.STRING_SCHEMA).
                field(CommonConfig.PARTITION_FIELD, Schema.INT32_SCHEMA).
                field(CommonConfig.OFFSET_FIELD, Schema.INT64_SCHEMA).
                field(CommonConfig.TIMESTAMP_FIELD, Schema.INT64_SCHEMA).
                field(CommonConfig.HEADERS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)));

        final Struct v = new Struct(schema).
                put(CommonConfig.KEY_FIELD, record.key()).
                put(CommonConfig.VALUE_FIELD, record.value()).
                put(CommonConfig.TOPIC_FIELD, record.topic()).
                put(CommonConfig.PARTITION_FIELD, record.kafkaPartition()).
                put(CommonConfig.OFFSET_FIELD, getOffset(record)).
                put(CommonConfig.TIMESTAMP_FIELD, record.timestamp()).
                put(CommonConfig.HEADERS_FIELD, convertHeaders(record.headers())); // TODO: implement header handling

        return record.newRecord(record.topic(), record.kafkaPartition(), null, null, schema, v, record.timestamp());
    }

    Map<String, List<Object>> convertHeaders(Headers headers) {
        return Collections.emptyMap();
    }

    Long getOffset(R record) {
        long kafkaOffset = -1;
        if (record instanceof SinkRecord) {
            final SinkRecord sr = (SinkRecord) record;
            return sr.kafkaOffset();
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        return configDef;
    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
