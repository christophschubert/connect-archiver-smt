package net.christophschubert.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class UnArchiver<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public R apply(R record) {
        if (record.keySchema() == null && record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }


    R applyWithSchema(R record) {
        final var schema = record.valueSchema();
        final var rawValue = record.value();
        if (!(rawValue instanceof Struct))
            throw new DataException("object needs to be a struct");
        final var value = (Struct) rawValue;


        return record.newRecord(
                value.getString(CommonConfig.TOPIC_FIELD),
                value.getInt32(CommonConfig.PARTITION_FIELD),
                schema.field(CommonConfig.KEY_FIELD).schema(),
                value.get(CommonConfig.KEY_FIELD),
                schema.field(CommonConfig.VALUE_FIELD).schema(),
                value.get(CommonConfig.VALUE_FIELD),
                value.getInt64(CommonConfig.TIMESTAMP_FIELD)
                );
    }

    @SuppressWarnings("unchecked")
    R applySchemaless(R record) {
        if (!(record.value() instanceof Map)) {
            final var msg = "record needs to be instance of Map";
            throw new DataException(msg);
        }
        final Map<String, Object> valueMap = (Map<String, Object>) record.value();
        return record.newRecord(
                (String) valueMap.get(CommonConfig.TOPIC_FIELD),
                (Integer) valueMap.get(CommonConfig.PARTITION_FIELD),
                null,
                valueMap.get(CommonConfig.KEY_FIELD),
                null,
                valueMap.get(CommonConfig.VALUE_FIELD),
                (Long) valueMap.get(CommonConfig.TIMESTAMP_FIELD)
        );
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
