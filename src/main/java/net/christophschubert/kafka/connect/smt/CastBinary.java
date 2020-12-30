package net.christophschubert.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Cast either the whole key object or the whole value object from a byte-array to a given type.
 * @param <R>
 */
public class CastBinary<R extends ConnectRecord<R>> implements Transformation<R> {

    static final Logger logger = LoggerFactory.getLogger(CastBinary.class);

    public static final String IS_VALUE_CONFIG = "is.value";
    public static final String CAST_TO_CONFIG = "cast.to";
    public static final String CHARSET_CONFIG = "charset";
    public static final Charset CHARSET_DEFAULT = StandardCharsets.UTF_8;
    public static final String BYTE_ORDER_CONFIG = "byte.order";
    public static final ByteOrder BYTE_ORDER_DEFAULT = ByteOrder.BIG_ENDIAN;
    public static final String FAIL_ON_EXCESSIVE_DATA_CONFIG = "fail.on.extra.bytes";

    final static ConfigDef configDef = new ConfigDef()
            .define(IS_VALUE_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, "case key or value")
            .define(CAST_TO_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "type to cast the value to")
            .define(CHARSET_CONFIG, ConfigDef.Type.STRING, CHARSET_DEFAULT.name(), ConfigDef.Importance.MEDIUM, "Charset used when casting to String")
            .define(BYTE_ORDER_CONFIG, ConfigDef.Type.STRING, BYTE_ORDER_DEFAULT.toString(), ConfigDef.Importance.MEDIUM, "Byte order used when casting to multi-byte datatype")
            .define(FAIL_ON_EXCESSIVE_DATA_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "Fail when input byte array contains bytes than necessary form a value. If set to false, the first ``sizeof(output-type)`` bytes will be used.");

    private boolean isValue = false;
    private String castToType = "String";
    private Charset charSet = CHARSET_DEFAULT;
    private ByteOrder byteOrder = BYTE_ORDER_DEFAULT;
    private boolean failOnExcessiveData = false;
    private Schema castToSchema = Schema.STRING_SCHEMA;

    @Override
    public R apply(R record) {
        if (isValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), castToSchema, doCast(record.value()), record.timestamp(), record.headers());
        } else {
            return record.newRecord(record.topic(), record.kafkaPartition(), castToSchema, doCast(record.key()), castToSchema, record.value(), record.timestamp(), record.headers());
        }
    }



    Object doCast(Object v) {
        final byte[] b = (byte[])v;
        switch (castToType) {
            case "HexString":
                return castToHexString(b);
            case "String":
                return new String(b, charSet);
            case "Double":
                return castDouble(b);
            case "Float":
                return castFloat(b);
            case "Long":
                return castLong(b);
            case "Int":
                return castInt(b);
            case "Short":
                return castShort(b);
            case "Byte":
                return castByte(b);
            case "Boolean":
                return castByte(b) != 0;
            default:
                throw new RuntimeException("Unknown type");
        }
    }

    String castToHexString(byte[] bytes) {
        final var sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }

    Double castDouble(byte[] b) {
        checkWidth(b, Double.BYTES);
        return ByteBuffer.wrap(b).order(byteOrder).getDouble();
    }

    Float castFloat(byte[] b) {
        checkWidth(b, Float.BYTES);
        return ByteBuffer.wrap(b).order(byteOrder).getFloat();
    }

    Byte castByte(byte[] b) {
        checkWidth(b, Byte.BYTES);
        return b[0];
    }

    Short castShort(byte[] b) {
        checkWidth(b, Short.BYTES);
        return ByteBuffer.wrap(b).order(byteOrder).getShort();
    }

    Integer castInt(byte[] b) {
        checkWidth(b, Integer.BYTES);
        return ByteBuffer.wrap(b).order(byteOrder).getInt();
    }

    Long castLong(byte[] b) {
        checkWidth(b, Long.BYTES);
        return ByteBuffer.wrap(b).order(byteOrder).getLong();
    }

    void checkWidth(byte[] b, int dataSize) {
        if (b.length < dataSize) {
            final String msg = String.format("Array of length %d cannot be cast to type %s.", b.length, castToType);
            throw new DataException(msg);
        }
        if (failOnExcessiveData && b.length > dataSize) {
            final String msg = String.format("Array of length %d cannot be cast to type %s.", b.length, castToType);
            throw new DataException(msg);
        }
    }


    @Override
    public ConfigDef config() {
        return configDef;
    }

    @Override
    public void close() {

    }

    Schema typeToSchema(String typeName) {
        switch (typeName) {
            case "HexString": //fallthrough
            case "String": return Schema.STRING_SCHEMA;
            case "Boolean": return Schema.BOOLEAN_SCHEMA;
            case "Double": return Schema.FLOAT64_SCHEMA;
            case "Float": return Schema.FLOAT32_SCHEMA;
            case "Long": return Schema.INT64_SCHEMA;
            case "Int":  return Schema.INT32_SCHEMA;
            case "Short": return Schema.INT16_SCHEMA;
            case "Byte": //fallthrough
            default: return Schema.INT8_SCHEMA;
        }
    }



    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("CASTER configure called");
        logger.info(configs.toString());

        final var simpleConfig = new SimpleConfig(configDef, configs);
        isValue             = simpleConfig.getBoolean(IS_VALUE_CONFIG);
        castToType          = simpleConfig.getString(CAST_TO_CONFIG);
        charSet             = Charset.forName(simpleConfig.getString(CHARSET_CONFIG));
        byteOrder           = simpleConfig.getString(BYTE_ORDER_CONFIG).equals(ByteOrder.LITTLE_ENDIAN.toString()) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        failOnExcessiveData = simpleConfig.getBoolean(FAIL_ON_EXCESSIVE_DATA_CONFIG);
        castToSchema        = typeToSchema(castToType);
    }

}
