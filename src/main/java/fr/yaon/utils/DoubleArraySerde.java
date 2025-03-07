package fr.yaon.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class DoubleArraySerde implements Serde<double[]> {
    @Override
    public Serializer<double[]> serializer() {
        return new Serializer<double[]>() {
            @Override
            public byte[] serialize(String topic, double[] data) {
                if (data == null)
                    return null;
                ByteBuffer buffer = ByteBuffer.allocate(16); // 2 doubles = 16 octets
                buffer.putDouble(data[0]);
                buffer.putDouble(data[1]);
                return buffer.array();
            }
        };
    }

    @Override
    public Deserializer<double[]> deserializer() {
        return new Deserializer<double[]>() {
            @Override
            public double[] deserialize(String topic, byte[] data) {
                if (data == null || data.length != 16)
                    return new double[]{0.0, 0.0};
                ByteBuffer buffer = ByteBuffer.wrap(data);
                double sum = buffer.getDouble();
                double count = buffer.getDouble();
                return new double[]{sum, count};
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}