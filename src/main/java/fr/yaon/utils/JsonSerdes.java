package fr.yaon.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerdes<T> implements Serde<T> {

    @Override
    public Serializer<T> serializer() {
        return new JsonPOJOSerializer<T>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonPOJODeserializer<T>();
    }
}
