package fr.yaon.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class JsonSerdes<T> implements Serde<T> {

    @Override
    public Serializer<T> serializer() {
        return new JsonPOJOSerializer<T>();
    }

    @Override
    public Deserializer<T> deserializer() {
        Deserializer<T> deserializer = new JsonPOJODeserializer<T>();

        Map<String, Object> serdeProps = new HashMap<>();

        //le truc le plus bizarre de ma vie mais c'est comme ça que ça marche selon les exemples de la doc Kafka
        //https://github.com/apache/kafka/blob/2.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java

        serdeProps.put("JsonPOJOClass", TemperatureData.class);
        deserializer.configure(serdeProps, false);

        return deserializer;
    }
}
