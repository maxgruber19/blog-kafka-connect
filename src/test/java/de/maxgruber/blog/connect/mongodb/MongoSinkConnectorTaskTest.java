package de.maxgruber.blog.connect.mongodb;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
class MongoSinkConnectorTaskTest {

    MongoSinkConnectorTask mongoSinkConnectorTask = new MongoSinkConnectorTask();

    @Test
    void version() {
        log.info("version: {}", mongoSinkConnectorTask.version());
    }

    @BeforeEach
    void init() {
        Map<String, String> props = new HashMap<>();
        props.put("connection", "mongodb://localhost:50735");
        props.put("db", "connect");
        props.put("collection", "connect");
        mongoSinkConnectorTask.start(props);
    }

    @Test
    void put() {
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord("topic", 0, null, null, null, "{}", 0));
        mongoSinkConnectorTask.put(records);
    }

    @Test
    void stop() {
        mongoSinkConnectorTask.stop();
    }
}