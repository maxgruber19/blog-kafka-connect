package de.maxgruber.blog.connect.mongodb;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

@Slf4j
class MongoSinkConnectorTest {

    MongoSinkConnector mongoSinkConnector = new MongoSinkConnector();

    @Test
    void version() {
        log.info("version: {}", mongoSinkConnector.version());
    }

    @Test
    void start() {
        Map<String, String> props = new HashMap<>();
        mongoSinkConnector.start(props);
    }

    @Test
    void taskClass() {
        log.info("taskClass: {}", mongoSinkConnector.taskClass());
    }

    @Test
    void taskConfigs() {
        mongoSinkConnector.taskConfigs(1);
    }

    @Test
    void stop() {
        mongoSinkConnector.stop();
    }

    @Test
    void config() {
        mongoSinkConnector.config();
    }
}