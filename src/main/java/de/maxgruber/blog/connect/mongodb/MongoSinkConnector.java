package de.maxgruber.blog.connect.mongodb;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MongoSinkConnector extends SinkConnector {

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("connection", Type.STRING, null, Importance.HIGH, "how to connect to mongo")
            .define("db", Type.STRING, "connect", Importance.HIGH, "mongo db to be used")
            .define("collection", Type.STRING, "connect", Importance.HIGH, "mongo collection to be used");

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        log.info("connector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoSinkConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}