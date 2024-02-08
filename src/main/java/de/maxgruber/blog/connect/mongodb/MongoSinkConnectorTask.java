package de.maxgruber.blog.connect.mongodb;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class MongoSinkConnectorTask extends SinkTask {

    public MongoSinkConnectorTask() {
    }

    @Override
    public String version() {
        return new MongoSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        //TODO creat mongo db connection
        //TODO check for shit
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            //TODO Mongo create
            log.info("putting record with value {}", record.value());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {}

}