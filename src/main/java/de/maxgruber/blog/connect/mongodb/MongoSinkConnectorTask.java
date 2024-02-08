package de.maxgruber.blog.connect.mongodb;

import com.mongodb.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import org.bson.types.ObjectId;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class MongoSinkConnectorTask extends SinkTask {

    private MongoClient mongoClient = null;
    private MongoCollection<Document> mongoCollection = null;
    private MongoDatabase mongoDatabase = null;

    public MongoSinkConnectorTask() {}

    @Override
    public String version() {
        return new MongoSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.mongoClient = MongoClients.create("mongodb://localhost:50735");
        this.mongoDatabase = mongoClient.getDatabase("connect");
        mongoDatabase.createCollection("connect");
        this.mongoCollection = mongoDatabase.getCollection("connect");

        mongoDatabase.listCollectionNames().forEach(c -> log.info("collection: {}", c));
        log.info("collection has {} docs registered", mongoCollection.countDocuments());
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            mongoCollection.insertOne(new Document()
                .append("_id", new ObjectId())
                .append("body", record.value())
            );
            log.info("putting record with value {}", record.value());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {}

}