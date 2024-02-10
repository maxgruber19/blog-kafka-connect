package de.maxgruber.blog.connect.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class MongoSinkConnectorTask extends SinkTask {

    private Map<String, String> props = null;
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
        this.props = props;
        this.mongoClient = MongoClients.create(props.get("connection"));
        this.mongoDatabase = mongoClient.getDatabase(props.get("db"));
        mongoDatabase.createCollection(props.get("collection"));
        this.mongoCollection = mongoDatabase.getCollection(props.get("collection"));

        mongoDatabase.listCollectionNames().forEach(c -> log.info("collection: {}", c));
        log.info("collection has {} docs registered", mongoCollection.countDocuments());
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            String id = UUID.randomUUID().toString();
            Document doc = new Document().append("_id", id).append("body", record.value());
            mongoCollection.insertOne(doc);
            log.info("doc inserted and found {}", Objects.requireNonNull(mongoCollection.find(Filters.eq("_id", id)).first()).toJson());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {
        mongoClient.close();
    }

}