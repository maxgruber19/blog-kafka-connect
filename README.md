# Building a custom Kafka Connector to stream records into MongoDB
## Introduction
Data Mesh is evolving fast and Apache Kafka is a great way to serve its data streaming requirements. Kafka Connect is a powerful weapon to transfer data from / to different systems in a simple way. There are lots of connectors developed by Confluent and other third parties that can communicate with databases, objectstores or even APIs.

Often the public implementations don't match special requirements a developer has to satisfy. This can be caused by various reasons like security issues, custom data models or the need to connect databases less widely used.

This post will guide you through the implementation, packaging, deployment and test of a simple sink connector because imho a lot of comprehensible documentation on that topic is still missing. We'll use MongoDB as the sink for our records. Keep in mind that there is already a very good connector for MongoDB which is ready to be downloaded.

---

## Prerequisites
All the code discussed here is publicly available @ github. Docker will be used to bootstrap a kafka stack on your local machine so that you have a working environment the code can be deployed to. Commands may be unix specific and may not work on windows hosts without translation.
```bash 
git clone https://github.com/maxgruber19/blog-kafka-connect.git
```

## Setting up a kafka cluster with docker-compose (cp-quickstart)
We'll use the cp-quickstart docker-compose installation published by Confluent because it provides all components we need. If you want to continue your kafka journey have a look at the other services the cp-quickstart package provides like ksql or kafka-rest-proxy.

```bash
wget https://raw.githubusercontent.com/confluentinc/cp-all-in-one/7.5.3-post/cp-all-in-one-kraft/docker-compose.yml
docker-compose up -d connect broker control-center
```
Verify that you have connect, broker and control-center up and running by visiting http://localhost:9021 and checking the containers running on your host.
```bash
docker ps | grep confluent
```
In order to store messages somewhere we need a new topic which can be created by using the kafka-topics command the broker is provided with.
```bash
docker exec -it broker /bin/bash -c 'kafka-topics --create  --bootstrap-server localhost:9092 --topic mongo-sink-connector-topic'
```
The topic should be visible in the control center now.

## Setting up a MongoDB instance with docker (mongo-docker)
Bootstrapping a MongoDB deployment is super simple and can be done by running a single docker image exposing its port on the host
```bash
docker run --name mongo -d mongo:latest --expose=27017
```
Congratulations, you're all set and ready to open your favorite IDE. We'll modify the cluster slightly when we're done packaging our connector.

---

## Connector development
We'll use maven and Java 17 to build our first simple SinkConnector implementation. Unfortunately there is no support for languages like python or c# when implementing kafka connectors.

We need two classes extending SinkConnector and SinkTask. The task contains the code when the connector is running and processing the messages. The connector won't get much attention from us because its only used for basic configs.

To be able to use the required classes we first need to import kafka-connect-api to our pom.xml.

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>connect-api</artifactId>
  <version>3.6.1</version>
</dependency>
```

The less exciting part of the development is the connector class itself. Only highlights will be discussed here. Find the full code @ github. We define external configs the connector will need like connection urls, passwords or collection names. These parameters will be exposed for configuration in the control center and in the connect REST API.

```java
static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define("connection", Type.STRING, null, Importance.HIGH, "how to connect to mongo")
    .define("db", Type.STRING, "connect", Importance.HIGH, "mongo db to be used")
    .define("collection", Type.STRING, "connect", Importance.HIGH, "mongo collection to be used");
```

That's almost everything you have to know about the connector class. Let's dive into the task class because that's where the magic happens.

As soon as you start your connector kafka tries to spawn as much tasks as specified in your configuration (default: 1). The tasks will be instantiated on random connect workers across the cluster. One of the first methods to be invoked is start(Map<String, String> props) which is a good point to store the task configs and create the clients our application requires. The task configs get passed by our connector class and contain the parsed properties we defined in its ConfigDef object. We save them in a private field to access it later. Additionally we'll set up our MongoDB connection in the start(Map<String, String> props) method because we don't want to reinitialize the client again and again with every message the consumer receives. Another benefit is that the connector crashes before starting to consume in case of database misconfiguration.

Converting consumed kafka records to database compatible rows / documents is what put(Collection<SinkRecord> sinkRecords) does. The method is invoked for each poll containing messages. In our simple case we translate the message to a bson document and send it to the collection we specified in our ConfigDef object.

Consider closing the client or other connections in the tasks method stop() because you're a good developer.

That's it for the development! Of course things get way more complex if you add custom logic or security requirements. But usually these aspects are different from usecase to usecase and system to system.

We're ready to package the code and deploy it to our running cluster.

### Packaging the connector

Confluent provides a maven plugin which is very useful to package your connector in a compatible format. The plugin will create a connector zip and directory in target/components/packages. We will pass these files to our connect container in the next section.

```xml
<build>
 <plugins>
 <plugin>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-connect-maven-plugin</artifactId>
  <version>0.12.0</version>
   <configuration>
   <componentTypes>sink</componentTypes>
   <name>CustomMongoConnector</name>
   <ownerUsername>Blog</ownerUsername>
   <description>some funny connector</description>
  </configuration>
  </plugin>
 </plugins>
</build>
```

### Installing the connector
The cp-quickstart package is shipped with some default connectors. We now need to make kafka know about our custom implementation.
The docker-compose.yaml curled in the prerequisites must be modified now. First we have to add a custom directory /plugins to the path where kafka looks for all its plugins.
```yaml
CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/plugins"
```
We can fill the /plugins directory by adding volumes to the docker-compose.yaml file. We'll mount our target/components/packages directory from our host machine into /plugins in our connect container. Make sure you copy the fully qualified path from you IDE.
```yaml
volumes:
  - /Users/maxgruber/IdeaProjects/blog-kafka-connect/target/components/packages:/plugins
```
Now we just need to restart the cluster and the connector will be ready for deployment.
```bash
docker-compose down && docker-compose up -d connect broker control-center
```
You can verify its readiness either by accessing the connect section in the control center or you can use the connect REST API to list all available connect plugins.
```bash
curl http://localhost:8083/connector-plugins | jq
```
```json
[
    {
        "class": "de.maxgruber.blog.connect.mongodb.MongoSinkConnector",
        "type": "sink",
        "version": "7.5.0-ce"
    },
    {
        "class": "io.confluent.kafka.connect.datagen.DatagenConnector",
        "type": "source",
        "version": "null"
    },
    {
        "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
        "type": "source",
        "version": "7.5.0-ce"
    },
    {
        "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
        "type": "source",
        "version": "7.5.0-ce"
    },
    {
        "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
        "type": "source",
        "version": "7.5.0-ce"
    }
]
```

### Deploying the connector
The connector can now be deployed either via control center or connect REST API. We'll use control center and upload the following config.json. You will have to change the port of MongoDB to the one exposed on your machine. You can find that out by asking docker for the mapped port.
```bash
docker port mongo | awk -F ':' '{print $NF}'
```

```json
{
    "name": "MongoSinkConnectorConnector",
    "config": {
        "name": "MongoSinkConnectorConnector",
        "connector.class": "de.maxgruber.blog.connect.mongodb.MongoSinkConnector",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "topics": "mongo-sink-connector-topic",
        "connection": "mongodb://host.docker.internal:50735",
        "db": "connect",
        "collection": "connect"
    }
}
```
You should now see the connector up and running.

### Testing the connector
We can access MongoDB by using the mongosh command the container is shipped with. Counting the documents in our collection should return 0 if you didn't already insert something by yourself.
```bash
docker exec -it mongo bash -c 'echo "db.connect.countDocuments();" | mongosh connect -f'
```

Now we just need to add messages to the topic we connected to our MongoDB deployment. We can do this by using the kafka-console-producer command the broker is shipped with. You can also use the control-center to produce messages to topics if you prefer using mouseclicks over shell commands.

```bash
docker exec -it broker /bin/bash -c 'echo "Hello Kafka!" | kafka-console-producer --bootstrap-server localhost:9092 --topic mongo-sink-connector-topic'
```

If you check the connect log, you'll see that the message has been sent to the specified MongoDB collection
```bash
docker logs connect | grep "Hello Kafka"
```
```
INFO doc inserted and found {"_id": "69c21f07-c141-4e26-9e6d-f363d84bd143", "body": "Hello Kafka!"}
```
Just to have the stream fully covered we'll check the collection manually to make sure we now have a document stored in it.
```bash
docker exec -it mongo bash -c 'echo "db.connect.find();" | mongosh connect -f'
```
The result will be something like the following
```json
[
  { "_id": "69c21f07-c141-4e26-9e6d-f363d84bd143", "body": "Hello Kafka!" }
]
```
That's it. You've successfully connected Kafka and MongoDB with your own SinkConnector implementation.

---

## Summary
Implementing a custom connector is quite simple as you have seen in this post. It's a suitable solution when generic connectors don't bring the functionality you expect. Unfortunately testing the connector on your local machine can not be fully covered because there is no runnable you can just start yourself. The most important aspects of your connector will be start() and put() which can be easily covered.

This article tried to help you learning the ropes of connector development and can be the basis for you to implement connectors for different systems and usecases. Feel free to raise your digital thumbs if you found what you were looking for.

Open source is good source. Feel free to contribute to the repository if you have additional ideas, bugs or dependency upgrades for example.