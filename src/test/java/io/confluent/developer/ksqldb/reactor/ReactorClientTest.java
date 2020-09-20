package io.confluent.developer.ksqldb.reactor;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.testcontainers.KsqlDbServerContainer;
import io.confluent.testcontainers.SchemaRegistryContainer;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static reactor.core.publisher.Flux.fromIterable;

@Slf4j(topic = "ReactorClient for ksqlDb")
public class ReactorClientTest {

  private static final KafkaContainer kafka = new KafkaContainer("5.5.1").withNetwork(Network.newNetwork());
  private static final SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer("5.5.1");

  private static final KsqlDbServerContainer ksqlServer = new KsqlDbServerContainer("0.11.0");

  private static ReactorClient reactorClient;

  public static final String CREATE_STREAM_STATEMENT =
      "CREATE STREAM %s (shipmentId INT KEY, cheese VARCHAR, shipmentTimestamp VARCHAR) "
      + " WITH (kafka_topic='cheese_shipments', partitions=3, value_format='json');";
  public static final String SHIPMENTS_TOPIC_NAME = "cheese_shipments";

  @BeforeAll
  public static void setUpClass() {
    // for ksql command topic 
    kafka.addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    kafka.addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    kafka.addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
    kafka.start();

    schemaRegistry.withKafka(kafka).start();

    ksqlServer.withKafka(kafka)
        // uncomment to debug issues in ksqldb server
//        .withLogConsumer(new Slf4jLogConsumer(log))
        .start();

    ClientOptions options = ClientOptions
        .create()
        .setHost(ksqlServer.getHost())
        .setPort(ksqlServer.getMappedPort(8088));

    reactorClient = ReactorClient.from(Client.create(options));
  }

  @AfterEach
  public void DROP() {
    final ExecuteStatementResult result = reactorClient
        .executeStatement(String.format("DROP STREAM IF EXISTS %s;", SHIPMENTS_TOPIC_NAME))
        .block();
    log.debug("[ @AfterEach ] Stream {} is dropped. Result: {}", SHIPMENTS_TOPIC_NAME, result);
  }

  @Test
  public void shouldCreateStream() {
    final List<StreamInfo> blockingResponse =
        reactorClient
            .executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME))
            .then(reactorClient.listStreams())
            .block();

    if (blockingResponse != null) {
      fromIterable(blockingResponse)
          .take(1)
          .subscribe(info -> {
            assertThat(info.getTopic(), equalTo(SHIPMENTS_TOPIC_NAME));
            assertThat(info.getFormat(), equalTo("JSON"));
          });
    }
  }

  @Test
  public void shouldInsertInToStream() {
    reactorClient
        .executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME))
        .block();

    final InsertAck insertAck =
        reactorClient
            .streamInserts(SHIPMENTS_TOPIC_NAME, cheesyData())
            .take(4)
            .blockLast();
    if (insertAck != null) {
      // four inserts, sequence starts from 0
      assertThat(insertAck.seqNum(), equalTo(3L));
    }
  }

  @Test
  public void shouldStreamQueryResults() {
    // create cheesy stream
    reactorClient
        .executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME))
        .block();

    // inserting test data 
    final InsertAck insertAck =
        reactorClient
            .streamInserts(SHIPMENTS_TOPIC_NAME, cheesyData())
            .take(4)
            .blockLast();
    if (insertAck != null) {
      // four inserts, sequence starts from 0
      assertThat(insertAck.seqNum(), equalTo(3L));
    }

    // doing select 
    String pushQuery = "SELECT * FROM %s EMIT CHANGES;";
    final Map<String, Object> properties = of("auto.offset.reset", "earliest");

    reactorClient
        .streamQuery(String.format(pushQuery, SHIPMENTS_TOPIC_NAME), properties)
        .take(1)
        .subscribe(row -> {
          log.debug("Row {}:", row);
          final List<String> list = Arrays.asList("shipmentId", "cheese", "shipmentTimestamp");
          assertThat(row.columnNames(), equalTo(list));
          assertThat(row.getInteger("shipmentId"), equalTo(42));
        });
  }

  @Test
  public void shouldNotInsertBadData() {
    // insert into cheese_shipments (shipmentId, cheese, shipmentTimestamp) values ('bad shipment id', 'american', 'june 12th 2019');
    final KsqlObject row = new KsqlObject(of("shipmentId", "bad shipment id",
                                             "cheese", "american",
                                             "shipmentTimestamp", "june 12th 2019"));

    assertThatExceptionOfType(KsqlClientException.class).isThrownBy(() -> reactorClient
        .executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME))
        .then(reactorClient.insertInto(SHIPMENTS_TOPIC_NAME, row))
        .onErrorStop()
        .block());
  }

  protected Flux<KsqlObject> cheesyData() {

    // insert into cheese_shipments (shipmentId, cheese, shipmentTimestamp) values (42, 'provolone', 'june 5th 2019');
    // insert into cheese_shipments (shipmentId, cheese, shipmentTimestamp) values (45, 'cheddar', 'june 8th 2019');
    // insert into cheese_shipments (shipmentId, cheese, shipmentTimestamp) values (47, 'swiss', 'june 8th 2019');
    // insert into cheese_shipments (shipmentId, cheese, shipmentTimestamp) values (51, 'cooper', 'june 11th 2019');

    return Flux.just(new KsqlObject(of("shipmentId", 42,
                                       "cheese", "provolone",
                                       "shipmentTimestamp", "june 5th 2019")),
                     new KsqlObject(of("shipmentId", 45,
                                       "cheese", "cheddar",
                                       "shipmentTimestamp",
                                       "june 8th 2019")),
                     new KsqlObject(of("shipmentId", 47,
                                       "cheese", "swiss",
                                       "shipmentTimestamp", "june 8th 2019")),
                     new KsqlObject(of("shipmentId", 51,
                                       "cheese", "cooper",
                                       "shipmentTimestamp", "june 11th 2019")));
  }
}

