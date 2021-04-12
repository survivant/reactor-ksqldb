package io.confluent.developer.ksqldb.reactor;


import static java.util.Arrays.asList;
import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.File;
import java.time.Instant;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

@Slf4j(topic = "ReactorClient for ksqlDb")
public class ReactorClientWithDockerComposeTest {

  @SuppressWarnings("rawtypes")
  private static final DockerComposeContainer CONTAINER = new DockerComposeContainer(new File("docker-compose.yml"));

  private static ReactorClient reactorClient;

  public static final String CREATE_STREAM_STATEMENT =
      "CREATE STREAM %s (shipmentId INT KEY, cheese VARCHAR, shipmentTimestamp VARCHAR) "
      + " WITH (kafka_topic='cheese_shipments', partitions=3, value_format='json');";
  public static final String SHIPMENTS_TOPIC_NAME = "cheese_shipments";

  @BeforeAll
  public static void setUpClass() {
    // wait after ksqldb to be ready
    CONTAINER.waitingFor("ksqldb-server", Wait.forLogMessage(".*INFO Server up and running.*\\s", 1));
    CONTAINER.withTailChildContainers(true);
    CONTAINER.start();

    ClientOptions options = ClientOptions
        .create()
        .setHost("localhost")
        .setPort(8088);

    reactorClient = ReactorClient.from(Client.create(options));
  }

  @AfterEach
  public void DROP() {
    final ExecuteStatementResult result = reactorClient
        .executeStatement(String.format("DROP STREAM IF EXISTS %s;", SHIPMENTS_TOPIC_NAME))
        .block();
    log.debug("[ @AfterEach ] Stream {} is dropped. Result: {}", SHIPMENTS_TOPIC_NAME, result);
  }

  @AfterAll
  public static void cleanup(){
    CONTAINER.close();
  }

  @Test
  public void shouldCreateStream() {
    final List<StreamInfo> blockingResponse =
        reactorClient
            .executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME))
            .then(reactorClient.listStreams())
            .block();

    Assertions.assertThat(blockingResponse)
        .isNotNull()
        .hasSize(1)
        .first()
        .extracting(StreamInfo::getTopic, StreamInfo::getFormat)
        .isEqualTo(asList(SHIPMENTS_TOPIC_NAME, "JSON"));
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
    Assertions.assertThat(insertAck)
        .isNotNull()
        .extracting(InsertAck::seqNum)
        .isEqualTo(3L);
  }

  @Test
  public void shouldInsertAfterCreateStatement() {
    // Mono ~= Supplier<CompletableFuture<T>>

    final List<Row> list = reactorClient
        .insertInto(
            SHIPMENTS_TOPIC_NAME,
            new KsqlObject()
                .put("shipmentId", 42)
                .put("cheese", "smile")
                .put("shipmentTimestamp", Instant.now().toString())
        )
        .log()
        .retryWhen(
            Retry.from(f -> f.take(1)
                .delayUntil(signal -> reactorClient
                    .executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME)))))
        .log("beforeBlock")
        .then(reactorClient.executeQueryFromBeginning("SELECT * FROM cheese_shipments EMIT CHANGES LIMIT 1;"))
        .block();

    Assertions.assertThat(list)
        .isNotNull()
        .first()
        .extracting(it -> it.getInteger("SHIPMENTID"))
        .isEqualTo(42);
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
    Assertions.assertThat(insertAck)
        .isNotNull()
        .extracting(InsertAck::seqNum)
        // four inserts, sequence starts from 0
        .isEqualTo(3L);

    // doing select 
    String pushQuery = "SELECT * FROM %s EMIT CHANGES LIMIT 1;";

    var row = reactorClient
        .streamQueryFromBeginning(String.format(pushQuery, SHIPMENTS_TOPIC_NAME))
        .blockFirst();

    log.debug("Row {}:", row);
    final List<String> columns = asList("SHIPMENTID", "CHEESE", "SHIPMENTTIMESTAMP");

    Assertions.assertThat(row)
        .isNotNull()
        .extracting(Row::columnNames, it -> it.getInteger("SHIPMENTID"))
        .isEqualTo(asList(columns, 42));
  }

  @Test
  public void shouldNotInsertBadData() {
    // insert into cheese_shipments (shipmentId, cheese, shipmentTimestamp) values ('bad shipment id', 'american', 'june 12th 2019');
    final KsqlObject row = new KsqlObject(of("shipmentId", "bad shipment id",
                                             "cheese", "american",
                                             "shipmentTimestamp", "june 12th 2019"));

    assertThatExceptionOfType(KsqlClientException.class)
        .isThrownBy(() -> reactorClient.executeStatement(String.format(CREATE_STREAM_STATEMENT, SHIPMENTS_TOPIC_NAME))
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

