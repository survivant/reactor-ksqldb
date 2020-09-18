package io.confluent.developer.ksqldb.reactor;

import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class ReactorClient {

  private final Client ksqlDbClient;

  private ReactorClient(final Client ksqlDbClient) {
    this.ksqlDbClient = ksqlDbClient;
  }

  /**
   * Creates Reactor wrapper for ksqlDB Client
   */
  static ReactorClient from(Client ksqlDbClient) {
    return new ReactorClient(ksqlDbClient);
  }

  Mono<ExecuteStatementResult> executeStatement(String sql, Map<String, Object> properties) {
    final CompletableFuture<ExecuteStatementResult> future = ksqlDbClient.executeStatement(sql, properties);
    return Mono.fromFuture(future);
  }

  Mono<ExecuteStatementResult> executeStatement(String sql) {
    return this.executeStatement(sql, Collections.emptyMap());
  }

  Flux<InsertAck> streamInserts(String streamName, Publisher<KsqlObject> insertsPublisher) {
    final CompletableFuture<AcksPublisher> future = this.ksqlDbClient.streamInserts(streamName, insertsPublisher);
    return Mono.fromFuture(future).flatMapMany(acksPublisher -> acksPublisher);
  }

  Flux<Row> streamQuery(String sql, Map<String, Object> properties) {
    final CompletableFuture<StreamedQueryResult> future = this.ksqlDbClient.streamQuery(sql, properties);

    return Mono.fromFuture(future).flatMapMany(streamedQueryResult -> {
      log.info("Result column names: {}", streamedQueryResult.columnNames());
      return streamedQueryResult;
    });
  }

  Flux<Row> streamQuery(String sql) {
    return this.streamQuery(sql, Collections.emptyMap());
  }

  Mono<List<Row>> executeQuery(String sql) {
    final BatchedQueryResult queryResult = this.executeQuery(sql, Collections.emptyMap());
    return Mono.fromFuture(queryResult);
  }

  BatchedQueryResult executeQuery(String sql, Map<String, Object> properties) {
    return this.ksqlDbClient.executeQuery(sql, properties);
  }

  Mono<List<StreamInfo>> listStreams() {
    return Mono.fromFuture(this.ksqlDbClient.listStreams());
  }

  /**
   * Returns the list of ksqlDB tables from the ksqlDB server's metastore
   */
  Mono<List<TableInfo>> listTables() {
    return Mono.fromFuture(this.ksqlDbClient.listTables());
  }

  /**
   * Returns the list of Kafka topics available for use with ksqlDB.
   */
  Mono<List<TopicInfo>> listTopics() {
    return Mono.fromFuture(this.ksqlDbClient.listTopics());
  }

  /**
   * Returns the list of queries currently running on the ksqlDB server.
   */
  Mono<List<QueryInfo>> listQueries() {
    return Mono.fromFuture(this.ksqlDbClient.listQueries());
  }


}
