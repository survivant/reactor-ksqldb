
package io.confluent.developer.ksqldb.reactor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.KsqlObject;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import static io.confluent.developer.ksqldb.reactor.Queries.CREATE_ANOMALIES_TABLE;
import static io.confluent.developer.ksqldb.reactor.Queries.CREATE_TRANSACTIONS_STREAM;
import static io.confluent.developer.ksqldb.reactor.Queries.SELECT_POSSIBLE_ANOMALIES;

@Slf4j
// an example taken from https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/#event-driven-microservice
// https://docs.ksqldb.io/en/latest/tutorials/event-driven-microservice/
public class EventDrivenMicroservice {

  private static final Map<String, Object> properties = Map.of("auto.offset.reset", "earliest");

  public static void main(String[] args) {

    final ReactorClient reactorClient = ReactorClient.from(Client.create(localCpKsqlDbClientOptions()));

    // Create the rows to insert
    final Flux<KsqlObject> insertData = getInsertData();

    log.info("CLEANUP PREVIOUS DATA");

    // cleanup
    reactorClient
            .executeStatement("DROP TABLE IF EXISTS possible_anomalies;")
            .block();

    reactorClient
        .executeStatement("DROP STREAM IF EXISTS transactions;")
        .block();

    log.info("CLEANUP DONE");

    log.info("CREATE STREAM");
    // create stream
    reactorClient
        .executeStatement(CREATE_TRANSACTIONS_STREAM, properties)
        .then(reactorClient.executeStatement(CREATE_ANOMALIES_TABLE, properties))
        .subscribe();

    log.info("INSERT TRANSACTION DATA");

    reactorClient
        .streamInserts("TRANSACTIONS", insertData)
        .subscribe(result -> log.info(result.toString()),
                   error -> log.error("Can't create stream", error));

    log.info("KSQL Select");
    // push query example
    reactorClient
        .streamQuery(SELECT_POSSIBLE_ANOMALIES, properties)
        .buffer(10)
        .subscribe(row -> log.info("row= {}", row.size()),
                   error -> log.error("Push query request failed: " + error));

    log.info("DONE");

  }

  private static Flux<KsqlObject> getInsertData() {
    var insertRows = new ArrayList<KsqlObject>();
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "michael@example.com")
                       .put("CARD_NUMBER", "358579699410099")
                       .put("TX_ID", "f88c5ebb-699c-4a7b-b544-45b30681cc39")
                       .put("TIMESTAMP", "2020-04-22T03:19:58")
                       .put("AMOUNT", new BigDecimal("50.25")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "derek@example.com")
                       .put("CARD_NUMBER", "352642227248344")
                       .put("TX_ID", "0cf100ca-993c-427f-9ea5-e892ef350363")
                       .put("TIMESTAMP", "2020-04-25T12:50:30")
                       .put("AMOUNT", new BigDecimal("18.97")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "colin@example.com")
                       .put("CARD_NUMBER", "373913272311617")
                       .put("TX_ID", "de9831c0-7cf1-4ebf-881d-0415edec0d6b")
                       .put("TIMESTAMP", "2020-04-19T09:45:15")
                       .put("AMOUNT", new BigDecimal("12.50")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "michael@example.com")
                       .put("CARD_NUMBER", "358579699410099")
                       .put("TX_ID", "044530c0-b15d-4648-8f05-940acc321eb7")
                       .put("TIMESTAMP", "2020-04-22T03:19:54")
                       .put("AMOUNT", new BigDecimal("103.43")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "derek@example.com")
                       .put("CARD_NUMBER", "352642227248344")
                       .put("TX_ID", "5d916e65-1af3-4142-9fd3-302dd55c512f")
                       .put("TIMESTAMP", "2020-04-25T12:50:25")
                       .put("AMOUNT", new BigDecimal("3200.80")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "derek@example.com")
                       .put("CARD_NUMBER", "352642227248344")
                       .put("TX_ID", "d7d47fdb-75e9-46c0-93f6-d42ff1432eea")
                       .put("TIMESTAMP", "2020-04-25T12:51:55")
                       .put("AMOUNT", new BigDecimal("154.32")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "michael@example.com")
                       .put("CARD_NUMBER", "358579699410099")
                       .put("TX_ID", "c5719d20-8d4a-47d4-8cd3-52ed784c89dc")
                       .put("TIMESTAMP", "2020-04-22T03:19:32")
                       .put("AMOUNT", new BigDecimal("78.73")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "colin@example.com")
                       .put("CARD_NUMBER", "373913272311617")
                       .put("TX_ID", "2360d53e-3fad-4e9a-b306-b166b7ca4f64")
                       .put("TIMESTAMP", "2020-04-19T09:45:35")
                       .put("AMOUNT", new BigDecimal("234.65")));
    insertRows.add(new KsqlObject()
                       .put("EMAIL_ADDRESS", "colin@example.com")
                       .put("CARD_NUMBER", "373913272311617")
                       .put("TX_ID", "de9831c0-7cf1-4ebf-881d-0415edec0d6b")
                       .put("TIMESTAMP", "2020-04-19T09:44:03")
                       .put("AMOUNT", new BigDecimal("150.00")));
    return Flux.fromIterable(insertRows);
  }

  private static ClientOptions localCpKsqlDbClientOptions() {
    return ClientOptions.create()
        .setHost("localhost")
        .setPort(8088);
  }
}
