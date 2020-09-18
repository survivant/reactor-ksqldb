package io.confluent.developer.ksqldb.reactor;public class Queries {

  public static final String CREATE_ANOMALIES_TABLE = "CREATE TABLE possible_anomalies WITH ("
                                                      + "    kafka_topic = 'possible_anomalies',"
                                                      + "    VALUE_AVRO_SCHEMA_FULL_NAME = 'io.ksqldb.tutorial.PossibleAnomaly'"
                                                      + ")   AS"
                                                      + "    SELECT card_number AS `card_number_key`,"
                                                      + "           as_value(card_number) AS `card_number`,"
                                                      + "           latest_by_offset(email_address) AS `email_address`,"
                                                      + "           count(*) AS `n_attempts`,"
                                                      + "           sum(amount) AS `total_amount`,"
                                                      + "           collect_list(tx_id) AS `tx_ids`,"
                                                      + "           WINDOWSTART as `start_boundary`,"
                                                      + "           WINDOWEND as `end_boundary`"
                                                      + "    FROM transactions"
                                                      + "    WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)"
                                                      + "    GROUP BY card_number"
                                                      + "    HAVING count(*) >= 3"
                                                      + "    EMIT CHANGES;";
  public static final String SELECT_POSSIBLE_ANOMALIES = "SELECT * FROM possible_anomalies EMIT CHANGES;";
  static final String CREATE_TRANSACTIONS_STREAM = "CREATE STREAM transactions ("
                                                   + "     tx_id VARCHAR KEY,"
                                                   + "    email_address VARCHAR,"
                                                   + "     card_number VARCHAR,"
                                                   + "     timestamp VARCHAR,"
                                                   + "     amount DECIMAL(12, 2)"
                                                   + ") WITH ("
                                                   + "     kafka_topic = 'transactions',"
                                                   + "     partitions = 8,"
                                                   + "     value_format = 'avro',"
                                                   + "     timestamp = 'timestamp',"
                                                   + "     timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'"
                                                   + ");";
}
