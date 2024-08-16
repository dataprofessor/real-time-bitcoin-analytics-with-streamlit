import logging
import psycopg2

from quixstreams import Application


KAFKA_BROKER_ADDRESS = "kafka-broker:9092"
INPUT_TOPIC_NAME = "trades-ohlc"

POSTGRESQL_HOST = "database"
POSTGRESQL_PORT = 5432
POSTGRESQL_DATABASE = "quix"
POSTGRESQL_USER = "user"
POSTGRESQL_PASSWORD = "password"


def main():
    app = Application(
        broker_address=KAFKA_BROKER_ADDRESS,
        consumer_group="trades-ohlc-sink",
        auto_offset_reset="latest",
    )

    input_topic = app.topic(INPUT_TOPIC_NAME)

    sdf = app.dataframe(input_topic)

    client = psycopg2.connect(
                        host=POSTGRESQL_HOST,
                        port=POSTGRESQL_PORT,
                        database=POSTGRESQL_DATABASE,
                        user=POSTGRESQL_USER,
                        password=POSTGRESQL_PASSWORD,
    )
    
    cursor = client.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlc (
            symbol VARCHAR,
            open DECIMAL,
            high DECIMAL,
            low DECIMAL,
            close DECIMAL,
            timestamp_ms BIGINT
        );
    """)
    client.commit()

    sdf = sdf.update(lambda val: insert_data(client, val))
    
    app.run(sdf)


def insert_data(client, msg: dict):
    cursor = client.cursor()
    cursor.execute("""
                   INSERT INTO ohlc (symbol, open, high, low, close, timestamp_ms) VALUES (%s, %s, %s, %s, %s, %s)
                   """,
                   (msg["symbol"], msg["open"], msg["high"], msg["low"], msg["close"], msg["timestamp_ms"])
    )
    client.commit()
    logging.debug(f"Inserted record: {msg}")


if __name__ == "__main__":
    try:
        logging.basicConfig(level="DEBUG")
        main()
    except KeyboardInterrupt:
        print("Exiting")
