import logging
import os
import psycopg2
import psycopg2.sql as sql

from quixstreams import Application


KAFKA_BROKER_ADDRESS = os.environ["KAFKA_BROKER_ADDRESS"]
INPUT_TOPIC_NAME = os.environ["INPUT_TOPIC_NAME"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = eval(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DATABASE = os.environ["POSTGRESQL_DATABASE"]
POSTGRESQL_USER = os.environ["POSTGRESQL_USER"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_TABLE = os.environ["POSTGRESQL_TABLE"]


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
    cursor.execute(
        sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol VARCHAR,
                    open DECIMAL,
                    high DECIMAL,
                    low DECIMAL,
                    close DECIMAL,
                    timestamp_ms BIGINT)
        """).format(table=sql.Identifier(POSTGRESQL_TABLE))
    )
    client.commit()

    sdf = sdf.update(lambda val: insert_data(client, val))
    
    app.run(sdf)


def insert_data(client, msg: dict):
    cursor = client.cursor()
    cursor.execute(
        sql.SQL("""
                INSERT INTO {} (symbol, open, high, low, close, timestamp_ms) VALUES (%s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(POSTGRESQL_TABLE)),
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
