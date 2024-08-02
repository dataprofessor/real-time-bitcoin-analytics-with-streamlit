import logging
import json
import websocket

from datetime import datetime, timezone
from quixstreams import Application


KAFKA_BROKER_ADDRESS = "kafka-broker:9092"
OUTPUT_TOPIC_NAME = "trades-raw"
# Docs https://docs.kraken.com/websockets-v2
KRAKEN_API_URL = "wss://ws.kraken.com/v2"
KRAKEN_SYMBOL_PAIRS = ["BTC/USD"]


def main():
    kraken_client = websocket.create_connection(KRAKEN_API_URL)

    subscription_msg = {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": KRAKEN_SYMBOL_PAIRS,
            "snapshot": False,
        },
    }

    kraken_client.send(json.dumps(subscription_msg))

    # Discard the first message since it is a channel status response
    logging.info(f"Kraken status: {kraken_client.recv()}")
    # Discard the next message for each symbol pair since it's a subscription confirmation response
    for symbol_pair in KRAKEN_SYMBOL_PAIRS:
        logging.info(f"Kraken subscription: {kraken_client.recv()}")

    while True:
        raw_msg = kraken_client.recv()

        if "heartbeat" in raw_msg:
            logging.debug(f"Discarded heartbeat event: {raw_msg}")
        else:
            logging.debug(f"Received trade: {raw_msg}")
            msg = json.loads(raw_msg)

            """
            Example trade response:
            {
                "channel": "trade",
                "data": [
                    {
                        "ord_type": "market",
                        "price": 4136.4,
                        "qty": 0.23374249,
                        "side": "sell",
                        "symbol": "BTC/USD",
                        "timestamp": "2022-06-13T08:09:10.123456Z",
                        "trade_id": 0
                    },
                    {
                        "ord_type": "market",
                        "price": 4136.4,
                        "qty": 0.00060615,
                        "side": "sell",
                        "symbol": "BTC/USD",
                        "timestamp": "2022-06-13T08:09:20.123456Z",
                        "trade_id": 0
                    },
                    {
                        "ord_type": "market",
                        "price": 4136.4,
                        "qty": 0.00000136,
                        "side": "sell",
                        "symbol": "BTC/USD",
                        "timestamp": "2022-06-13T08:09:30.123456Z",
                        "trade_id": 0
                    }
                ],
                "type": "update"
            }
            """
            trades = []
            for trade in msg["data"]:
                trades.append(
                    {
                        "symbol": trade["symbol"],
                        "side": trade["side"],
                        "price": trade["price"],
                        "quantity": trade["qty"],
                        "timestamp_ms": timestamp_to_ms(trade["timestamp"]),
                    }
                )
            
            app = Application(
                broker_address=KAFKA_BROKER_ADDRESS,
                auto_create_topics=True
            )

            with app.get_producer() as producer:    
                for trade in trades:
                    trade_str = json.dumps(trade) 
                    logging.info(f"Producing trade: {trade_str}")
                    producer.produce(
                        topic=OUTPUT_TOPIC_NAME,
                        key=trade["symbol"],
                        value=trade_str,
                    )
                    logging.info(f"Produced")


def timestamp_to_ms(timestamp_str: str) -> int:
    """
    Transforms timestamps of the format '2024-01-10T09:30:01.12345Z'
    into a Unix-style timestamp in milliseconds.

    Args:
        timestamp (str): A timestamp expressed as a string.

    Returns:
        int: A timestamp expressed in milliseconds.
    """
    timestamp = datetime.fromisoformat(timestamp_str[:-1]).replace(tzinfo=timezone.utc)

    return int(timestamp.timestamp() * 1000)


if __name__ == "__main__":
    try:
        logging.basicConfig(level="DEBUG")
        main()
    except KeyboardInterrupt:
        print("Exiting")
