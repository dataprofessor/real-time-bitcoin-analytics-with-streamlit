import logging
import os

from datetime import timedelta
from typing import Any, List, Optional, Tuple
from quixstreams import Application


KAFKA_BROKER_ADDRESS = os.environ["KAFKA_BROKER_ADDRESS"]
INPUT_TOPIC_NAME = os.environ["INPUT_TOPIC_NAME"]
OUTPUT_TOPIC_NAME = os.environ["OUTPUT_TOPIC_NAME"]
OHLC_WINDOW_DURATION_MS = eval(os.environ["OHLC_WINDOW_DURATION_MS"])
OHLC_WINDOW_GRACE_MS = eval(os.environ["OHLC_WINDOW_GRACE_MS"])


def main():
    app = Application(
        broker_address=KAFKA_BROKER_ADDRESS,
        consumer_group="trades-ohlc-aggregator",
        auto_offset_reset="latest",
    )
    
    input_topic = app.topic(
        name=INPUT_TOPIC_NAME,
        value_deserializer="json",
        timestamp_extractor=custom_ts_extractor,
    )
    
    output_topic = app.topic(OUTPUT_TOPIC_NAME, value_serializer="json")

    sdf = app.dataframe(topic=input_topic)

    sdf = sdf.tumbling_window(
        duration_ms=OHLC_WINDOW_DURATION_MS,
        grace_ms=OHLC_WINDOW_GRACE_MS,
    )

    sdf = sdf.reduce(
        reducer=update_ohlc_with_trade,
        initializer=init_ohlc,
    ).final()

    """
    Now has the following schema:
    {
        "start": 1722617460000,
        "end": 1722617520000,
        "value": {
            "symbol": "BTC/USD",
            "open": 63394.1,
            "high": 63394.1,
            "low": 63373.1,
            "close": 63373.1
        }
    }
    """    
    sdf["symbol"] = sdf["value"]["symbol"]
    sdf["open"] = sdf["value"]["open"]
    sdf["high"] = sdf["value"]["high"]
    sdf["low"] = sdf["value"]["low"]
    sdf["close"] = sdf["value"]["close"]
    sdf["timestamp_ms"] = sdf["end"]

    sdf = sdf[["symbol", "open", "high", "low", "close", "timestamp_ms"]]

    """
    Now has the following schema:
    {
        "symbol": "BTC/USD",
        "open": 63222.2,
        "high": 63222.2,
        "low": 63181.4,
        "close": 63209.1,
        "timestamp_ms":1722618720000
    }
    """
    sdf = sdf.update(logging.info)

    sdf = sdf.to_topic(output_topic)
    
    app.run(sdf)


def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: Any,
) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload
    instead of Kafka timestamp.
    
    See the Quix Streams documentation here
    https://quix.io/docs/quix-streams/windowing.html#extracting-timestamps-from-messages
    """
    return value["timestamp_ms"]


def update_ohlc_with_trade(ohlc: dict, trade: dict) -> dict:
    return {
        "symbol": trade["symbol"],
        "open": ohlc["open"],
        "high": max(ohlc["high"], trade["price"]),
        "low": min(ohlc["low"], trade["price"]),
        "close": trade["price"],
    }

def init_ohlc(value: dict) -> dict:
    return {
        "symbol": value["symbol"],
        "open": value["price"],
        "high": value["price"],
        "low": value["price"],
        "close": value["price"],
    }


if __name__ == "__main__":
    try:
        logging.basicConfig(level="DEBUG")
        main()
    except KeyboardInterrupt:
        print("Exiting")
