"""Ingests live trade events from the Coinbase public exchange feed and
publishes them onto the Kafka "transactions" topic as simulated financial
transactions.

Each trade is assigned a synthetic per-transaction card/account id
(`card_number`), independent of the asset traded (`asset`), so that
downstream velocity detection is measuring a real per-entity signal rather
than just market volume on popular pairs - see README.md "Simulated
Fraud-Detection Methodology".
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from typing import Any, Optional

import websockets
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
KAFKA_TOPIC = "transactions"
DEFAULT_CARD_POOL_SIZE = 200
FLUSH_TIMEOUT_SECONDS = 10

SUBSCRIBE_MESSAGE = {
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"],
    "channels": ["matches"],
}

REQUIRED_MATCH_FIELDS = ("sequence", "price", "size", "product_id", "side", "time")


def assign_card_number(seed: str, pool_size: int = DEFAULT_CARD_POOL_SIZE) -> str:
    """Deterministically map a seed (the transaction id) to one of a fixed
    pool of synthetic card numbers, independent of which asset was traded.
    """
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    bucket = int(digest, 16) % pool_size
    return f"CARD_{bucket:04d}"


def build_transaction_payload(
    match_event: dict[str, Any], card_pool_size: int = DEFAULT_CARD_POOL_SIZE
) -> Optional[dict[str, Any]]:
    """Map a raw Coinbase "match" event to the transaction schema, or return
    None (logging why) if the event should be skipped - either because it
    isn't a finalized trade match, or because a required field is missing
    or unparseable. Never silently coerces missing/invalid data to a
    fabricated default.
    """
    if match_event.get("type") != "match":
        return None

    missing = [field for field in REQUIRED_MATCH_FIELDS if match_event.get(field) is None]
    if missing:
        logger.warning("Skipping match event with missing fields %s: %s", missing, match_event)
        return None

    try:
        price = float(match_event["price"])
        size = float(match_event["size"])
    except (TypeError, ValueError):
        logger.warning("Skipping match event with non-numeric price/size: %s", match_event)
        return None

    transaction_id = f"TXN_{match_event['sequence']}"

    return {
        "transaction_id": transaction_id,
        "card_number": assign_card_number(transaction_id, card_pool_size),
        "asset": match_event["product_id"],
        "timestamp": str(match_event["time"])[:19].replace("T", " "),
        "amount": round(price * size, 2),
        "merchant_id": f"EXCHANGE_{str(match_event['side']).upper()}",
        "location": "GLOBAL_NET",
    }


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Kafka delivery failed for key=%s: %s", msg.key(), err)


def _handle_raw_message(raw_message: str, producer: Producer, card_pool_size: int) -> None:
    try:
        match_event = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Skipping non-JSON message: %.200s", raw_message)
        return

    payload = build_transaction_payload(match_event, card_pool_size)
    if payload is None:
        return

    producer.produce(
        KAFKA_TOPIC,
        value=json.dumps(payload).encode("utf-8"),
        key=payload["card_number"].encode("utf-8"),
        callback=_delivery_report,
    )
    producer.poll(0)


async def stream_transactions(producer: Producer, card_pool_size: int) -> None:
    """Connect to the Coinbase feed and publish transactions to Kafka.

    Uses websockets' documented auto-reconnect idiom: `async for websocket
    in websockets.connect(...)` transparently reconnects (with backoff) on
    OSError/WebSocketException, so a dropped connection - e.g. poor
    connectivity - doesn't kill the whole producer. The subscribe message is
    re-sent at the top of every iteration since each reconnect yields a
    fresh connection.
    """
    async for websocket in websockets.connect(COINBASE_WS_URL):
        try:
            await websocket.send(json.dumps(SUBSCRIBE_MESSAGE))
            logger.info("Stream connected and subscribed to %s.", SUBSCRIBE_MESSAGE["product_ids"])

            async for raw_message in websocket:
                _handle_raw_message(raw_message, producer, card_pool_size)

        except websockets.ConnectionClosed as exc:
            logger.warning("WebSocket connection closed (%s); flushing and reconnecting...", exc)
            producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)
            continue


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    card_pool_size = int(os.environ.get("CARD_POOL_SIZE", str(DEFAULT_CARD_POOL_SIZE)))
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    try:
        asyncio.run(stream_transactions(producer, card_pool_size))
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    finally:
        logger.info("Flushing outstanding Kafka messages before shutdown...")
        remaining = producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)
        if remaining > 0:
            logger.warning("%d message(s) could not be flushed before shutdown.", remaining)


if __name__ == "__main__":
    main()
