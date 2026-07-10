import json
import time
from datetime import datetime, timedelta
from websocket import create_connection
from confluent_kafka import Producer

conf = {'bootstrap.servers':  "localhost:9092"}
kafka_producer = Producer(conf)
topic = "transactions"

coinbase_websocket_url = "wss://ws-feed.exchange.coinbase.com"
websocket = create_connection(coinbase_websocket_url)

# subscribing to match the channel's emitted finalized trade transaction
subscribe_msg = {
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"],
    "channels": ["matches"]
}

websocket.send(json.dumps(subscribe_msg))
print("Stream connected.")

try:
    while True:
        result = websocket.recv()
        data = json.loads(result)

        if data.get("type") == "match":
            amount = float(data.get("price", 0)) * float(data.get("size", 0))

            payload = {
                "transaction_id": f"TXN_{data.get('sequence')}",
                "card_number": data.get("product_id"),
                "timestamp": data.get("time")[:19].replace("T", " "),
                "amount": round(amount, 2),
                "merchant_id": f"EXCHANGE_{data.get('side').upper()}",
                "location": "GLOBAL_NET"
            }

            kafka_producer.produce(topic, value = json.dumps(payload).encode('utf-8'))
            kafka_producer.poll(0)

except KeyBoardInterrupt:
    print("Cancelling the stream connection.")
    ws.close()