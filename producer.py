import websockets
from websockets.exceptions import ConnectionClosedError
from confluent_kafka import Producer
import asyncio
import json



URL = 'wss://ws.kraken.com/v2'

SUBSCRIPTION = {
    "method": "subscribe",
    "params": {
        "channel": "ohlc",
        "symbol": [
            "BTC/USD",
        ],
        "interval": 1
    }
}

producer = Producer({"bootstrap.servers":"localhost:9092"})


async def main():
    try:
        while True:
            try:
                async with websockets.connect(URL) as ws:
                    await ws.send(json.dumps(SUBSCRIPTION))
                    print("Connected")

                    last = None
                    while True:
                        msg = await ws.recv()
                        raw_data = json.loads(msg)

                        if raw_data.get("channel") == "ohlc":
                            for data in raw_data["data"]:
                                if last is not None and data["interval_begin"] != last["interval_begin"]:
                                    producer.produce("btc_ohlc", value=json.dumps(data))
                                last = data

            except ConnectionClosedError as e:
                print(e)
                asyncio.sleep(10)
    finally:
        print('hi')
        producer.flush()

            


if __name__ == "__main__":
    asyncio.run(main())