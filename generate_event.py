import asyncio
import json
import uuid
import time
import random
import signal
from typing import Dict, Any
from aiokafka import AIOKafkaProducer


# ======================
# Configuration
# ======================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "tracardi.events"

SOURCE_ID = "e922b9c7-5264-4e92-805c-3f48a1768853"
EVENT_RATE_PER_SEC = 2  # events / second


EVENT_TYPES = ["page-view", "purchase", "sign-up", "search", "add-to-cart"]
URLS = [
    "https://example.com",
    "https://example.com/products",
    "https://example.com/checkout",
    "https://example.com/login",
]
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)",
]


# ======================
# Event Generator
# ======================
class TracardiEventGenerator:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer
        self.running = True

    def _event_properties(self, event_type: str) -> Dict[str, Any]:
        if event_type == "page-view":
            return {"url": random.choice(URLS), "title": "Random Page"}
        if event_type == "purchase":
            return {
                "product_id": str(uuid.uuid4()),
                "price": round(random.uniform(10, 100), 2),
                "currency": "USD",
            }
        if event_type == "search":
            return {"query": random.choice(["shoes", "laptop", "phone"])}
        if event_type == "add-to-cart":
            return {"product_id": str(uuid.uuid4()), "quantity": random.randint(1, 3)}
        return {}

    def build_payload(self) -> Dict[str, Any]:
        event_type = random.choice(EVENT_TYPES)
        session_id = str(uuid.uuid4())

        return {
            "source": {"id": SOURCE_ID},
            "session": {"id": session_id},
            "events": [
                {
                    "type": event_type,
                    "properties": self._event_properties(event_type),
                    "options": {},
                }
            ],
            "metadata": {
                "time": {
                    "insert": int(time.time() * 1000)
                }
            },
            "context": {
                "browser": {
                    "local": {
                        "browser": {
                            "userAgent": random.choice(USER_AGENTS)
                        }
                    }
                }
            },
            "options": {},
        }, session_id

    async def run(self):
        print("🚀 Kafka event generator started")
        try:
            while self.running:
                payload, session_id = self.build_payload()

                await self.producer.send_and_wait(
                    topic=KAFKA_TOPIC,
                    key=session_id.encode(),
                    value=payload,
                )

                print(f"✔ Sent event: {payload['events'][0]['type']}")

                await asyncio.sleep(1 / EVENT_RATE_PER_SEC)
        except asyncio.CancelledError:
            pass
        finally:
            print("🛑 Event generator stopped")

    def stop(self):
        self.running = False


# ======================
# Main
# ======================
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        linger_ms=10,
    )

    await producer.start()

    generator = TracardiEventGenerator(producer)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, generator.stop)

    try:
        await generator.run()
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
