"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis pub/sub (Phase 1) et dans
Kafka (Phase 2, après décommentage).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
    python -m src.p2p_simulator.simulator --mode late_events

TODO Phase 1 :  Compléter _generate_listening_event() et _publish_to_redis()
TODO Phase 2 :  Activer _publish_to_kafka() et le mode fraude
"""

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import redis

# Phase 2 — décommenter quand Kafka est prêt
# from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379/1"
KAFKA_BOOTSTRAP = "kafka-1:9092"

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]

SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120000, 300000)}
    for i in range(50)
]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]
SAMPLE_PEERS = [str(uuid.uuid4()) for _ in range(20)]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:

    def __init__(self, n_peers=10, events_per_second=5.0, mode="normal"):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0

        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def run(self):
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur : {e}")
                time.sleep(1)

    # ─────────────────────────────────────────────
    # LISTENING EVENT
    # ─────────────────────────────────────────────

    def _generate_listening_event(self) -> dict:
        track = random.choice(SAMPLE_TRACKS)

        duration_ms = random.randint(30000, track["duration_ms"])

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(SAMPLE_USERS),
            "track_id": track["id"],
            "source_peer": random.choice(self.active_peers),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "duration_ms": duration_ms,
            "device_type": random.choice(DEVICE_TYPES),
            "geo_country": random.choice(GEO_COUNTRIES),
            "completed": duration_ms > 30000,
            "event_source": random.choice(EVENT_SOURCES),
        }

        return event

    # ─────────────────────────────────────────────
    # P2P EVENT
    # ─────────────────────────────────────────────

    def _generate_p2p_network_event(self) -> dict:
        event_type = random.choice([
            "peer_connect",
            "peer_disconnect",
            "chunk_transfer",
            "cache_hit",
            "cache_miss"
        ])

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id": random.choice(self.active_peers),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        if event_type == "chunk_transfer":
            event["chunk_id"] = str(uuid.uuid4())
            event["from_peer"] = random.choice(self.active_peers)
            event["to_peer"] = random.choice(self.active_peers)

        if event_type in ["cache_hit", "cache_miss"]:
            event["track_id"] = random.choice(SAMPLE_TRACKS)["id"]

        return event

    # ─────────────────────────────────────────────
    # REDIS PUBLISH
    # ─────────────────────────────────────────────

    def _publish_to_redis(self, channel: str, payload: str):
        try:
            self.redis.publish(channel, payload)
        except Exception as e:
            logger.error(f"Redis error: {e}")

    def _publish_event(self, topic_key, event):
        payload = json.dumps(event)
        channel = TOPICS[topic_key]
        self._publish_to_redis(channel, payload)

    def _shutdown(self, signum, frame):
        self.running = False
        logger.info("Arrêt simulateur")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--peers", type=int, default=10)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--mode", type=str, default="normal")

    args = parser.parse_args()

    sim = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode
    )
    sim.run()


if __name__ == "__main__":
    main()