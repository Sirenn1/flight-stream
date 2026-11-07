import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

from src.producer.config import OPENSKY_API_URL, OPENSKY_TOKEN_URL, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def transform_state_vector(vector):
    # OpenSky state vector format:
    # 0: icao24, 1: callsign, 2: origin_country, 3: time_position, 4: last_contact,
    # 5: longitude, 6: latitude, 7: baro_altitude, 8: on_ground, 9: velocity,
    # 10: true_track, 11: vertical_rate, 12: sensors, 13: geo_altitude, 14: squawk, 15: spi, 16: position_source
    if not vector or len(vector) < 7:
        return None

    icao24 = vector[0]
    callsign = vector[1].strip() if vector[1] else None
    origin_country = vector[2]
    time_position = vector[3]
    last_contact = vector[4]
    longitude = vector[5]
    latitude = vector[6]
    baro_altitude = vector[7] if len(vector) > 7 else None
    on_ground = bool(vector[8]) if len(vector) > 8 else False
    velocity = vector[9] if len(vector) > 9 else None
    true_track = vector[10] if len(vector) > 10 else None
    vertical_rate = vector[11] if len(vector) > 11 else None
    geo_altitude = vector[13] if len(vector) > 13 else None

    return {
        "icao24": icao24,
        "callsign": callsign,
        "origin_country": origin_country,
        "time_position": time_position,
        "last_contact": last_contact,
        "longitude": longitude,
        "latitude": latitude,
        "baro_altitude": baro_altitude,
        "on_ground": on_ground,
        "velocity": velocity,
        "true_track": true_track,
        "vertical_rate": vertical_rate,
        "geo_altitude": geo_altitude,
    }


def is_within_bbox(lat, lon, lamin, lamax, lomin, lomax):
    if lat is None or lon is None:
        return False
    return lamin <= lat <= lamax and lomin <= lon <= lomax


class TokenManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0.0

    def get_token(self):
        # Refresh 60 seconds before expiration
        if self.token and time.time() < (self.expires_at - 60):
            return self.token

        try:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            res = requests.post(OPENSKY_TOKEN_URL, data=payload, timeout=10)
            res.raise_for_status()
            data = res.json()
            self.token = data.get("access_token")
            expires_in = data.get("expires_in", 1800)
            self.expires_at = time.time() + expires_in
            logger.info("Acquired fresh OpenSky OAuth token")
            return self.token
        except Exception as e:
            logger.error(f"Failed to fetch OAuth token: {e}")
            return None


def fetch_states(config, token_manager=None):
    params = {
        "lamin": config["lamin"],
        "lamax": config["lamax"],
        "lomin": config["lomin"],
        "lomax": config["lomax"],
    }

    headers = {}
    if token_manager:
        token = token_manager.get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

    max_retries = 4
    base_backoff = 2.0

    for attempt in range(max_retries):
        try:
            res = requests.get(OPENSKY_API_URL, params=params, headers=headers, timeout=15)

            remaining = res.headers.get("X-Rate-Limit-Remaining")
            if remaining is not None:
                logger.info(f"OpenSky rate limit remaining credits: {remaining}")

            if res.status_code == 200:
                return res.json()

            if res.status_code == 429:
                retry_after = res.headers.get("X-Rate-Limit-Retry-After-Seconds")
                sleep_sec = float(retry_after) if retry_after else 60.0
                logger.warning(f"Rate limited (HTTP 429). Sleeping for {sleep_sec}s")
                time.sleep(sleep_sec)
                continue

            if 500 <= res.status_code < 600:
                logger.warning(f"Server error {res.status_code}. Retrying...")
            else:
                logger.error(f"Unexpected status code {res.status_code}: {res.text}")
                return None

        except requests.RequestException as e:
            logger.warning(f"Network error on attempt {attempt + 1}: {e}")

        # Exponential backoff with jitter on 5xx or network errors
        sleep_time = (base_backoff ** attempt) + random.uniform(0.5, 1.5)
        logger.info(f"Backing off for {sleep_time:.2f}s")
        time.sleep(sleep_time)

    logger.error("Exceeded maximum retries for OpenSky API call")
    return None


def get_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    # Retries built into kafka client
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_states(
    producer: KafkaProducer,
    topic: str,
    raw_payload: dict,
    config: dict,
) -> int:
    states = raw_payload.get("states") or []
    count = 0

    for state in states:
        item = transform_state_vector(state)
        if not item:
            continue

        if not is_within_bbox(
            item["latitude"],
            item["longitude"],
            config["lamin"],
            config["lamax"],
            config["lomin"],
            config["lomax"],
        ):
            continue

        producer.send(topic, key=item["icao24"], value=item)
        count += 1

    producer.flush()
    return count


def replay_samples(
    filepath: str,
    config: dict,
    producer: KafkaProducer,
    speed: float = 1.0,
):
    logger.info(f"Starting replay from {filepath} at {speed}x speed")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                published = publish_states(producer, config["kafka_topic"], data, config)
                logger.info(f"Replayed snapshot: published {published} aircraft states to {config['kafka_topic']}")
                
                # Sleep based on poll interval adjusted by speed factor
                sleep_interval = max(0.5, config["poll_interval"] / speed)
                time.sleep(sleep_interval)
    except FileNotFoundError:
        logger.error(f"Sample file not found: {filepath}")
        sys.exit(1)


def record_sample(filepath: str, raw_payload: dict):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(raw_payload) + "\n")
    logger.info(f"Recorded snapshot to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="OpenSky to Redpanda Flight Telemetry Producer")
    parser.add_argument("--record", type=str, help="Save API responses to the specified jsonl file")
    parser.add_argument("--replay", type=str, help="Replay flight data from the specified jsonl file")
    parser.add_argument("--speed", type=float, default=1.0, help="Replay speed multiplier (e.g. 2.0)")
    parser.add_argument("--once", action="store_true", help="Fetch/publish one batch and exit")
    args = parser.parse_args()

    config = load_config()
    logger.info(
        f"Producer started with bbox lat ({config['lamin']}..{config['lamax']}), "
        f"lon ({config['lomin']}..{config['lomax']}), cost={config['cost_per_call']} credits, "
        f"interval={config['poll_interval']:.1f}s, OAuth={config['has_oauth']}"
    )

    producer = get_kafka_producer(config["kafka_bootstrap_servers"])

    if args.replay:
        replay_samples(args.replay, config, producer, speed=args.speed)
        return

    token_manager = (
        TokenManager(config["client_id"], config["client_secret"])
        if config["has_oauth"]
        else None
    )

    while True:
        data = fetch_states(config, token_manager)
        if data:
            if args.record:
                record_sample(args.record, data)

            published = publish_states(producer, config["kafka_topic"], data, config)
            logger.info(f"Published {published} states to {config['kafka_topic']}")
        else:
            logger.warning("No data retrieved in this poll cycle")

        if args.once:
            break

        time.sleep(config["poll_interval"])


if __name__ == "__main__":
    main()
