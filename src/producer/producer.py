import argparse
import json
import logging
import random
import sys
import time
import requests
from kafka import KafkaProducer

from src.producer.config import OPENSKY_API_URL, OPENSKY_TOKEN_URL, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def transform_state_vector(vector):
    if not vector or len(vector) < 7:
        return None

    return {
        "icao24": vector[0],
        "callsign": vector[1].strip() if vector[1] else None,
        "origin_country": vector[2],
        "time_position": vector[3],
        "last_contact": vector[4],
        "longitude": vector[5],
        "latitude": vector[6],
        "baro_altitude": vector[7] if len(vector) > 7 else None,
        "on_ground": bool(vector[8]) if len(vector) > 8 else False,
        "velocity": vector[9] if len(vector) > 9 else None,
        "true_track": vector[10] if len(vector) > 10 else None,
        "vertical_rate": vector[11] if len(vector) > 11 else None,
        "geo_altitude": vector[13] if len(vector) > 13 else None,
    }


def is_within_bbox(lat, lon, lamin, lamax, lomin, lomax):
    if lat is None or lon is None:
        return False
    return lamin <= lat <= lamax and lomin <= lon <= lomax


def get_token(client_id, client_secret):
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(OPENSKY_TOKEN_URL, data=payload, timeout=10)
    resp.raise_for_status()
    return resp.json().get("access_token")


def fetch_states(config, token=None):
    params = {
        "lamin": config["lamin"],
        "lamax": config["lamax"],
        "lomin": config["lomin"],
        "lomax": config["lomax"],
    }
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    for attempt in range(4):
        try:
            resp = requests.get(OPENSKY_API_URL, params=params, headers=headers, timeout=15)
            rem = resp.headers.get("X-Rate-Limit-Remaining")
            if rem:
                logger.info(f"remaining credits: {rem}")

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                wait = float(resp.headers.get("X-Rate-Limit-Retry-After-Seconds", 60))
                logger.warning(f"429, waiting {wait}s")
                time.sleep(wait)
                continue

            logger.warning(f"status {resp.status_code}, retrying")
        except requests.RequestException as e:
            logger.warning(f"network error: {e}")

        # backoff with jitter on error
        time.sleep((2 ** attempt) + random.uniform(0.5, 1.5))

    return None


def get_kafka_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_states(producer, topic, raw_payload, config):
    states = raw_payload.get("states") or []
    count = 0

    for s in states:
        item = transform_state_vector(s)
        if not item:
            continue
        if not is_within_bbox(
            item["latitude"], item["longitude"],
            config["lamin"], config["lamax"], config["lomin"], config["lomax"]
        ):
            continue

        producer.send(topic, key=item["icao24"], value=item)
        count += 1

    producer.flush()
    return count


def replay_samples(filepath, config, producer, delay=2.0):
    logger.info(f"replaying {filepath} with {delay}s delay")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                n = publish_states(producer, config["kafka_topic"], data, config)
                logger.info(f"replayed {n} states")
                time.sleep(delay)
    except FileNotFoundError:
        logger.error(f"file not found: {filepath}")
        sys.exit(1)


def record_sample(filepath, raw_payload):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(raw_payload) + "\n")
    logger.info(f"saved snapshot to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="opensky producer")
    parser.add_argument("--record", type=str, help="save jsonl to path")
    parser.add_argument("--replay", type=str, help="replay jsonl from path")
    parser.add_argument("--delay", type=float, default=2.0, help="replay delay seconds")
    parser.add_argument("--once", action="store_true", help="run once and exit")
    args = parser.parse_args()

    config = load_config()
    logger.info(f"started producer bbox ({config['lamin']}..{config['lamax']}), interval {config['poll_interval']:.1f}s")

    producer = get_kafka_producer(config["kafka_bootstrap_servers"])

    if args.replay:
        replay_samples(args.replay, config, producer, delay=args.delay)
        return

    token = None
    if config["has_oauth"]:
        try:
            token = get_token(config["client_id"], config["client_secret"])
        except Exception as e:
            logger.warning(f"failed to get oauth token: {e}")

    while True:
        data = fetch_states(config, token)
        if data:
            if args.record:
                record_sample(args.record, data)

            n = publish_states(producer, config["kafka_topic"], data, config)
            logger.info(f"published {n} states")
        else:
            logger.warning("no states received")

        if args.once:
            break

        time.sleep(config["poll_interval"])


if __name__ == "__main__":
    main()
