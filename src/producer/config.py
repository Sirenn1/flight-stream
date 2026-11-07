import os
import logging

logger = logging.getLogger(__name__)

# Java and Bali only !!!
DEFAULT_LAMIN = -9.0
DEFAULT_LAMAX = -5.5
DEFAULT_LOMIN = 105.0
DEFAULT_LOMAX = 116.0

OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"


def calculate_credit_cost(lamin, lamax, lomin, lomax):
    area = (lamax - lamin) * (lomax - lomin)
    if area <= 25:
        return 1
    elif area <= 100:
        return 2
    elif area <= 400:
        return 3
    return 4


def compute_safe_interval(cost_per_call, daily_budget, desired_interval):
    # 86400 seconds per day
    min_safe_interval = (86400 * cost_per_call) / daily_budget
    if desired_interval < min_safe_interval:
        logger.warning(
            f"Desired interval {desired_interval}s is too fast for daily budget {daily_budget} "
            f"and cost {cost_per_call} credits/call. Clamping to safe minimum {min_safe_interval:.1f}s."
        )
        return min_safe_interval
    return desired_interval


def load_config():
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    has_oauth = bool(client_id and client_secret)

    daily_budget = 4000 if has_oauth else 400

    lamin = float(os.getenv("BBOX_LAMIN", DEFAULT_LAMIN))
    lamax = float(os.getenv("BBOX_LAMAX", DEFAULT_LAMAX))
    lomin = float(os.getenv("BBOX_LOMIN", DEFAULT_LOMIN))
    lomax = float(os.getenv("BBOX_LOMAX", DEFAULT_LOMAX))

    cost_per_call = calculate_credit_cost(lamin, lamax, lomin, lomax)

    # Default interval: 60s for OAuth, otherwise safe interval for anonymous budget
    default_interval = 60.0 if has_oauth else (86400 * cost_per_call) / daily_budget
    desired_interval = float(os.getenv("POLL_INTERVAL_SECONDS", default_interval))

    interval = compute_safe_interval(cost_per_call, daily_budget, desired_interval)

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "flight-raw-stream")

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "has_oauth": has_oauth,
        "daily_budget": daily_budget,
        "lamin": lamin,
        "lamax": lamax,
        "lomin": lomin,
        "lomax": lomax,
        "cost_per_call": cost_per_call,
        "poll_interval": interval,
        "kafka_bootstrap_servers": kafka_bootstrap_servers,
        "kafka_topic": kafka_topic,
    }
