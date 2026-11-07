import pytest
from src.producer.config import calculate_credit_cost, compute_safe_interval
from src.producer.producer import is_within_bbox, transform_state_vector


def test_transform_state_vector_full():
    vector = [
        "8a0121",
        "GIA123  ",
        "Indonesia",
        1710000000,
        1710000005,
        106.65,
        -6.12,
        8500.0,
        False,
        210.5,
        95.0,
        1.2,
        None,
        8600.0,
        "1234",
        False,
        0,
    ]
    res = transform_state_vector(vector)
    assert res is not None
    assert res["icao24"] == "8a0121"
    assert res["callsign"] == "GIA123"  # stripped whitespace
    assert res["origin_country"] == "Indonesia"
    assert res["time_position"] == 1710000000
    assert res["last_contact"] == 1710000005
    assert res["longitude"] == 106.65
    assert res["latitude"] == -6.12
    assert res["baro_altitude"] == 8500.0
    assert res["on_ground"] is False
    assert res["velocity"] == 210.5
    assert res["true_track"] == 95.0
    assert res["vertical_rate"] == 1.2
    assert res["geo_altitude"] == 8600.0


def test_transform_state_vector_with_nulls():
    vector = [
        "8a0999",
        None,
        "Indonesia",
        None,
        1710000010,
        107.0,
        -6.5,
        None,
        True,
        None,
        None,
        None,
        None,
        None,
    ]
    res = transform_state_vector(vector)
    assert res is not None
    assert res["icao24"] == "8a0999"
    assert res["callsign"] is None
    assert res["time_position"] is None
    assert res["baro_altitude"] is None
    assert res["on_ground"] is True
    assert res["velocity"] is None
    assert res["geo_altitude"] is None


def test_transform_state_vector_invalid():
    assert transform_state_vector([]) is None
    assert transform_state_vector(["short"]) is None


def test_is_within_bbox():
    # Java & Bali bbox: lat -9.0..-5.5, lon 105.0..116.0
    lamin, lamax, lomin, lomax = -9.0, -5.5, 105.0, 116.0

    # Inside (Jakarta)
    assert is_within_bbox(-6.12, 106.65, lamin, lamax, lomin, lomax) is True

    # Inside (Bali)
    assert is_within_bbox(-8.74, 115.16, lamin, lamax, lomin, lomax) is True

    # Outside (Singapore)
    assert is_within_bbox(1.35, 103.81, lamin, lamax, lomin, lomax) is False

    # Edge cases
    assert is_within_bbox(-9.0, 105.0, lamin, lamax, lomin, lomax) is True
    assert is_within_bbox(-5.5, 116.0, lamin, lamax, lomin, lomax) is True

    # Null values
    assert is_within_bbox(None, 106.0, lamin, lamax, lomin, lomax) is False
    assert is_within_bbox(-6.0, None, lamin, lamax, lomin, lomax) is False
    assert is_within_bbox(None, None, lamin, lamax, lomin, lomax) is False


def test_calculate_credit_cost():
    # Area <= 25 sq deg -> 1 credit
    assert calculate_credit_cost(-6.0, -4.0, 100.0, 105.0) == 1  # 2 * 5 = 10 sq deg

    # Area <= 100 sq deg -> 2 credits (Java + Bali bbox: 3.5 * 11 = 38.5 sq deg)
    assert calculate_credit_cost(-9.0, -5.5, 105.0, 116.0) == 2

    # Area <= 400 sq deg -> 3 credits
    assert calculate_credit_cost(-10.0, 5.0, 100.0, 120.0) == 3  # 15 * 20 = 300 sq deg

    # Area > 400 sq deg -> 4 credits (Full Indonesia box: 17 * 46 = 782 sq deg)
    assert calculate_credit_cost(-11.0, 6.0, 95.0, 141.0) == 4


def test_compute_safe_interval_clamping():
    cost_per_call = 2
    budget = 4000  # OAuth daily budget
    # Min safe = 86400 * 2 / 4000 = 43.2 seconds

    # Desired 60s is safe and kept
    assert compute_safe_interval(cost_per_call, budget, 60.0) == 60.0

    # Desired 10s is too fast, should be clamped to 43.2s
    clamped = compute_safe_interval(cost_per_call, budget, 10.0)
    assert clamped == pytest.approx(43.2, 0.01)
