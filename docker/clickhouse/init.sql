CREATE DATABASE IF NOT EXISTS default;

-- 1. Raw deduplicated aircraft state telemetry
CREATE TABLE IF NOT EXISTS default.flight_states (
    icao24 String,
    callsign Nullable(String),
    origin_country String,
    time_position Nullable(Int64),
    last_contact Int64,
    longitude Float64,
    latitude Float64,
    baro_altitude Nullable(Float32),
    on_ground UInt8,
    velocity Nullable(Float32),
    true_track Nullable(Float32),
    vertical_rate Nullable(Float32),
    geo_altitude Nullable(Float32),
    event_time DateTime
) ENGINE = ReplacingMergeTree
ORDER BY (icao24, event_time);

-- 2. 5-minute tumbling window aggregated analytics
CREATE TABLE IF NOT EXISTS default.flight_analytics_5min (
    window_start DateTime,
    window_end DateTime,
    active_aircraft UInt32,
    avg_velocity Float32,
    avg_baro_altitude Float32,
    min_velocity Float32,
    max_velocity Float32,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (window_start);

-- 3. Latest state per aircraft view
CREATE VIEW IF NOT EXISTS default.flight_latest AS
SELECT
    icao24,
    argMax(callsign, event_time) AS callsign,
    argMax(origin_country, event_time) AS origin_country,
    argMax(longitude, event_time) AS longitude,
    argMax(latitude, event_time) AS latitude,
    argMax(baro_altitude, event_time) AS baro_altitude,
    argMax(on_ground, event_time) AS on_ground,
    argMax(velocity, event_time) AS velocity,
    argMax(true_track, event_time) AS true_track,
    argMax(vertical_rate, event_time) AS vertical_rate,
    argMax(geo_altitude, event_time) AS geo_altitude,
    max(event_time) AS last_seen
FROM default.flight_states
GROUP BY icao24;
