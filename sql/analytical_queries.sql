-- Query 1: Top Pickup Zones
SELECT pulocationid, COUNT(*) AS trip_count
FROM analytics.yellow_taxi_trips_2024
GROUP BY pulocationid
ORDER BY trip_count DESC
LIMIT 10;

-- Query 2: Monthly Trip Volume
SELECT DATE_TRUNC('month', tpep_pickup_datetime) AS month,
COUNT(*) AS trips
FROM analytics.yellow_taxi_trips_2024
GROUP BY month
ORDER BY month;

-- Query 3: Average Trip Distance
SELECT
DATE_TRUNC('month', tpep_pickup_datetime) AS month,
ROUND(AVG(trip_distance)::NUMERIC,2) AS avg_trip_distance
FROM analytics.yellow_taxi_trips_2024
GROUP BY month
ORDER BY month;

-- Query 4: Passenger Distribution
SELECT
passenger_count,
ROUND(AVG(fare_amount)::NUMERIC,2) AS avg_fare,
COUNT(*) AS trips
FROM analytics.yellow_taxi_trips_2024
GROUP BY passenger_count
ORDER BY passenger_count;

-- Query 5: Revenue by Pickup Zone
SELECT
pulocationid,
ROUND(SUM(total_amount)::NUMERIC,2) AS total_revenue
FROM analytics.yellow_taxi_trips_2024
GROUP BY pulocationid
ORDER BY total_revenue DESC
LIMIT 10;