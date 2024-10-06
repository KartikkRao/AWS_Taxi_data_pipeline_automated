SET search_path TO 'taxi-model';


--select count(*) from dim_pickup_details;
INSERT INTO dim_pickup_details (pickup_date, pickup_year, pickup_month, pickup_week, pickup_day, pickup_hour, pickup_minutes, pickup_seconds, pickup_locationid)
SELECT
    t.pickup_date,
    EXTRACT(Year FROM t.pickup_date),
    EXTRACT(MONTH FROM t.pickup_date),
    EXTRACT(WEEK FROM t.pickup_date),
    EXTRACT(DAY FROM t.pickup_date),
    t.pickup_hours,
    t.pickup_minutes,
    t.pickup_seconds,
    t.pulocationid
FROM taxi_trip_data AS t
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_pickup_details AS dp  
    WHERE
        dp.pickup_date = t.pickup_date AND
        dp.pickup_hour = t.pickup_hours AND
        dp.pickup_minutes = t.pickup_minutes AND
        dp.pickup_seconds = t.pickup_seconds AND
        dp.pickup_locationid = t.pulocationid

);


INSERT INTO dim_drop_details (drop_date, drop_year, drop_month, drop_week, drop_day, drop_hour, drop_minutes, drop_seconds, drop_locationid)
SELECT
    t.drop_date,
    EXTRACT(Year FROM t.drop_date),
    EXTRACT(MONTH FROM t.drop_date),
    EXTRACT(WEEK FROM t.drop_date),
    EXTRACT(DAY FROM t.drop_date),
    t.drop_hours,
    t.drop_minutes,
    t.drop_seconds,
    t.dolocationid
FROM taxi_trip_data AS t
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_drop_details AS dd  
    WHERE
        dd.drop_date = t.drop_date AND
        dd.drop_hour = t.drop_hours AND
        dd.drop_minutes = t.drop_minutes AND
        dd.drop_seconds = t.drop_seconds AND
        dd.drop_locationid = t.dolocationid

);


INSERT INTO dim_loaction(location_id, borough, zone, service_zone)
SELECT
    l.location_id,
    l.borough,
    l.zone,
    l.service_zone
FROM location AS l 
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_loaction AS dl
    WHERE dl.location_id = l.location_id
);

INSERT INTO dim_payment_type(payment_id,payment_type)
SELECT
    p.payment_id,
    p.payment_type
FROM payment_type AS p
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_payment_type AS dp
    WHERE dp.payment_id = p.payment_id
);

INSERT INTO dim_rate_code(rate_id,rate_type)
SELECT
    r.rate_id,
    r.rate_type
FROM rate_code AS r
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_rate_code AS dr
    WHERE dr.rate_id = r.rate_id
);

INSERT INTO dim_store_fwd(fwd_symbol,swd_type)
SELECT
    f.fwd_symbol,
    f.fwd_type
FROM store_fwd AS f
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_store_fwd AS df
    WHERE df.fwd_symbol = f.fwd_symbol
);

INSERT INTO dim_taxi_type(taxi_id,taxi_type)
SELECT
    t.taxi_id,
    t.taxi_type
FROM taxi_type AS t
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_taxi_type AS dt
    WHERE dt.taxi_id = t.taxi_id
);


INSERT INTO dim_trip_type(trip_id,trip_type)
SELECT
    t.trip_id,
    t.trip_type
FROM trip_type AS t
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_trip_type AS dt
    WHERE dt.trip_id = t.trip_id
);

INSERT INTO dim_vendor_details(vendor_id,vendor_name)
SELECT
    v.vendor_id,
    v.vendor_name
FROM vendor_details AS v
WHERE NOT EXISTS(
    SELECT 1
    FROM dim_vendor_details AS dv
    WHERE dv.vendor_id = v.vendor_id
);

INSERT INTO fact_taxi_data(vendor_key, rate_key, payment_key, trip_key, taxi_key, fwd_key, pickup_key, drop_key, passenger_count, 
                            trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, 
                            congestion_surcharge, total_amount )
SELECT
    dv.vendor_key,
    dr.rate_key,
    dpp.payment_key,
    dt.trip_key,
    dtt.taxi_key,
    df.fwd_key,
    dp.pickup_key,
    dd.drop_key,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.total_amount
FROM taxi_trip_data AS t 
INNER JOIN dim_vendor_details AS dv ON dv.vendor_id = t.vendor_id
INNER JOIN dim_rate_code AS dr ON dr.rate_id = t.ratecodeid
INNER JOIN dim_payment_type AS dpp ON dpp.payment_id = t.payment_type
INNER JOIN dim_trip_type AS dt ON dt.trip_id = t.trip_type 
INNER JOIN dim_taxi_type AS dtt on dtt.taxi_type = t.taxi_type
INNER JOIN dim_store_fwd AS df on df.fwd_symbol = t.store_and_fwd_flag
INNER JOIN dim_pickup_details AS dp on dp.pickup_date = t.pickup_date AND dp.pickup_hour = t.pickup_hours AND dp.pickup_minutes = t.pickup_minutes
                                       AND dp.pickup_seconds = t.pickup_seconds AND dp.pickup_locationid = t.pulocationid
INNER JOIN dim_drop_details AS dd on dd.drop_date = t.drop_date AND dd.drop_hour = t.drop_hours AND dd.drop_minutes = t.drop_minutes
                                       AND dd.drop_seconds = t.drop_seconds AND dd.drop_locationid = t.dolocationid
WHERE NOT EXISTS(
    SELECT 1
    FROM fact_taxi_data AS ft 
    WHERE

        ft.vendor_key = dv.vendor_key AND
        ft.rate_key = dr.rate_key AND
        ft.payment_key = dpp.payment_key AND
        ft.trip_key = dt.trip_key AND
        ft.taxi_key = dtt.taxi_key AND
        ft.fwd_key = df.fwd_key AND
        ft.pickup_key = dp.pickup_key AND
        ft.drop_key = dd.drop_key
);

--select count(*) from fact_taxi_data;
--SELECT COUNT(*) from dim_pickup_details;
--SELECT COUNT(*) from dim_drop_details;
--select count(*) from taxi_trip_data;
 truncate table taxi_trip_data;
