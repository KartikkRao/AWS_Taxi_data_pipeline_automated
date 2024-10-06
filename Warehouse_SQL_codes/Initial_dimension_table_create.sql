SET search_path TO 'taxi-model';

CREATE TABLE dim_pickup_details (
    pickup_key BIGINT IDENTITY(1, 1),
    pickup_date DATE,
    pickup_year INT,
    pickup_month INT,
    pickup_week INT,
    pickup_day INT,
    pickup_hour INT,
    pickup_minutes INT,
    pickup_seconds INT,
    pickup_locationid INT
);

CREATE TABLE dim_drop_details (
    drop_key BIGINT IDENTITY(1, 1),
    drop_date DATE,
    drop_year INT,
    drop_month INT,
    drop_week INT,
    drop_day INT,
    drop_hour INT,
    drop_minutes INT,
    drop_seconds INT,
    drop_locationid INT
);

CREATE TABLE dim_loaction(
    location_id INT,
    borough VARCHAR(100),
    zone VARCHAR(100),
    service_zone VARCHAR(100)
);

CREATE TABLE dim_store_fwd(
    fwd_key INT IDENTITY(1,1),
    fwd_symbol VARCHAR(10),
    swd_type VARCHAR(100)
);
    

CREATE TABLE dim_vendor_details(
    vendor_key INT IDENTITY(1,1),
    vendor_id INT,
    vendor_name VARCHAR(100)
);


CREATE TABLE dim_rate_code(
    rate_key INT IDENTITY(1,1),
    rate_id INT,
    rate_type VARCHAR(100)
);

CREATE TABLE dim_payment_type(
    payment_key INT IDENTITY(1,1),
    payement_id INT,
    payment_type VARCHAR(100)
);

CREATE TABLE dim_trip_type(
    trip_key INT IDENTITY(1,1),
    trip_id INT,
    trip_type VARCHAR(100)
);


CREATE TABLE dim_taxi_type(
    taxi_key INT IDENTITY(1,1),
    taxi_id INT,
    taxi_type VARCHAR(100)
);

CREATE TABLE fact_taxi_data(
    vendor_key INT,
    rate_key INT,
    payment_key INT,
    trip_key INT,
    taxi_key INT,
    fwd_key INT,
    pickup_key BIGINT,
    drop_key BIGINT,
    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
);
