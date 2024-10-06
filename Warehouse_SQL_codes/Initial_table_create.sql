SET search_path TO 'taxi-model';
CREATE TABLE taxi_trip_data (
    vendor_id INTEGER,
    passenger_count BIGINT,
    trip_distance DOUBLE PRECISION,
    pickup_date DATE,
    pickup_hours INTEGER,
    pickup_minutes INTEGER,
    pickup_seconds INTEGER,
    drop_date DATE,
    drop_hours INTEGER,
    drop_minutes INTEGER,
    drop_seconds INTEGER,
    RatecodeID BIGINT,
    store_and_fwd_flag VARCHAR(10),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    trip_type INTEGER,
    taxi_type VARCHAR(50)
);


CREATE TABLE vendor_details(
    vendor_id INTEGER,
    vendor_name VARCHAR(100)
);

CREATE TABLE store_fwd(
    fwd_symbol VARCHAR(5),
    fwd_type varchar(100)
);

CREATE TABLE rate_code(
    rate_id INTEGER,
    rate_type varchar(100)
);

CREATE TABLE payment_type(
    payment_id INTEGER,
    payment_type varchar(100)
);

CREATE TABLE trip_type(
    trip_id INTEGER,
    trip_type VARCHAR(50)
);

CREATE TABLE taxi_type(
    taxi_id INTEGER,
    taxi_type VARCHAR(50)
);

CREATE TABLE location(
    location_id INTEGER,
    borough VARCHAR(100),
    zone VARCHAR(100),
    service_zone VARCHAR(100)
);
