SET search_path TO 'taxi-model';

COPY taxi_trip_data
FROM 's3://taxi-data203/transformed_data/yellow_taxi/data_2024-09-27_20-16-41/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY taxi_trip_data
FROM 's3://taxi-data203/transformed_data/green_taxi/data_2024-09-27_20-16-41/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY location
FROM 's3://taxi-data203/transformed_data/location_detail/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY payment_type
FROM 's3://taxi-data203/transformed_data/payment_methods/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

copy rate_code
FROM 's3://taxi-data203/transformed_data/rateid_detail/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY store_fwd
FROM 's3://taxi-data203/transformed_data/store_fwd/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY taxi_type
FROM 's3://taxi-data203/transformed_data/taxi_type/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY trip_type
FROM 's3://taxi-data203/transformed_data/trip_type/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';

COPY vendor_details
FROM 's3://taxi-data203/transformed_data/vendor_detail/'
IAM_ROLE 'IAM_ROLE'
CSV
IGNOREHEADER 1
DELIMITER ',';
