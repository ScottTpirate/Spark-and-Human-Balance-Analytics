CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
    timeStamp TIMESTAMP,
    user STRING,
    x FLOAT,
    y FLOAT,
    z FLOAT
)
STORED AS PARQUET
LOCATION 's3://path-to-landing-zone/accelerometer_landing/';
