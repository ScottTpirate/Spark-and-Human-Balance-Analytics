CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorReadingTime TIMESTAMP,
    serialNumber STRING,
    distanceFromObject FLOAT
)
STORED AS PARQUET
LOCATION 's3://path-to-landing-zone/step_trainer_landing/';
