CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorReadingTime TIMESTAMP,
    serialNumber STRING,
    distanceFromObject FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://billybob-s3-scott-udacity/step_trainer/landing/';
