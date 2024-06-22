CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
    serialnumber STRING,
    sharewithpublicasofdate BIGINT,
    birthday STRING,
    registrationdate BIGINT,
    sharewithresearchasofdate BIGINT,
    customername STRING,
    email STRING,
    lastupdatedate BIGINT,
    phone STRING,
    sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://billybob-s3-scott-udacity/customer/landing/';
