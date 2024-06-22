CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
    serialnumber STRING,
    sharewithpublicasofdate DATE,
    birthday DATE,
    registrationdate DATE,
    sharewithresearchasofdate DATE,
    customername STRING,
    email STRING,
    lastupdatedate DATE,
    phone STRING,
    sharewithfriendsasofdate DATE
)
STORED AS PARQUET
LOCATION 's3://path-to-landing-zone/customer_landing/';
