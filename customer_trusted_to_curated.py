import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Replace this with your own S3 bucket before running the Glue job.
STEDI_BUCKET = "s3://scott-wgu-d609"

# Script generated for node customer_trusted
customer_trusted_node1719083133576 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1719083133576")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719083246551 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719083246551")

# Script generated for node SQL Query
SqlQuery1 = '''
SELECT distinct
    c.serialnumber,
    c.sharewithpublicasofdate,
    c.birthday,
    c.registrationdate,
    c.sharewithresearchasofdate,
    c.customername,
    c.email,
    c.lastupdatedate,
    c.phone,
    c.sharewithfriendsasofdate
FROM 
    customer_trusted c
JOIN 
    accelerometer_trusted a
ON 
    c.email = a.user
'''
SQLQuery_node1719083815068 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"accelerometer_trusted":accelerometer_trusted_node1719083246551, "customer_trusted":customer_trusted_node1719083133576}, transformation_ctx = "SQLQuery_node1719083815068")

# Script generated for node distinct
SqlQuery0 = '''
select distinct * from myDataSource
'''
distinct_node1719084836773 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":SQLQuery_node1719083815068}, transformation_ctx = "distinct_node1719084836773")

# Script generated for node customer_curated
customer_curated_sink = glueContext.getSink(
    path=f"{STEDI_BUCKET}/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_sink"
)
customer_curated_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_curated")
customer_curated_sink.setFormat("json")
customer_curated_sink.writeFrame(distinct_node1719084836773)

job.commit()
