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

# Script generated for node accelerometer_landing
accelerometer_landing_node1719081484967 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{STEDI_BUCKET}/accelerometer/landing/"], "recurse": True},
    transformation_ctx="accelerometer_landing_node1719081484967"
)

# Script generated for node customer_trusted
customer_trusted_node1719081906979 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1719081906979")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z
FROM 
    accelerometer_landing a
JOIN 
    customer_trusted c
ON 
    a.user = c.email
'''
SQLQuery_node1719081510520 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":accelerometer_landing_node1719081484967, "customer_trusted":customer_trusted_node1719081906979}, transformation_ctx = "SQLQuery_node1719081510520")

# Script generated for node accelerometer_trusted
accelerometer_trusted_sink = glueContext.getSink(
    path=f"{STEDI_BUCKET}/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_sink"
)
accelerometer_trusted_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
accelerometer_trusted_sink.setFormat("json")
accelerometer_trusted_sink.writeFrame(SQLQuery_node1719081510520)

job.commit()
