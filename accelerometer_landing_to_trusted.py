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

# Script generated for node accelerometor_landing
accelerometor_landing_node1719081484967 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometor_landing_node1719081484967")

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
    accelerometor_landing a
JOIN 
    customer_trusted c
ON 
    a.user = c.email
'''
SQLQuery_node1719081510520 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometor_landing":accelerometor_landing_node1719081484967, "customer_trusted":customer_trusted_node1719081906979}, transformation_ctx = "SQLQuery_node1719081510520")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719082030194 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1719081510520, database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1719082030194")

job.commit()