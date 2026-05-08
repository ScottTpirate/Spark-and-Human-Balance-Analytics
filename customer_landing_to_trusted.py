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

# Script generated for node customer_landing
customer_landing_node1719080617387 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{STEDI_BUCKET}/customer/landing/"], "recurse": True},
    transformation_ctx="customer_landing_node1719080617387"
)

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1719081056235 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1719080617387}, transformation_ctx = "SQLQuery_node1719081056235")

# Script generated for node customer_trusted
customer_trusted_sink = glueContext.getSink(
    path=f"{STEDI_BUCKET}/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)
customer_trusted_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
customer_trusted_sink.setFormat("json")
customer_trusted_sink.writeFrame(SQLQuery_node1719081056235)

job.commit()
