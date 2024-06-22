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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719080617387 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1719080617387")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1719081056235 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1719080617387}, transformation_ctx = "SQLQuery_node1719081056235")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719080913078 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1719081056235, database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1719080913078")

job.commit()