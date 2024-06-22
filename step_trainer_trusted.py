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

# Script generated for node step trainer landing
steptrainerlanding_node1719085317734 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="steptrainerlanding_node1719085317734")

# Script generated for node customer curated
customercurated_node1719085342041 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customercurated_node1719085342041")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    c.sensorReadingTime,
    c.serialNumber,
    c.distanceFromObject
FROM 
    step_trainer_landing c
JOIN 
    customer_curated a
ON 
    c.serialNumber = a.serialnumber
'''
SQLQuery_node1719085377858 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customercurated_node1719085342041, "step_trainer_landing":steptrainerlanding_node1719085317734}, transformation_ctx = "SQLQuery_node1719085377858")

# Script generated for node step trainer trusted
steptrainertrusted_node1719085608885 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1719085377858, database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1719085608885")

job.commit()