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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719086370653 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719086370653")

# Script generated for node customers_curated
customers_curated_node1719086370042 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customers_curated_node1719086370042")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1719086371042 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1719086371042")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    s.sensorReadingTime,
    s.serialNumber as stepTrainerSerialNumber,
    CAST(s.distanceFromObject AS FLOAT) as distanceFromObject, -- casting to FLOAT if necessary
    a.user as email,
    a.timeStamp as accelerometerTimeStamp,
    CAST(a.x AS FLOAT) as x,  -- ensure this matches the FLOAT type in table definition
    CAST(a.y AS FLOAT) as y,
    CAST(a.z AS FLOAT) as z
FROM 
    step_trainer_trusted s
JOIN 
    accelerometer_trusted a ON s.sensorReadingTime = a.timeStamp
JOIN 
    customers_curated c ON s.serialNumber = c.serialnumber
'''
SQLQuery_node1719086409736 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1719086371042, "accelerometer_trusted":accelerometer_trusted_node1719086370653, "customers_curated":customers_curated_node1719086370042}, transformation_ctx = "SQLQuery_node1719086409736")

# Script generated for node ml_curated
ml_curated_node1719086455134 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1719086409736, database="stedi", table_name="machine_learning_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="ml_curated_node1719086455134")

job.commit()