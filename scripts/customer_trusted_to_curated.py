import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerator landing
acceleratorlanding_node1737043092034 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/accelerometer/landing/"], "recurse": True}, transformation_ctx="acceleratorlanding_node1737043092034")

# Script generated for node customer trusted
customertrusted_node1737029263662 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1737029263662")

# Script generated for node CustPrivacyJoin
CustPrivacyJoin_node1737043266213 = Join.apply(frame1=customertrusted_node1737029263662, frame2=acceleratorlanding_node1737043092034, keys1=["email"], keys2=["user"], transformation_ctx="CustPrivacyJoin_node1737043266213")

# Script generated for node Drop Fields and Duplicates
SqlQuery549 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource

'''
DropFieldsandDuplicates_node1737047352819 = sparkSqlQuery(glueContext, query = SqlQuery549, mapping = {"myDataSource":CustPrivacyJoin_node1737043266213}, transformation_ctx = "DropFieldsandDuplicates_node1737047352819")

# Script generated for node accelerometer trusted 
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1737047352819, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737041541712", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1737043495684 = glueContext.write_dynamic_frame.from_options(frame=DropFieldsandDuplicates_node1737047352819, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-d609/customer/curated/", "partitionKeys": []}, transformation_ctx="accelerometertrusted_node1737043495684")

job.commit()