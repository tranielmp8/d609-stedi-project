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

# Script generated for node customLanding
customLanding_node1736996022141 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/customer/landing/"], "recurse": True}, transformation_ctx="customLanding_node1736996022141")

# Script generated for node SQL Query
SqlQuery124 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null;
'''
SQLQuery_node1737003012563 = sparkSqlQuery(glueContext, query = SqlQuery124, mapping = {"myDataSource":customLanding_node1736996022141}, transformation_ctx = "SQLQuery_node1737003012563")

# Script generated for node customerTrusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737003012563, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736996010370", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customerTrusted_node1736996193579 = glueContext.getSink(path="s3://stedi-lake-house-d609/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="customerTrusted_node1736996193579")
customerTrusted_node1736996193579.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customerTrusted_node1736996193579.setFormat("json")
customerTrusted_node1736996193579.writeFrame(SQLQuery_node1737003012563)
job.commit()