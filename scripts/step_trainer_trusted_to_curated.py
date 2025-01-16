import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node accelerometer trusted
accelerometertrusted_node1737060291289 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1737060291289")

# Script generated for node step trainer  trusted
steptrainertrusted_node1737060293196 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/step_trainer/trusted/"], "recurse": True}, transformation_ctx="steptrainertrusted_node1737060293196")

# Script generated for node machine learning join
machinelearningjoin_node1737060296576 = Join.apply(frame1=steptrainertrusted_node1737060293196, frame2=accelerometertrusted_node1737060291289, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="machinelearningjoin_node1737060296576")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=machinelearningjoin_node1737060296576, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737060268663", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737060535751 = glueContext.write_dynamic_frame.from_options(frame=machinelearningjoin_node1737060296576, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-d609/machine_learning/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1737060535751")

job.commit()