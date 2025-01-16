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

# Script generated for node customer_curated
customer_curated_node1737054348259 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1737054348259")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1737054005531 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-d609/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1737054005531")

# Script generated for node transform_column_names
transform_column_names_node1737057840482 = ApplyMapping.apply(frame=step_trainer_trusted_node1737054005531, mappings=[("sensorreadingtime", "bigint", "sensorreadingtime", "long"), ("serialnumber", "string", "step_serialnumber", "string"), ("distancefromobject", "int", "step_distancefromobject", "int")], transformation_ctx="transform_column_names_node1737057840482")

# Script generated for node Join
Join_node1737054388473 = Join.apply(frame1=transform_column_names_node1737057840482, frame2=customer_curated_node1737054348259, keys1=["step_serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1737054388473")

# Script generated for node Drop Fields
DropFields_node1737058240148 = DropFields.apply(frame=Join_node1737054388473, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "phone", "sharewithresearchasofdate"], transformation_ctx="DropFields_node1737058240148")

# Script generated for node Change Schema
ChangeSchema_node1737058991805 = ApplyMapping.apply(frame=DropFields_node1737058240148, mappings=[("sensorreadingtime", "long", "sensorreadingtime", "long"), ("step_serialnumber", "string", "serialnumber", "string"), ("step_distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1737058991805")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1737058991805, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737045674366", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1737055900484 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1737058991805, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-d609/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="step_trainer_trusted_node1737055900484")

job.commit()