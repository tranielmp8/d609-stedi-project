CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `x` float COMMENT 'from deserializer', 
  `y` float COMMENT 'from deserializer', 
  `z` float COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-d609/accelerometer/landing'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'classification'='json', 
  'last_modified_by'='hadoop', 
  'last_modified_time'='1737052708', 
  'numFiles'='9', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='6871328')