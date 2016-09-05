CREATE DATABASE IF NOT EXISTS spark_homework3;

USE spark_homework3;

DROP Table Target_TEMPORARY;

CREATE TEMPORARY TABLE Target_TEMPORARY (
  result DOUBLE
) stored as textfile;

LOAD DATA INPATH '/user/maria_dev/Target.csv' OVERWRITE INTO TABLE Target_TEMPORARY;

DROP Table Target;

create table Target as SELECT *, ROW_NUMBER() OVER () AS row_num FROM Target_TEMPORARY;

