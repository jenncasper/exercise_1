# Notes for renaming and loading data to HDFS

Manually renamed the files on local machine while looking through the data dictionary to figure out what files I'd consider loading

# Copy the files up to the EC2 instance

scp -i ~/.ssh/20160905_demo-UCB.pem ./*.csv root@ec2-54-175-135-209.compute-1.amazonaws.com:~/hospital_data

# Need to do this everytime? Is this the full start up?

./setup_ucb_complete_plus_postgres.sh /dev/xvdf
hadoop fs -ls /user/w205

# Move the data into HDFS; keep the headers to preserve the original csv files

cp /root/hospital_data ~w205
hadoop fs -mkdir /user/w205/hospital_compare
hadoop fs -put ./hospital_data/*.csv /user/w205/hospital_compare
hadoop fs -ls /user/w205/hospital_compare

# ComplicationsHospital.csv

# do this if the tableproperties isn't available - sed -i 1d ComplicationsHospital.csv

DROP TABLE IF EXISTS ComplicationsHospital;
CREATE EXTERNAL TABLE ComplicationsHospital (
  ProviderID INT,
  HospitalName STRING,
  Address STRING,
  City STRING,
  State STRING,
  Zip_Code STRING,
  CountyName STRING,
  PhoneNumber STRING,
  MeasureName STRING,
  MeasureID STRING,
  ComparedToNational STRING,
  Denominator INT,
  Score DECIMAL,
  LowerEstimate DECIMAL,
  HigherEstimate DECIMAL,
  Footnote STRING,
  MeasureStartDate DATE,
  MeasureEndDate DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare'
tblproperties("skip.header.line.count"="1");

select count(*) from complicationshospital;
32721

# ComplicationsNational.csv

DROP TABLE IF EXISTS ComplicationsNational;
CREATE EXTERNAL TABLE ComplicationsNational (
  MeasureName STRING,
  MeasureID STRING,
  NationalRate DECIMAL,
  NumberHospitalsWorse INT,
  NumberHospitalsSame INT,
  NumberHospitalsBetter INT,
  NumberHospitalsTooFew INT,
  Footnote STRING,
  MeasureStartDate DATE,
  MeasureEndDate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare'
tblproperties("skip.header.line.count"="1");

select count(*) from complicationshospital;
32721

CREATE DATABASE IF NOT EXISTS lahman;

USE lahman;

CREATE TABLE AllstarFull (playerID string,yearID string,gameNum string,gameID string,teamID string,lgID string,GP string,startingPos string) row format delimited fields terminated by ',' stored as textfile;

LOAD DATA INPATH '/user/bigdataproject/AllstarFull.csv' OVERWRITE INTO TABLE AllstarFull;

SELECT * FROM AllstarFull;

DROP TABLE IF EXISTS ComplicationsNational;
CREATE EXTERNAL TABLE ComplicationsNational (
  MeasureName STRING,
  MeasureID STRING,
  NationalRate DECIMAL,
  NumberHospitalsWorse INT,
  NumberHospitalsSame INT,
  NumberHospitalsBetter INT,
  NumberHospitalsTooFew INT,
  Footnote STRING,
  MeasureStartDate DATE,
  MeasureEndDate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:8020/user/w205/hospital_compare/ComplicationsNational.csv'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsNational.csv' OVERWRITE INTO TABLE ComplicationsNational;
