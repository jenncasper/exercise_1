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



CREATE TABLE temp 
  ( 
     name STRING, 
     id   INT 
  ) 
row format delimited fields terminated BY ',' lines terminated BY '\n' 
tblproperties("skip.header.line.count"="1"); 

load data local inpath '/home/cluster/TestHive.csv' into table db.test;

sed -i 1d ComplicationsHospital.csv

DROPTABLE IF EXISTS ComplicationsHospital;
CREATE EXTERNAL TABLE ComplicationsHospital (
  Provider_ID INT,
  Hospital_Name STRING,
  Address STRING,
  City STRING,
  State STRING,
  Zip_Code STRING,
  County_Name STRING,
  Phone_Number STRING,
  Measure_Name STRING,
  Measure_ID STRING,
  Compared_to_National STRING,
  Denominator INT,
  Score DECIMAL,
  Lower_Estimate DECIMAL,
  Higher_Estimate DECIMAL,
  Footnote STRING,
  Measure_Start_Date DATE,
  Measure_End_Date DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
LOCATION ‘/user/w205/hospital_compare’;



CREATE EXTERNAL TABLE IF NOT EXISTS Cars(
  Provider_ID INT,
  Hospital_Name STRING,
  Address STRING,
  City STRING,
  State STRING,
  Zip_Code STRING,
  County_Name STRING,
  Phone_Number STRING,
  Measure_Name STRING,
  Measure_ID STRING,
  Compared_to_National STRING,
  Denominator INT,
  Score DECIMAL,
  Lower_Estimate DECIMAL,
  Higher_Estimate DECIMAL,
  Footnote STRING,
  Measure_Start_Date DATE,
  Measure_End_Date DATE)
COMMENT 'ComplicationsHospital.csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/<username>/visdata';
