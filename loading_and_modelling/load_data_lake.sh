# Notes for renaming and loading data to HDFS

Manually renamed the files on local machine while looking through the data dictionary to figure out what files I'd consider loading

# Copy the files up to the EC2 instance

scp -i ~/.ssh/20160905_demo-UCB.pem ./*.csv root@ec2-54-175-135-209.compute-1.amazonaws.com:~/hospital_data

# Need to do this everytime? Is this the full start up?

./setup_ucb_complete_plus_postgres.sh /dev/xvdf
hadoop fs -ls /user/w205

# Move the data into HDFS; keep the headers to preserve the original csv files; gzip prior to move for ingest ease

cp /root/hospital_data ~w205
hadoop fs -mkdir /user/w205/hospital_compare
gzip *.csv
hadoop fs -put ./hospital_data/*.csv.gz /user/w205/hospital_compare
hadoop fs -ls /user/w205/hospital_compare

# Table setup and loading

-- complicationshospital

DROP TABLE IF EXISTS complicationshospital;
CREATE EXTERNAL TABLE complicationshospital (
  providerid INT,
  hospitalname STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  countyname STRING,
  phonenumber STRING,
  measurename STRING,
  measureid STRING,
  comparedtonational STRING,
  denominator INT,
  score DECIMAL,
  lowerestimate DECIMAL,
  higherestimate DECIMAL,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsHospital.csv.gz' OVERWRITE INTO TABLE complicationshospital;

select count(*) from complicationshospital;
-- 32721

-- complicationsnational

DROP TABLE IF EXISTS complicationsnational;
CREATE EXTERNAL TABLE complicationsnational (
  measurename STRING,
  measureid STRING,
  nationalrate DECIMAL,
  numberhospitalsworse INT,
  numberhospitalssame INT,
  numberhospitalsbetter INT,
  numberhospitalstoofew INT,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsNational.csv.gz' OVERWRITE INTO TABLE complicationsnational;

select count(*) from complicationshospital;
-- 7

-- complicationsstate

DROP TABLE IF EXISTS complicationsstate;
CREATE EXTERNAL TABLE complicationsstate (
  state STRING,
  measurename STRING,
  measureid STRING,
  numberofhospitalsworse INT,
  numberofhospitalssame INT,
  numberofhospitalsbetter INT,
  numberofhospitalstoofew INT,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsState.csv.gz' OVERWRITE INTO TABLE complicationsstate;

select count(*) from complicationshospital;
-- 392

-- hcahpshospital

DROP TABLE IF EXISTS hcahpshospital;
CREATE EXTERNAL TABLE hcahpshospital (
  providerid STRING,
  hospitalname STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  countyname STRING,
  phonenumber STRING,
  hcahpsmeasureid STRING,
  hcahpsquestion STRING,
  hcahpsanswerdesc STRING,
  patientsurveystarrating STRING,
  patientsurveystarratingfootnote STRING,
  hcahpsanswerpercent STRING,
  hcahpsanswerpercentfootnote STRING,
  numberofcompletedsurveys STRING,
  numberofcompletedsurveysfootnote STRING,
  surveyresponseratepercent STRING,
  surveyresponseratepercentfootnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSHospital.csv.gz' OVERWRITE INTO TABLE hcahpshospital;

select count(*) from hcahpshospital;
-- 204,864

-- HCAHPS National

DROP TABLE IF EXISTS hcahpsmational;
CREATE EXTERNAL TABLE hcahpsnational (
  hcahpsmeasureid STRING,
  hcahpsquestion STRING,
  hcahpsanswerdesc STRING,
  hcahpsanswerpercent STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSNational.csv.gz' OVERWRITE INTO TABLE hcahpsnational;

select count(*) from hcahpsnational;
-- 32

-- hcahpsstate

DROP TABLE IF EXISTS hcahpsstate;
CREATE EXTERNAL TABLE hcahpsstate (
  state STRING,
  hcahpsquestion STRING,
  hcahpsmeasureid STRING,
  hcahpsanswerdesc STRING,
  hcahpsanswerpercent STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSState.csv.gz' OVERWRITE INTO TABLE hcahpsstate;

select count(*) from hcahpsstate;
-- 1792

-- hospitalgeneralinfo

DROP TABLE IF EXISTS hospitalgeneralinfo;
CREATE EXTERNAL TABLE hospitalgeneralinfo (
  providerid INT,
  hospitalname STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  countyname STRING,
  phonenumber STRING,
  hospitaltype STRING,
  hospitalownership STRING,
  emergencyservices STRING )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/HospitalGeneralInformation.csv.gz' OVERWRITE INTO TABLE hospitalgeneralinfo;

select count(*) from hospitalgeneralinfo;
-- 4824

-- measuredates

DROP TABLE IF EXISTS measuredates;
CREATE EXTERNAL TABLE measuredates (
  measurename STRING,
  measureid STRING,
  measurestartquarter STRING,
  measurestartdate DATE,
  measureendquarter STRING,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/MeasureDates.csv.gz' OVERWRITE INTO TABLE measuredates;

select count(*) from measuredates;
-- 100

-- readmissionsanddeathshospital

DROP TABLE IF EXISTS readmissionsanddeathshospital;
CREATE EXTERNAL TABLE readmissionsanddeathshospital (
  providerid INT,
  hospitalname STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  countyname STRING,
  phonenumber STRING,
  measurename STRING,
  measureid STRING,
  comparedtonational STRING,
  denominator INT,
  score DECIMAL,
  lowerestimate DECIMAL,
  higherestimate DECIMAL,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsHospital.csv.gz' OVERWRITE INTO TABLE readmissionsanddeathshospital;

select count(*) from readmissionsanddeathshospital;
-- 66,990

-- readmissionsanddeathsnational

DROP TABLE IF EXISTS readmissionsanddeathsnational;
CREATE EXTERNAL TABLE readmissionsanddeathsnational (
  measurename STRING,
  measureid STRING,
  nationalrate DECIMAL,
  numberhospitalsworse INT,
  numberhospitalssame INT,
  numberhospitalsbetter INT,
  numberhospitalstoofew INT,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsNational.csv.gz' OVERWRITE INTO TABLE readmissionsanddeathsnational;

select count(*) from readmissionsanddeathsnational;
-- 14

-- readmissionsanddeathsstate

DROP TABLE IF EXISTS readmissionsanddeathsstate;
CREATE EXTERNAL TABLE readmissionsanddeathsstate (
  state STRING,
  measurename STRING,
  measureid STRING,
  numberofhospitalsworse INT,
  numberofhospitalssame INT,
  numberofhospitalsbetter INT,
  numberofhospitalstoofew INT,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsState.csv.gz' OVERWRITE INTO TABLE readmissionsanddeathsstate;

select count(*) from readmissionsanddeathsstate;
-- 784

-- timelyandeffectivecarehospital

DROP TABLE IF EXISTS timelyandeffectivecarehospital;
CREATE EXTERNAL TABLE timelyandeffectivecarehospital (
  providerid STRING,
  hospitalname STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  countyname STRING,
  phonenumber STRING,
  condition STRING,
  measureid STRING,
  measurename STRING,
  scores STRING,
  sample STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareHospital.csv.gz' OVERWRITE INTO TABLE timelyandeffectivecarehospital;

select count(*) from timelyandeffectivecarehospital;
-- 217,821

-- timelyandeffectivecarenational

DROP TABLE IF EXISTS timelyandeffectivecarenational;
CREATE EXTERNAL TABLE timelyandeffectivecarenational (
  measurename STRING,
  measureid STRING,
  condition STRING,
  category STRING,
  score STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareNational.csv.gz' OVERWRITE INTO TABLE timelyandeffectivecarenational;

select count(*) from timelyandeffectivecarenational;
-- 78

-- timelyandeffectivecarestate

DROP TABLE IF EXISTS timelyandeffectivecarestate;
CREATE EXTERNAL TABLE timelyandeffectivecarestate (
  state STRING,
  condition STRING,
  measurename STRING,
  measureid STRING,
  score STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareState.csv.gz' OVERWRITE INTO TABLE timelyandeffectivecarestate;

select count(*) from timelyandeffectivecarestate;
-- 3827
