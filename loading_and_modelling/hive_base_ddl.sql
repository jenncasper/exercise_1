-- complicationshospital
DROP TABLE IF EXISTS exercise1.complicationshospital;
CREATE EXTERNAL TABLE exercise1.complicationshospital (
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
COMMENT 'Hospital-level results for surgical complications measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsHospital.csv.gz' OVERWRITE INTO TABLE exercise1.complicationshospital;
-- select count(*) from exercise1.complicationshospital;
-- 32721

-- complicationsnational
DROP TABLE IF EXISTS exercise1.complicationsnational;
CREATE EXTERNAL TABLE exercise1.complicationsnational (
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
COMMENT 'National-level results for surgical complications measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsNational.csv.gz' OVERWRITE INTO TABLE exercise1.complicationsnational;
-- select count(*) from exercise1.complicationshospital;
-- 7

-- complicationsstate
DROP TABLE IF EXISTS exercise1.complicationsstate;
CREATE EXTERNAL TABLE exercise1.complicationsstate (
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
COMMENT 'State-level results for surgical complications measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsState.csv.gz' OVERWRITE INTO TABLE exercise1.complicationsstate;
-- select count(*) from exercise1.complicationshospital;
-- 392

-- hcahpshospital
DROP TABLE IF EXISTS exercise1.hcahpshospital;
CREATE EXTERNAL TABLE exercise1.hcahpshospital (
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
COMMENT 'Hospital-level results for the Hospital Consumer Assessment of Healthcare Providers and Systems'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSHospital.csv.gz' OVERWRITE INTO TABLE exercise1.hcahpshospital;
-- select count(*) from exercise1.hcahpshospital;
-- 204,864

-- HCAHPS National
DROP TABLE IF EXISTS exercise1.hcahpsmational;
CREATE EXTERNAL TABLE exercise1.hcahpsnational (
  hcahpsmeasureid STRING,
  hcahpsquestion STRING,
  hcahpsanswerdesc STRING,
  hcahpsanswerpercent STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
COMMENT 'National-level results for the Hospital Consumer Assessment of Healthcare Providers and Systems'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSNational.csv.gz' OVERWRITE INTO TABLE exercise1.hcahpsnational;
-- select count(*) from exercise1.hcahpsnational;
-- 32

-- hcahpsstate
DROP TABLE IF EXISTS exercise1.hcahpsstate;
CREATE EXTERNAL TABLE exercise1.hcahpsstate (
  state STRING,
  hcahpsquestion STRING,
  hcahpsmeasureid STRING,
  hcahpsanswerdesc STRING,
  hcahpsanswerpercent STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
COMMENT 'State-level results for the Hospital Consumer Assessment of Healthcare Providers and Systems'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSState.csv.gz' OVERWRITE INTO TABLE exercise1.hcahpsstate;
-- select count(*) from exercise1.hcahpsstate;
-- 1792

-- hospitalgeneralinfo
DROP TABLE IF EXISTS exercise1.hospitalgeneralinfo;
CREATE EXTERNAL TABLE exercise1.hospitalgeneralinfo (
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
COMMENT 'General information on hospitals within the dataset'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HospitalGeneralInformation.csv.gz' OVERWRITE INTO TABLE exercise1.hospitalgeneralinfo;
-- select count(*) from exercise1.hospitalgeneralinfo;
-- 4824

-- measuredates
DROP TABLE IF EXISTS exercise1.measuredates;
CREATE EXTERNAL TABLE exercise1.measuredates (
  measurename STRING,
  measureid STRING,
  measurestartquarter STRING,
  measurestartdate DATE,
  measureendquarter STRING,
  measureenddate DATE )
COMMENT 'Current collection dates for all measures on Hospital Compare'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/MeasureDates.csv.gz' OVERWRITE INTO TABLE exercise1.measuredates;
-- select count(*) from exercise1.measuredates;
-- 100

-- readmissionsanddeathshospital
DROP TABLE IF EXISTS exercise1.readmissionsanddeathshospital;
CREATE EXTERNAL TABLE exercise1.readmissionsanddeathshospital (
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
COMMENT 'Hospital-level results for 30-day mortality and readmissions measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsHospital.csv.gz' OVERWRITE INTO TABLE exercise1.readmissionsanddeathshospital;
-- select count(*) from exercise1.readmissionsanddeathshospital;
-- 66,990

-- readmissionsanddeathsnational
DROP TABLE IF EXISTS exercise1.readmissionsanddeathsnational;
CREATE EXTERNAL TABLE exercise1.readmissionsanddeathsnational (
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
COMMENT 'National-level results for 30-day mortality and readmissions measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsNational.csv.gz' OVERWRITE INTO TABLE exercise1.readmissionsanddeathsnational;
-- select count(*) from exercise1.readmissionsanddeathsnational;
-- 14

-- readmissionsanddeathsstate
DROP TABLE IF EXISTS exercise1.readmissionsanddeathsstate;
CREATE EXTERNAL TABLE exercise1.readmissionsanddeathsstate (
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
COMMENT 'State-level results for 30-day mortality and readmissions measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsState.csv.gz' OVERWRITE INTO TABLE exercise1.readmissionsanddeathsstate;
-- select count(*) from exercise1.readmissionsanddeathsstate;
-- 784

-- timelyandeffectivecarehospital
DROP TABLE IF EXISTS exercise1.timelyandeffectivecarehospital;
CREATE EXTERNAL TABLE exercise1.timelyandeffectivecarehospital (
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
COMMENT 'Hospital-level results for Process of Care measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareHospital.csv.gz' OVERWRITE INTO TABLE exercise1.timelyandeffectivecarehospital;
-- select count(*) from exercise1.timelyandeffectivecarehospital;
-- 217,821

-- timelyandeffectivecarenational
DROP TABLE IF EXISTS exercise1.timelyandeffectivecarenational;
CREATE EXTERNAL TABLE exercise1.timelyandeffectivecarenational (
  measurename STRING,
  measureid STRING,
  condition STRING,
  category STRING,
  score STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
COMMENT 'National-level results for Process of Care measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareNational.csv.gz' OVERWRITE INTO TABLE exercise1.timelyandeffectivecarenational;
-- select count(*) from exercise1.timelyandeffectivecarenational;
-- 78

-- timelyandeffectivecarestate
DROP TABLE IF EXISTS exercise1.timelyandeffectivecarestate;
CREATE EXTERNAL TABLE exercise1.timelyandeffectivecarestate (
  state STRING,
  condition STRING,
  measurename STRING,
  measureid STRING,
  score STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
COMMENT 'State-level results for Process of Care measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareState.csv.gz' OVERWRITE INTO TABLE exercise1.timelyandeffectivecarestate;
-- select count(*) from exercise1.timelyandeffectivecarestate;
-- 3827
