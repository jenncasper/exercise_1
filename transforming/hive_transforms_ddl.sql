-- EXERCISE 1
DROP DATABASE IF EXISTS jenncasper_exercise1 CASCADE;
CREATE DATABASE jenncasper_exercise1;
USE jenncasper_exercise1;

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
COMMENT 'Hospital-level results for surgical complications measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsHospital.csv.gz' OVERWRITE INTO TABLE complicationshospital;
-- select count(*) from complicationshospital;
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
COMMENT 'National-level results for surgical complications measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsNational.csv.gz' OVERWRITE INTO TABLE complicationsnational;
-- select count(*) from complicationshospital;
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
COMMENT 'State-level results for surgical complications measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ComplicationsState.csv.gz' OVERWRITE INTO TABLE complicationsstate;
-- select count(*) from complicationshospital;
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
COMMENT 'Hospital-level results for the Hospital Consumer Assessment of Healthcare Providers and Systems'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSHospital.csv.gz' OVERWRITE INTO TABLE hcahpshospital;
-- select count(*) from hcahpshospital;
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
COMMENT 'National-level results for the Hospital Consumer Assessment of Healthcare Providers and Systems'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSNational.csv.gz' OVERWRITE INTO TABLE hcahpsnational;
-- select count(*) from hcahpsnational;
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
COMMENT 'State-level results for the Hospital Consumer Assessment of Healthcare Providers and Systems'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HCAHPSState.csv.gz' OVERWRITE INTO TABLE hcahpsstate;
-- select count(*) from hcahpsstate;
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
COMMENT 'General information on hospitals within the dataset'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/HospitalGeneralInformation.csv.gz' OVERWRITE INTO TABLE hospitalgeneralinfo;
-- select count(*) from hospitalgeneralinfo;
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
COMMENT 'Current collection dates for all measures on Hospital Compare'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/MeasureDates.csv.gz' OVERWRITE INTO TABLE measuredates;
-- select count(*) from measuredates;
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
COMMENT 'Hospital-level results for 30-day mortality and readmissions measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsHospital.csv.gz' OVERWRITE INTO TABLE readmissionsanddeathshospital;
-- select count(*) from readmissionsanddeathshospital;
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
COMMENT 'National-level results for 30-day mortality and readmissions measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsNational.csv.gz' OVERWRITE INTO TABLE readmissionsanddeathsnational;
-- select count(*) from readmissionsanddeathsnational;
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
COMMENT 'State-level results for 30-day mortality and readmissions measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/ReadmissionsandDeathsState.csv.gz' OVERWRITE INTO TABLE readmissionsanddeathsstate;
-- select count(*) from readmissionsanddeathsstate;
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
COMMENT 'Hospital-level results for Process of Care measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareHospital.csv.gz' OVERWRITE INTO TABLE timelyandeffectivecarehospital;
-- select count(*) from timelyandeffectivecarehospital;
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
COMMENT 'National-level results for Process of Care measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareNational.csv.gz' OVERWRITE INTO TABLE timelyandeffectivecarenational;
-- select count(*) from timelyandeffectivecarenational;
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
COMMENT 'State-level results for Process of Care measures'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = '"', "escapeChar" = '\\')
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/w205/hospital_compare/TimelyandEffectiveCareState.csv.gz' OVERWRITE INTO TABLE timelyandeffectivecarestate;
-- select count(*) from timelyandeffectivecarestate;
-- 3827

—- EXERCISE 1 - Part II (aka Week 6 in exercise pdf)

—- Questions to answer with transformed data:
—- What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.

select count(*) from hcahpshospital;
—- 204,864

select count(*) from measuredates;
—- 100

select count(distinct measureid) from measuredates;
—- 100 —- get measure name from this table

select count(*) from hospitalgeneralinfo;
—- 4824

select count(distinct providerid) from hospitalgeneralinfo;
—- 4824 —- get hospital name from this table

—- create a new table joining hcahpshospital records with measuredates and hospitalgeneralinfo

DROP TABLE IF EXISTS tmp_hcahpshospital;
CREATE TABLE tmp_hcahpshospital (
  viewTime INT, userid BIGINT,
     page_url STRING, referrer_url STRING,
     ip STRING COMMENT 'IP Address of the User')
 COMMENT ‘Tmp table for casting necessary values to supporting query’
 PARTITIONED BY(dt STRING, country STRING)
 STORED AS SEQUENCEFILE;

DROP TABLE IF EXISTS tmp_hcahpshospital;
CREATE TABLE tmp_hcahpshospital (
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
  patientsurveystarrating BIGINT,
  patientsurveystarratingfootnote STRING,
  hcahpsanswerpercent STRING,
  hcahpsanswerpercentfootnote STRING,
  numberofcompletedsurveys STRING,
  numberofcompletedsurveysfootnote STRING,
  surveyresponseratepercent STRING,
  surveyresponseratepercentfootnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
COMMENT 'Tmp table for casting necessary values to supporting query’;

INSERT INTO TABLE tablename SELECT value1,value2 FROM tempTable_with_atleast_one_records LIMIT 1.

select x from table where cast(field as double) is not null
select case when abs(x%1)>0 then x else cast(x as bigint) end
insert into new_table select col1,col2..cast(coln as bigint)..colm from old_table

INSERT OVERWRITE TABLE tmp_hcahpshospital
  SELECT providerid, hospitalname, address, city, state, zip_code, countyname, 
         phonenumber, hcahpsmeasureid, hcahpsquestion, hcahpsanswerdesc, 
         cast(patientsurveystarrating as bigint), patientsurveystarratingfootnote, hcahpsanswerpercent, 
         hcahpsanswerpercentfootnote, numberofcompletedsurveys, numberofcompletedsurveysfootnote, 
         surveyresponseratepercent, surveyresponseratepercentfootnote, measurestartdate, measureenddate
  FROM hcahpshospital;
—- 204,864
—- 42,576 non null patientsurveystarrating records

DROP TABLE IF EXISTS tmp_measuredates;
CREATE TABLE tmp_measuredates (
  measurename STRING,
  measureid STRING )
COMMENT 'Tmp table for the first query';

INSERT OVERWRITE TABLE tmp_measuredates
  SELECT measurename, measureid
  FROM measuredates;

DROP TABLE IF EXISTS tmp_hospitalgeneralinfo;
CREATE TABLE tmp_hospitalgeneralinfo (
  providerid STRING,
  hospitaltype STRING,
  hospitalownership STRING,
  emergencyservices STRING )
COMMENT 'Tmp table for the first query';

INSERT OVERWRITE TABLE tmp_hospitalgeneralinfo
  SELECT providerid, hospitalname, address, city, state, zip_code, countyname, 
  phonenumber, hospitaltype, hospitalownership, emergencyservices
  FROM hospitalgeneralinfo;


—- What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.

DROP TABLE IF EXISTS tmp_timelyandeffectivecarehospital;
CREATE TABLE tmp_timelyandeffectivecarehospital (
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
  score BIGINT,
  sample STRING,
  footnote STRING,
  measurestartdate DATE,
  measureenddate DATE )
COMMENT 'Tmp table for hospital scores on high procedures';

INSERT OVERWRITE TABLE tmp_timelyandeffectivecarehospital
  SELECT providerid, hospitalname, address, city, state, zip_code, countyname, phonenumber, condition, 
  measureid, measurename,  
  CASE
    WHEN scores LIKE 'Very High%' THEN cast(1000 as bigint)
    WHEN scores LIKE 'High%' THEN cast(900 as bigint)
    WHEN scores LIKE 'Medium%' THEN cast(500 as bigint)
    WHEN scores LIKE 'Low%' THEN cast(10 as bigint)
    ELSE cast(scores as bigint)
  END AS score, 
  sample, footnote, 
  cast(concat(substr(measurestartdate,7,4), '-', substr(measurestartdate,1,2), '-', substr(measurestartdate,4,2)) as date) as measurestartdate, 
  cast(concat(substr(measureenddate,7,4), '-', substr(measureenddate,1,2), '-', substr(measureenddate,4,2)) as date) as measureenddate
  FROM timelyandeffectivecarehospital;

DROP TABLE IF EXISTS summary_of_timelyandeffectivecarehospital_hospitalprocedures;
CREATE TABLE summary_of_timelyandeffectivecarehospital_hospitalprocedures AS SELECT providerid, measureid, score, avg(score) AS avg, max(score) AS max, min(score) AS min FROM tmp_timelyandeffectivecarehospital GROUP BY providerid, measureid, score;

select providerid, measureid, avg(score), count(*) from (
  select providerid, measureid, score from tmp_timelyandeffectivecarehospital
) t
group by providerid, measureid;

insert overwrite local directory '/home/carter/staging' row format delimited fields terminated by ',' select * from hugetable;

—- measured, hospital, avg, high, low


-- Create new table and copy all data.
CREATE TABLE clone_of_t1 AS SELECT * FROM t1;
-- Same idea as CREATE TABLE LIKE, do not copy any data.
CREATE TABLE empty_clone_of_t1 AS SELECT * FROM t1 WHERE 1=0;
-- Copy some data.
CREATE TABLE subset_of_t1 AS SELECT * FROM t1 WHERE x > 100 AND y LIKE 'A%';
CREATE TABLE summary_of_t1 AS SELECT c1, sum(c2) AS total, avg(c2) AS average FROM t1 GROUP BY c2;
-- Switch file format.
CREATE TABLE parquet_version_of_t1 STORED AS PARQUET AS SELECT * FROM t1;
-- Create tables with different column order, names, or types than the original.
CREATE TABLE some_columns_from_t1 AS SELECT c1, c3, c5 FROM t1;
CREATE TABLE reordered_columns_from_t1 AS SELECT c4, c3, c1, c2 FROM t1;
CREATE TABLE synthesized_columns AS SELECT upper(c1) AS all_caps, c2+c3 AS total, "California" AS state FROM t1;

CREATE TABLE tmp_hospitalquality AS SELECT * FROM tmp_hcahpshospital JOIN tmp_measuredates ON (tmp_hcahpshospital.hcahpsmeasureid = tmp_measuredates.measureid);

SELECT a.val, b.val, c.val FROM a JOIN b ON (a.key = b.key1) JOIN c ON (c.key = b.key2)

CREATE TABLE tmp_hospitalquality AS SELECT 

INSERT OVERWRITE TABLE _tableName_ PARTITION (_partitionColumn_= _partitionValue_) 
SELECT [other Things], CASE WHEN id=206 THEN 'florida' ELSE location END AS location, [other Other Things] 
FROM _tableName_ WHERE [_whereClause_];

SELECT species, sex, COUNT(*) FROM Bird GROUP BY species, sex;

select customer_name, product_name, avg(price)
from (
  select distinct customer_name, product_name, price, occurance_id from cprd
) t
group by customer_name, product_name




—- query the table for aggregate, average, and variability of scores by hospital
—- query the table for aggregate, average, and variability of scores by procedure and then by hospital


—- 2) What states are models of high-quality care?

CREATE TABLE summary_of_timelyandeffectivecarehospital_hospitalprocedures AS SELECT providerid, measureid, avg(score) AS avg, max(score) AS max, min(score) AS min FROM tmp_timelyandeffectivecarehospital GROUP BY providerid, measureid;

select 

group by state
—- 3) Which procedures have the greatest variability between hospitals?

variance()
—- 4) Are average scores for hospital quality or procedural variability correlated with patient survey responses?

712
714
72
724
726
73
734
74
740
75
76
77
78
79
8
80
81
82
826
83
84
85
86
87
88
89
9
90
91
92
93
94
942
95
96
97
98
99
High (40,000 - 59,999 patients annually)
Low (0 - 19,999 patients annually)
Medium (20,000 - 39,999 patients annually)
Not Available
Very High (60,000+ patients annually)
Time taken: 55.283 seconds, Fetched: 560 row(s)

—- REFERENCES

—- https://www.medicare.gov/hospitalcompare/Data/Data-Updated.html#
—- http://www.java2s.com/Code/SQL/Select-Clause/Countandgroupbytwocolumns.htm
—- http://firebirdsql.org/manual/nullguide-aggrfunc.html
—- https://docs.treasuredata.com/articles/hive-aggregate-functions
—- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select#LanguageManualSelect-ALLandDISTINCTClauses