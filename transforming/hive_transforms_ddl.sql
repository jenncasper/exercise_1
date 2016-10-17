-- EXERCISE 1 PART 2 (aka Week 6 in exercise pdf)

-- NOTE: it is assumed hive_base_ddl.sql have been previously executed within the Hadoop environment so the tables are available for transforming and querying

USE jenncasper_exercise1;

-- Create table with hospital and state level procedures information
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

-- Fill tmp_timelyandeffectivecarehospital with transformed fields
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

-- Create table with patient survey information
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
COMMENT 'Tmp table for values supporting the fourth investigation';

-- Fill tmp_hcahpshospital with transformed fields
INSERT OVERWRITE TABLE tmp_hcahpshospital
  SELECT providerid, hospitalname, address, city, state, zip_code, countyname, phonenumber, 
  hcahpsmeasureid, hcahpsquestion, hcahpsanswerdesc, cast(patientsurveystarrating as bigint), 
  patientsurveystarratingfootnote, hcahpsanswerpercent, hcahpsanswerpercentfootnote, numberofcompletedsurveys, 
  numberofcompletedsurveysfootnote, surveyresponseratepercent, surveyresponseratepercentfootnote, 
  cast(concat(substr(measurestartdate,7,4), '-', substr(measurestartdate,1,2), '-', substr(measurestartdate,4,2)) as date) as measurestartdate, 
  cast(concat(substr(measureenddate,7,4), '-', substr(measureenddate,1,2), '-', substr(measureenddate,4,2)) as date) as measureenddate
  FROM hcahpshospital;

-- Create table with average scores by providerid
DROP TABLE IF EXISTS tmp_provideravgscore;
CREATE TABLE tmp_provideravgscore AS SELECT providerid, avg(score) AS avgscore FROM tmp_timelyandeffectivecarehospital GROUP BY providerid;

-- Create table with procedure variance scores by providerid
DROP TABLE IF EXISTS tmp_providervarscore;
CREATE TABLE tmp_providervarscore AS SELECT providerid, variance(score) AS variancescore FROM tmp_timelyandeffectivecarehospital GROUP BY providerid;

-- Create table with patient survey, average scores, and variance scores to support the fourth hospitals and patients investigation
DROP TABLE IF EXISTS tmp_hcahpshospitalwithscores;
CREATE TABLE tmp_hcahpshospitalwithscores AS SELECT a.*, b.avgscore, c.variancescore
FROM tmp_hcahpshospital a, tmp_provideravgscore b, tmp_providervarscore c
WHERE a.providerid = b.providerid AND a.providerid = c.providerid;


-- REFERENCES

-- https://www.medicare.gov/hospitalcompare/Data/Data-Updated.html#
-- http://www.java2s.com/Code/SQL/Select-Clause/Countandgroupbytwocolumns.htm
-- http://firebirdsql.org/manual/nullguide-aggrfunc.html
-- https://docs.treasuredata.com/articles/hive-aggregate-functions
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select#LanguageManualSelect-ALLandDISTINCTClauses
-- http://www.javatpoint.com/hive-sort-by-order-by
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
-- https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Joins