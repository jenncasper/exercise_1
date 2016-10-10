-- EXERCISE 1 PART 3 (aka Week 7 in exercise pdf)

-- NOTE: it is assumed hive_transforms_ddl.sql and hive_base_ddl.sql have been previously executed within the Hadoop environment so the tables are available for transforming and querying

--- Q4 INVESTIGATION - Are average scores for hospital quality or procedural variability correlated with patient survey responses? The interpreted intent of this question is to determine if the average score for a hospital or the procedure variability within a hospital may be indicative of what a patient experiences thus reflected in the patient survey responses. 

USE jenncasper_exercise1;

DROP TABLE IF EXISTS query_hospitalsandpatients;
CREATE TABLE query_hospitalsandpatients AS SELECT providerid, hospitalname, avgscore, variancescore, avg(patientsurveystarrating) AS avgsurvey FROM tmp_hcahpshospitalwithscores GROUP BY providerid, hospitalname, avgscore, variancescore ORDER BY avgscore desc, variancescore desc, avgsurvey desc;

select * from query_hospitalsandpatients;

select corr(avgscore, avgsurvey) from query_hospitalsandpatients;

select corr(variancescore, avgsurvey) from query_hospitalsandpatients;

-- REFERENCES

-- https://www.medicare.gov/hospitalcompare/Data/Data-Updated.html#
-- http://www.java2s.com/Code/SQL/Select-Clause/Countandgroupbytwocolumns.htm
-- http://firebirdsql.org/manual/nullguide-aggrfunc.html
-- https://docs.treasuredata.com/articles/hive-aggregate-functions
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select#LanguageManualSelect-ALLandDISTINCTClauses
-- http://www.javatpoint.com/hive-sort-by-order-by
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
-- https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient