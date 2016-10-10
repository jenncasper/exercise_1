-- EXERCISE 1 PART 3 (aka Week 7 in exercise pdf)

-- NOTE: it is assumed hive_transforms_ddl.sql and hive_base_ddl.sql have been previously executed within the Hadoop environment so the tables are available for transforming and querying

--- Q4 INVESTIGATION - Are average scores for hospital quality or procedural variability correlated with patient survey responses? The interpreted intent of this question is to determine if the average score for a hospital or the procedure variability within a hospital may be indicative of what a patient experiences thus reflected in the patient survey responses. 

USE jenncasper_exercise1;

DROP TABLE IF EXISTS query_procedurevariability;
CREATE TABLE query_procedurevariability AS SELECT measureid, measurename, count(distinct providerid) AS providernum, variance(score) AS variance FROM tmp_timelyandeffectivecarehospital GROUP BY measureid, measurename ORDER BY providernum desc, variance desc LIMIT 10;

select * from query_procedurevariability;

-- REFERENCES

-- https://www.medicare.gov/hospitalcompare/Data/Data-Updated.html#
-- http://www.java2s.com/Code/SQL/Select-Clause/Countandgroupbytwocolumns.htm
-- http://firebirdsql.org/manual/nullguide-aggrfunc.html
-- https://docs.treasuredata.com/articles/hive-aggregate-functions
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select#LanguageManualSelect-ALLandDISTINCTClauses
-- http://www.javatpoint.com/hive-sort-by-order-by