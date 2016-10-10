-- EXERCISE 1 PART 3 (aka Week 7 in exercise pdf)

-- NOTE: it is assumed hive_transforms_ddl.sql and hive_base_ddl.sql have been previously executed within the Hadoop environment so the tables are available for transforming and querying

--- Q1 INVESTIGATION - What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.

USE jenncasper_exercise1;

DROP TABLE IF EXISTS query_besthospitals;
CREATE TABLE query_besthospitals AS SELECT providerid, hospitalname, count(distinct measureid) AS measurenum, avg(score) AS avg FROM tmp_timelyandeffectivecarehospital GROUP BY providerid, hospitalname ORDER BY measurenum desc, avg desc LIMIT 10;

select * from query_besthospitals;

-- REFERENCES

-- https://www.medicare.gov/hospitalcompare/Data/Data-Updated.html#
-- http://www.java2s.com/Code/SQL/Select-Clause/Countandgroupbytwocolumns.htm
-- http://firebirdsql.org/manual/nullguide-aggrfunc.html
-- https://docs.treasuredata.com/articles/hive-aggregate-functions
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select#LanguageManualSelect-ALLandDISTINCTClauses
-- http://www.javatpoint.com/hive-sort-by-order-by