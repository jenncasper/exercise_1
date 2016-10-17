-- EXERCISE 1 PART 3 (aka Week 7 in exercise pdf)

-- NOTE: it is assumed hive_transforms_ddl.sql and hive_base_ddl.sql have been previously executed within the Hadoop environment so the tables are available for transforming and querying

--- Q3 INVESTIGATION - Which procedures have the greatest variability between hospitals? There are procedures that are more complex or more rare than others. One may assume then that some hospitals are better at the complex/rare procedures than other hospitals given higher frequency of practice. There are likely procedures that are quite common place that most any hospital should be able to perform well. The purpose of this investigation is too reveal those niche procedures given variability in score across hospitals. 

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