# exercise_1
UCB SRD Exercise1 - part 1

1. Create a folder in your github repository for exercise_1
  1.1. Create a folder under exercise_1 called loading_and_modelling
2. Load the raw data files into HDFS under “/user/w205/hospital_compare” on either your AWS or Vagrant machine
  2.1. It is strongly recommended that you remove the header lines from the files before loading them into HDFS
  2.2. You may want to consider renaming the files to eliminate spaces and better describe the contents
  2.3. Place the commands to do any renaming and loading to HDFS in a file called “load_data_lake.sh”
    2.3.1. Add this file to your git repository, commit and push the changes
3. Build an ER diagram for the entities and relationships you need to answer the questions above
  3.1. At minimum, your ER diagram must include entities for Hospitals, Procedures and Survey Results
  3.2. Save this ER diagram as a PNG file to the loading_and_modelling directory
  3.3. Add it to your git repository, commit and push the changes
4. Write Data Definition Language (DDL) SQL statements for each of the base files you have loaded into HDFS
  4.1. DDL statements are of the form
    4.1.1. DROPTABLE<table_name>;
    4.1.2. CREATE EXTERNAL TABLE <table_name> (<col1_name>, <col1_type>, ...)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
            WITH SERDEPROPERTIES (
            "separatorChar" = ",", 
            "quoteChar" = '"',
            "escapeChar" = '\\' 
            )
            STORED AS TEXTFILE
            LOCATION ‘/path/in/hdfs’;
    4.1.3. Store the statements in a file called “hive_base_ddl.sql”
      4.1.3.1. Run this file in Hive using: hive –f /path/to/hive_base_ddl.sql
      4.1.3.2. Refer to the Hive Language Manual for more detail on Data Definition statements
      4.1.3.3. When the DDL is error free, add this file to your git repository, commit and push the changes
