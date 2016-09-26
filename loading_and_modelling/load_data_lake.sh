## Notes for renaming and loading data to HDFS

# Manually renamed the files on local machine while looking 
# through the data dictionary to figure out what files I'd 
# consider loading. I only included the files I used in the
# loading_and_modelling\hospital_data folder on my git
# exercise1 project.

## Copy the files up to the EC2 instance

scp -i ~/.ssh/20160905_demo-UCB.pem ./*.csv root@ec2-54-175-135-209.compute-1.amazonaws.com:~/hospital_data

## Make sure everything is setup on the EC2 instance
# Things for me to look into:
# - Need to do this everytime? Is this the full start up?
# - Spin up multiple EC2 instances to use in the Hadoop cluster..how to do this?

./setup_ucb_complete_plus_postgres.sh /dev/xvdf
hadoop fs -ls /user/w205

## Move the data into HDFS
# Keep the headers to preserve the original csv files
# Gzip prior to move for ingest ease

cp /root/hospital_data ~w205
hadoop fs -mkdir /user/w205/hospital_compare
gzip *.csv
hadoop fs -put ./hospital_data/*.csv.gz /user/w205/hospital_compare
hadoop fs -ls /user/w205/hospital_compare

## Hive table setup and loading
# See hive_base_ddl.sql file in git project exercise1

hive –f ./hive_base_ddl.sql

