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
# See hive_base_ddl.sql file
hive –f ./hive_base_ddl.sql

