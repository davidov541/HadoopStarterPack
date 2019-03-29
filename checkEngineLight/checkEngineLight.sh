#!/bin/bash
hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
source environmentVars.sh
rm environmentVars.sh

# Get some common variables that will be used throughout the tests
user=$(klist | sed -rn 's/Default principal: (.*)@$DAVIDMCGINNIS.NET/\1/p' | tr '[:upper:]' '[:lower:]')
hdfsPath="hdfs://$NAMENODE"
workingDirectory="$hdfsPath/tmp/checkEngineLight"
startPattern="%-30s - "
success="SUCCESS"
failure="FAILURE"

# Try creating a directory in HDFS, and make sure it shows up in -test.
printf "$startPattern" "HDFS mkdir"
hdfs dfs -mkdir $workingDirectory
hdfs dfs -test -d "$workingDirectory"
result=$?
if [ $result -eq 0 ]; then
	echo $success
else
	echo $failure
fi

# Try placing a file from local into HDFS, and ensure it shows up in -test.
printf "$startPattern" "HDFS Put"
printf "1,1\n2,4\n3,9\n4,16\n5,25\n6,36\n7,49\n8,64\n9,81\n10,100" > testData.csv
hdfs dfs -put testData.csv $workingDirectory/testData.csv
hdfs dfs -test -e "$workingDirectory/testData.csv"
result=$?
if [ $result -eq 0 ]; then
	echo $success
else
	echo $failure
fi

# Try getting a file from HDFS and placing it locally, and ensure the file is copied.
printf "$startPattern" "HDFS Get"
hdfs dfs -get $workingDirectory/testData.csv newTestData.csv
if [ -f "newTestData.csv" ]; then
	echo $success
else
	echo $failure
fi

# Try running the standard SparkPi job in Spark1, and that it gives us a result.
printf "$startPattern" "Spark SparkPi"
# Even though this is the default, we set this here in case it was set before this script was run, to ensure
# we properly test Spark1 execution.
export SPARK_MAJOR_VERSION="1"
sparkResults=$(spark-submit --master yarn --class org.apache.spark.examples.SparkPi spark-examples.jar 2> /dev/null)
if [[ $sparkResults = *"Pi is roughly 3."* ]]; then
	echo $success
else
	echo $failure
fi

# Try running the same SparkPi job in Spark2, and ensure that it gives us a result.
printf "$startPattern" "Spark2 SparkPi"
export SPARK_MAJOR_VERSION="2"
sparkResults=$(spark-submit --master yarn --class org.apache.spark.examples.SparkPi spark2-examples.jar 2> /dev/null)
if [[ $sparkResults = *"Pi is roughly 3."* ]]; then
	echo $success
else
	echo $failure
fi

# Make sure the current user has a sandbox, since we are going to use that for our testing.
sandboxName=$user"_sandbox"
folderName=$sandboxName".db"
connectionString="$CONNECTIONSTRING;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;auth-kerberos"
beeline -u $connectionString -e "CREATE DATABASE IF NOT EXISTS $sandboxName LOCATION '/user/$user/$folderName';" > /dev/null 2> /dev/null

# Using Hive Binary mode, ensure that we can create a table in the user's sandbox.
printf "$startPattern" "Hive Binary Create Table"
tableName="checkenginelight_hivebinary"
databaseName=$user"_sandbox"
connectionString="$CONNECTIONSTRING$databaseName;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;auth-kerberos"
hdfs dfs -chmod 777 /tmp/checkEngineLight
hdfs dfs -chmod 777 /tmp/checkEngineLight/*
beeline -u $connectionString -e "CREATE EXTERNAL TABLE $tableName ( sqrt INT, square INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '$workingDirectory'" > /dev/null 2> /dev/null
showTables=$(beeline -u $connectionString -e "SHOW TABLES" 2> /dev/null)
if [[ $showTables = *"$tableName"* ]]; then
	echo $success
else
	echo $failure
fi

# Using Hive Binary mode, ensure that we can get data from a table in the user's sandbox.
printf "$startPattern" "Hive Binary Select Table"
selectResults=$(beeline -u $connectionString -e "SELECT COUNT(*), SUM(square), SUM(sqrt) FROM $tableName;" 2> /dev/null | grep "10[ ]*\|[ ]*385[ ]\|[ ]*55" | wc -l)
if [ $selectResults -eq "1" ]; then
	echo $success
else
	echo $failure
fi
beeline -u $connectionString -e "DROP TABLE IF EXISTS $tableName" > /dev/null 2> /dev/null

# Using Hive2 Binary mode, ensure that we can create a table in the user's sandbox.
# TODO: Due to DoAs issues, we can't run Hive2 on the sandbox database like we should. Otherwise we'll get permissions errors. Thus we are running this on default database.
printf "$startPattern" "Hive2 Binary Create Table"
tableName="checkenginelight_hive2binary"
databaseName="default"
connectionString="$CONNECTIONSTRING$databaseName;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2;auth-kerberos"
beeline -u $connectionString -e "CREATE EXTERNAL TABLE $tableName ( sqrt INT, square INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '$workingDirectory'" > /dev/null 2> /dev/null
showTables=$(beeline -u $connectionString -e "SHOW TABLES" 2> /dev/null)
if [[ $showTables = *"$tableName"* ]]; then
	echo $success
else
	echo $failure
fi

# Using Hive2 Binary mode, ensure that we can get data from a table in the user's sandbox.
printf "$startPattern" "Hive2 Binary Select Table"
selectResults=$(beeline -u $connectionString -e "SELECT COUNT(*), SUM(square), SUM(sqrt) FROM $tableName;" 2> /dev/null | grep "10[ ]*\|[ ]*385[ ]\|[ ]*55" | wc -l)
if [ $selectResults -eq "1" ]; then
	echo $success
else
	echo $failure
fi
beeline -u $connectionString -e "DROP TABLE IF EXISTS $tableName" > /dev/null 2> /dev/null

# Using Hive HTTP mode, ensure that we can create a table in the user's sandbox.
printf "$startPattern" "Hive HTTP Create Table"
tableName="checkenginelight_hive2http"
databaseName=$user"_sandbox"
connectionString="$CONNECTIONSTRING$databaseName;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-http;auth-kerberos;transportMode=http;httpPath=cliservice"
beeline -u $connectionString -e "CREATE EXTERNAL TABLE $tableName ( sqrt INT, square INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '$workingDirectory'" > /dev/null 2> /dev/null
showTables=$(beeline -u $connectionString -e "SHOW TABLES" 2> /dev/null)
if [[ $showTables = *"$tableName"* ]]; then
        echo $success
else
        echo $failure
fi

# Using Hive Binary mode, ensure that we can get data from a table in the user's sandbox.
printf "$startPattern" "Hive HTTP Select Table"
selectResults=$(beeline -u $connectionString -e "SELECT COUNT(*), SUM(square), SUM(sqrt) FROM $tableName;" 2> /dev/null | grep "10[ ]*\|[ ]*385[ ]\|[ ]*55" | wc -l)
if [ $selectResults -eq "1" ]; then
        echo $success
else
        echo $failure
fi
beeline -u $connectionString -e "DROP TABLE IF EXISTS $tableName" > /dev/null 2> /dev/null

# Ensure that we can create a collection in Solr with a basic schema.
printf "$startPattern" "Solr Create Collection"
solrHost=$(echo $SOLRHOSTS | grep -i -o "^[^,]*")
/opt/lucidworks-hdpsearch/solr/bin/solr create -c checkEngineLight -shards 1 -replicationFactor 1 -p 8983 > /dev/null 2> /dev/null
createResults=$(curl --negotiate -u : "http://$solrHost:8983/solr/admin/collections?action=LIST" 2> /dev/null | grep -o checkEngineLight | wc -l)
if [ $createResults -eq "1" ]; then
	echo $success
else
	echo $failure
fi

# Ensure that we can search a collection we just created in Solr
printf "$startPattern" "Solr Search Collection"
curl --negotiate -u : "http://$solrHost:8983/solr/checkEngineLight/update?commit=true" --data-binary @indexableData.json -H "Content-Type: text/json" > /dev/null 2> /dev/null
searchResults=$(curl --negotiate -u : "http://$solrHost:8983/solr/checkEngineLight/select?q=*%3A*&wt=json&indent=true" 2> /dev/null | grep '"numFound":5' | wc -l)
if [ $searchResults -eq "1" ]; then
	echo $success
else
	echo $failure
fi

# Ensure that we can run an Oozie workflow which is just going to create a directory in HDFS, which we will then test for.
printf "$startPattern" "Oozie Workflow"
printf "nameNode=$hdfsPath\nworkflowAppUri=$hdfsPath/tmp/checkEngineLight\noozie.wf.application.path=$workingDirectory/\nqueueName=batch\njobTracker=$RESOURCEMANAGER:8050\ntouchDirectory=$workingDirectory/oozie" > job.properties
hdfs dfs -put workflow.xml $workingDirectory/
# Start the workflow, and grab the Oozie workflow name, which will be the second "word" in the output.
wfName=$(oozie job -oozie http://$OOZIEHOST:11000/oozie -run -config job.properties 2> /dev/null | awk '{ print $2 }')
isRunning="1"
while [ $isRunning -eq "1" ]
do
	sleep 5
	isRunning=$(oozie jobs -oozie http://$OOZIEHOST:11000/oozie -filter status=RUNNING\;status=PREP -jobtype wf 2> /dev/null | grep $wfName | wc -l)
done
oozieOutput=$(hdfs dfs -ls $workingDirectory 2> /dev/null | grep oozie | wc -l)
if [ $oozieOutput -eq "1" ]; then
	echo $success
else
	echo $failure
fi

# Run the MapREduce TeraSuite, to ensure that MapReduce is running properly. TeraSuite is normally used for benchmarking and performance testing, so this takes a bit longer.
printf "$startPattern" "MapReduce TeraSuite"
pattern="Job job_[0-9_]* completed successfully"
teragenOut=$(hadoop jar hadoop-mapreduce-examples.jar teragen 1000000 $workingDirectory/teragen 2>&1 | grep "$pattern" | wc -l)
terasortOut=$(hadoop jar hadoop-mapreduce-examples.jar terasort $workingDirectory/teragen $workingDirectory/terasort 2>&1 | grep "$pattern" | wc -l)
teraValidateOut=$(hadoop jar hadoop-mapreduce-examples.jar teravalidate $workingDirectory/terasort $workingDirectory/teravalidate 2>&1 | grep "$pattern" | wc -l)
fileOutput=$(hdfs dfs -cat $workingDirectory/teravalidate/part* 2> /dev/null | grep "checksum" | wc -l)

if [ $fileOutput -eq "1" ] && [ $teragenOut -eq "1" ] && [ $terasortOut -eq "1" ] && [ $teraValidateOut -eq "1" ]; then
	echo $success
else
	echo $failure
fi

# Delete random files and directories and such that have been created during the tests, so that they won't be there the next time we run.
echo ""
echo "Cleaning Up"
/opt/lucidworks-hdpsearch/solr/bin/solr delete -c checkEngineLight -p 8983 > /dev/null 2> /dev/null
hdfs dfs -rm -r $workingDirectory 2> /dev/null
rm -rf job.properties
rm -rf testData.csv
rm -rf newTestData.csv
