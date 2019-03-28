#!/bin/bash
useSandbox=$1
principal=$2

hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
. environmentVars.sh
rm -f environmentVars.sh

kinit -kt $principal.keytab $principal
rm -f $principal.keytab

if [ "$useSandbox" == "true" ]; then
        actualLocationHDFS="hdfs:///user/$principal/application/data/regressionTestResults"
else
        actualLocationHDFS="hdfs:///data/application/regressionTestResults"
fi

if [ "$useSandbox" == "false" ]; then
	connectionString="${CONNECTIONSTRING}application;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;auth-kerberos"
	
        # Remove all of the input files, which should've been moved to the archive folder, in the same folder as actual data to keep.
        removeCommand="hdfs dfs -rm"
	for input in $(hdfs dfs -ls -R /projects/application/test/testData/* | grep -E --only-matching "input/.*\.(txt|csv)" | sed 's/input/\/data/g'); do
		landingFile=$(echo $input | sed 's/landing/*/g'); 
		removeCommand+=" $landingFile";
		failureFile=$(echo $input | sed -r 's/landing.*\/([^\/]*)/failure\/\1/g');
		removeCommand+=" $failureFile";
		archiveFile=$(echo $input | sed -r 's/landing.*\/([^\/]*)/archive\/\1/g');
		removeCommand+=" $archiveFile";
	done;
	eval $removeCommand > /dev/null 2> /dev/null

        # Reset the latest solr refresh so that the collection will be completely reindexed the next time through.
	hdfs dfs -get /projects/application/test/scripts/forceSolrReindex.hql .
        beeline -u $connectionString -f forceSolrReindex.hql --hivevar dbName=application --hivevar dataRoot=/data --hivevar resetTime="1990-01-01 00:00:00" > /dev/null 2> /dev/null
	rm -f forceSolrReindex.hql

        # In all of the tables we checked, go through and delete any data which has been inserted and checked, thus reverting it back.
        touch cleanup.hql
	hdfs dfs -cat /projects/application/test/testData/*/*/tables.list | sort | uniq | while read tableInfo; do
	        conditions=$(echo $tableInfo | grep --only-matching "[^|]*$")
        	table=$(echo $tableInfo | grep --only-matching "^[^|]*")
	        if [[ "$conditions" = "" ]]; then
        	        echo "INSERT OVERWRITE TABLE application.$table SELECT * FROM application.$table WHERE 1 = 0;" >> cleanup.hql
	        else
                	echo "INSERT OVERWRITE TABLE application.$table SELECT * FROM application.$table WHERE NOT (${conditions});" >> cleanup.hql
        	fi
	done

	beeline -u $connectionString -f cleanup.hql --force=true > /dev/null 2> /dev/null
	rm -f cleanup.hql

	# Delete all of the test data from the collections.
        for collectionInfo in $(hdfs dfs -cat /projects/application/test/testData/*/*/collections | sort | uniq); do
        	solrHost=$(echo $SOLRHOSTS | grep -i -o "^[^,]*")
		collection=$(echo $collectionInfo | sed 's/,.*//g')
	        curl --negotiate -u : http://$solrHost:8983/solr/$collection/update?commit=true --data '<delete><query>*:*</query></delete>' -H 'Content-type:text/xml' > /dev/null 2> /dev/null
        done
fi

hdfs dfs -cat $actualLocationHDFS/*.out > allResults.txt
hdfs dfs -put -f allResults.txt $actualLocationHDFS/
echo "resultsPath=$actualLocationHDFS/allResults.txt" 
