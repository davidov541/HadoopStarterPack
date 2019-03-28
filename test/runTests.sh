#!/bin/bash
function writeOutput {
	output=$1
	user=$2
	if [ "$useSandbox" == "true" ]; then
        	outputLocation="hdfs:///user/$user/application/data/regressionTestResults/$outputFileName.out"
	else
        	outputLocation="hdfs:///data/application/regressionTestResults/$outputFileName.out"
	fi

	echo "$output"
	echo "$output" | hdfs dfs -appendToFile - $outputLocation
}

function runTestPart {
	test=$1
	useSandbox=$2
	part=$3
	earliestDate=$4

	testPath=$test/$part
	if [[ -z "$part" ]]; then
		testPath=$test
	fi

	if [ "$useSandbox" == "true" ]; then
        	user=$(klist | sed -rn 's/Default principal: (.*)@DAVIDMCGINNIS.NET/\1/p' | tr '[:upper:]' '[:lower:]')
        	dbName=$user"_sandbox"
		dataRoot="/user/$user/application/data"
		actualLocationHDFS="hdfs:///user/$user/application/data/regressionTestResults/$testPath"
	else
        	user="applicationUser"
        	dbName="application"
		dataRoot="/data"
		actualLocationHDFS="hdfs:///data/application/regressionTestResults/$testPath"
		kinit -kt /etc/security/keytabs/applicationUser.keytab applicationUser
	fi

	hdfs dfs -put -f testData/$testPath/input/* $dataRoot/

	for workflow in $(cat testData/$testPath/workflows); do
        	oozieOutput=$(oozie job -oozie http://$OOZIEHOST:11000/oozie -run -config oozie/$workflow)
        	wfName=$(echo $oozieOutput | awk '{ print $2 }')
        	writeOutput "Started Workflow From $workflow. ID: $wfName" $user
        	isRunning="1"
		while [ $isRunning -eq "1" ]
        	do
                	sleep 5
                	isRunning=$(oozie jobs -oozie http://$OOZIEHOST:11000/oozie -filter status=RUNNING\;status=PREP -jobtype wf 2> /dev/null | grep $wfName | wc -l)
        	done
	done

	writeOutput "Checking Results..." $user
      	export SPARK_MAJOR_VERSION=2
	actualLocationLocal="testData/$testPath/actual/"
	expectedLocationLocal="testData/$testPath/expected/"
	hdfs dfs -mkdir $actualLocationHDFS > /dev/null 2> /dev/null
	hdfs dfs -rm -r $actualLocationHDFS/* > /dev/null 2> /dev/null
	
	tables=$(cat testData/$test/*.list | paste -sd '~' -)
	spark-submit --master yarn --deploy-mode cluster --name "Automated Testing for $testPath" --executor-memory 1G --num-executors 4 scripts/processTestOutput.py "$tables" $actualLocationHDFS $dbName > /dev/null 2> /dev/null
	
	mkdir $actualLocationLocal > /dev/null 2> /dev/null
	rm -rf $actualLocationLocal/* > /dev/null 2> /dev/null
	hdfs dfs -get $actualLocationHDFS/* $actualLocationLocal/
	
	rm -rf $actualLocationLocal/*/_SUCCESS

	startPattern="%-50s - %s"
	
	for tableName in $(cat testData/$test/*.list | sed -rn 's/([^|]*).*/\1/p'); do
	        sort $actualLocationLocal/$tableName/*  | tr '[:upper:]' '[:lower:]' > actual
		sort $expectedLocationLocal/$tableName/*  | tr '[:upper:]' '[:lower:]' > expected
		differences=$(diff -Bw actual expected | wc -l)
	        if [ "$differences" -eq "0" ]; then
			output=$(printf "$startPattern" $tableName "SUCCESS")
	        else
			output=$(printf "$startPattern" $tableName "FAILURE")
	        fi
		writeOutput "$output" $user
		rm actual
		rm expected
	done

	for collectionInfo in $(cat testData/$test/collections); do
		collection=$(echo $collectionInfo | sed 's/,.*//g')
		query=$(echo $collectionInfo | sed 's/.*,//g')
		mkdir $actualLocationLocal/$collection
		if [ "$useSandbox" == "true" ]; then
			collectionName=$user"_"$collection
		else
			collectionName=$collection
		fi
		solrHost=$(echo $SOLRHOSTS | grep -i -o "^[^,]*")
		curl --negotiate -u : "http://$solrHost:8983/solr/$collectionName/$query" > $actualLocationLocal/$collection/output.json 2> /dev/null
		hdfs dfs -mkdir $actualLocationHDFS/$collection
		hdfs dfs -put -f $actualLocationLocal/$collection/output.json $actualLocationHDFS/$collection/
		differences=$(diff -Bw $actualLocationLocal/$collection/* $expectedLocationLocal/$collection/* | grep --invert-match "version" | grep --invert-match "QTime" | grep [\<\>] | wc -l)
		if [ "$differences" -eq "0" ]; then
			output=$(printf "$startPattern" $collection "SUCCESS")
		else
			output=$(printf "$startPattern" $collection "FAILURE")
		fi
		writeOutput "$output" $user
	done

	# Delete any files that were left over from the last test. These may be purposefully left or not, but their presence could cause failures.
	dataRootEscaped=$(echo $dataRoot | sed 's/\//\\\//g')
	filesToDelete=$(find testData/$testPath/input/* -type f | sed "s/.*input\//$dataRootEscaped/p" | tr '\n' ' ')
	hdfs dfs -rm -f $filesToDelete > /dev/null 2> /dev/null
}

function runTest {
	testPath=$1
	useSandbox=$2

	if [[ -z "$useSandbox" ]]; then
        	useSandbox="true"
	fi

	if [ "$useSandbox" == "true" ]; then
		user=$(klist | sed -rn 's/Default principal: (.*)@DAVIDMCGINNIS.NET/\1/p' | tr '[:upper:]' '[:lower:]')
	else
		user="x981138"
	fi

	earliestDate="`date +'%Y-%m-%d %H:%M:%S.0'`"
        testName=$(echo $testPath | sed -rn 's/testData\/(.*)/\1/p')
	for part in "${parts[@]}"; do
                if [[ -z "$part" ]]; then
                        writeOutput "Running Test $testName" $user
                else
                        writeOutput "Running Test $testName on Part $part" $user
                fi
                runTestPart $testName $useSandbox "$part" "$earliestDate"
        done
}

testType=$1
testName=$2
useSandbox=$(echo $3 | tr '[:upper:]' '[:lower:]')

hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
. environmentVars.sh
rm environmentVars.sh

if [ "$testType" == "--test" ]; then
	isPart=$(echo $testName | grep --only-matching "/" | wc -l)
	if [ "$isPart" == 0 ]; then
	        testPath=$(ls -d testData/*/$testName)
        	outputFileName=$testName
	        if [ -d "$testPath/input" ]; then
			parts=("")
        	else
			parts=()
			for partPath in $testPath/*/; do
				part=$(basename $partPath)
				parts+=($part)
			done
        	fi
		runTest $testPath $useSandbox
	else
		testShortName=$(echo $testName | grep --only-matching "^[^/]*")
		testPath=$(ls -d testData/*/$testShortName)
		outputFileName=$testName
		part=$(basename $testName)
		parts+=($part)
		runTest $testPath $useSandbox
	fi
elif [ "$testType" == "--domain" ]; then
	outputFileName=$testName
	for testPath in testData/$testName/*/; do
		if [ -d "$testPath/input" ]; then
			parts=("")
		else
                        parts=()
                        for partPath in $testPath/*/; do
                                part=$(basename $partPath)
                                parts+=($part)
                        done
                fi
		runTest $testPath $useSandbox
	done
elif [ "$testType" == "--all" ]; then
	useSandbox=$(echo $testName | tr '[:upper:]' '[:lower:]')
        outputFileName="all"
	for testPath in testData/*/*/; do
		if [ -d "$testPath/input" ]; then
			parts=("")
		else
                	parts=()
                        for partPath in $testPath/*/; do
                                part=$(basename $partPath)
                                parts+=($part)
                        done
		fi
		runTest $testPath $useSandbox
	done
fi
