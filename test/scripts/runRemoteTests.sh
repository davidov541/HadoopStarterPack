#!/bin/bash

domainName=$1
useSandbox=$2
principal=$3

kinit -kt $principal.keytab $principal
rm $principal.keytab

if [ "$useSandbox" == "true" ]; then
	actualLocationHDFS="hdfs:///user/$principal/application/projects/test"
else
	actualLocationHDFS="hdfs:///projects/application/test"
fi

hdfs dfs -get $actualLocationHDFS .

pushd test
startTime="`date +'%Y-%m-%d %H:%M:%S.0'`"
chmod +x runTests.sh
./runTests.sh --domain $domainName $useSandbox $startTime
popd
