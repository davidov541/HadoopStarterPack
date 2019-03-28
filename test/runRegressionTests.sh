#!/bin/bash
email=$1

kinit -kt /etc/security/keytabs/applicationUser.keytab applicationUser

hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
. environmentVars.sh
rm environmentVars.sh

if [ "$env" != "prd" ]; then
	connectionString="${CONNECTIONSTRING}application;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;auth-kerberos"

	pushd scripts
	chmod +x setupRegressionTests.sh
	./setupRegressionTests.sh
	popd
	
	# Run all tests, checking only data inserted after we started testing.
	chmod +x runAllTests.sh
	./runAllTests.sh "$email" false
fi
