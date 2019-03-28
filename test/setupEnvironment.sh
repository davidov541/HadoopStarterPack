#!/bin/bash

modifierFlag=$(echo $1 | tr '[:upper:]' '[:lower:]')

hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
. ./environmentVars.sh
rm environmentVars.sh

user=$(klist | sed -rn 's/Default principal: (.*)@DAVIDMCGINNIS.NET/\1/p' | tr '[:upper:]' '[:lower:]')
sandboxName=$user"_sandbox"
connectionString="$CONNECTIONSTRING$databaseName;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;auth-kerberos"
env=$user"Dev"
dataRoot="/user/$user/application/data"

rm -r testData/*/*/actual
rm -r testData/*/*/*/actual

if [ "$user" == "applicationUser" ]; then
	echo "ERROR -- You should not create a PDE for the service user. Instead, either run the ./runRegressionTests.sh script, or kinit as your personal account and run again"
	exit
fi

echo "Setting up HDFS."
hdfs dfs -rm -f -r /user/$user/application
hdfs dfs -mkdir /user/$user/application
hdfs dfs -mkdir /user/$user/application/projects
hdfs dfs -mkdir $dataRoot
hdfs dfs -put ../hdfs/* /user/$user/application/projects/
hdfs dfs -mkdir $dataRoot/regressionTestResults

pushd ../ddl

if [ "$modifierFlag" == "--recreateddls" ]; then
	for ddl in $(ls *table*.hql); do
        	echo "Running $ddl to set up Hive tables and views."
		beeline -u $connectionString -f $ddl --hivevar dbName=$sandboxName --hivevar dataRoot=$dataRoot
		if [ $? -ne 0 ]; then
        		exit
		fi
	done

	for ddl in $(ls *views.hql); do
        	echo "Running $ddl to set up Hive tables and views."
	        beeline -u $connectionString -f $ddl --hivevar dbName=$sandboxName --hivevar dataRoot=$dataRoot
        	if [ $? -ne 0 ]; then
               		exit
	        fi
	done
fi

for ddl in $(ls *table*.hql); do
        tableLocations=$(cat $ddl | grep LOCATION | sed -rn 's/.*LOCATION .*\$\{dataRoot\}\/([a-zA-Z\/0-9_]*).*/\1/p')
        for tableLocation in $tableLocations; do
		hdfs dfs -mkdir -p $dataRoot/$tableLocation
	done
done

popd

pushd scripts
scriptFile='setupFakeVINs.hql'
beeline -u $connectionString -f $scriptFile --hivevar dbName=$sandboxName --hivevar updatedTimestamp=$(date +"%m-%d-%Y %H:%M:%S") --hivevar tableName=application.vhcl
popd

pushd ../ddl
echo "Creating the Solr collections based on the shell scripts in the DDL folder."
for script in $(ls *.sh); do
	collectionSuffix=$(echo $script | sed -rn 's/create_([^\.]*).sh/_\1/p')
	collectionName=$user$collectionSuffix
	echo "Creating $collectionName"
	chmod +x $script
	./$script $collectionName
done
popd

echo "Creating property files which use the custom parameters in order to run tests in your environment."
rm -rf oozie/
mkdir oozie
for file in ../*/*.properties; do
	isBundle=$(grep bundle $file | wc -l)
	fileName=$(echo "$file" | grep --only-matching "[^/]*\.properties")
	if [ $isBundle -eq "0" ]; then
		cp $file oozie/
		sed -i -e 's/oozie\.coord\.application\.path/oozie\.wf\.application\.path/g' oozie/$fileName
	else
		# Copy over the whole file as well as the individual portions, in case we want to test the bundle setup.
		cp $file oozie/
		prefix=$(echo $file | sed -rn 's/.*\/(.*)\.properties/\1/p')
		grep WorkflowAppUri $file | while read -r line; do
			qualifier=$(echo $line | sed -rn 's/(.*)WorkflowAppUri.*/\1/p');
			fileName="$prefix$qualifier"".properties";
			cp $file oozie/$fileName
			workflowVarName=$qualifier"WorkflowAppUri"
			sed -i -e 's/oozie\.bundle\.application\.path=.*/oozie\.wf\.application\.path=\${'$workflowVarName'}/g' oozie/$fileName
			collectionVariable=$qualifier"CollectionName"
			legacyVariable=$qualifier"LegacyCollectionName"
			collectionName=$user"_\${"$collectionVariable"}"
			legacyCollectionName=$user"_\${"$legacyVariable"}"
			echo "collectionName=$collectionName" >> oozie/$fileName
			echo "legacyCollectionName=$legacyCollectionName" >> oozie/$fileName
		done
	fi
done
sed -i -e "s/\/projects\/application/\/user\/$user\/application\/projects/g" oozie/*
sed -i -e "s/database=.*/database=$sandboxName/g" oozie/*
sed -i -e "s/dataRoot=.*/dataRoot=\$\{nameNode\}\/user\/$user\/application\/data/g" oozie/*
sed -i -e "s/email=.*/email=oozie@master.davidmcginnis.net/g" oozie/*
sed -i -e "s/keytab=.*/keytab=\/user\/$user\/$user\.keytab/g" oozie/*
sed -i -e "s/principal=.*/principal=$user/g" oozie/*
sed -i -e "s/env=.*/env=$env/g" oozie/*

hdfs dfs -put ../test/ /user/$user/application/projects/
