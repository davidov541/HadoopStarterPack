#!/bin/bash

email=$1
useSandbox=$2

hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
. environmentVars.sh
rm environmentVars.sh

if [ "$useSandbox" == "true" ]; then
        user=$(klist | sed -rn 's/Default principal: (.*)@DAVIDMCGINNIS.NET/\1/p' | tr '[:upper:]' '[:lower:]')
	projectsRoot="hdfs:///user/$user/application/projects"
	dataRoot="hdfs:///user/$user/application/data/application/regressionTestResults"
	keytab="hdfs:///user/$user/$user.keytab"
elif [ "$useSandbox" == "false" ]; then
	kinit -kt /etc/security/keytabs/applicationUser.keytab applicationUser
	user=$(klist | sed -rn 's/Default principal: (.*)@DAVIDMCGINNIS.NET/\1/p' | tr '[:upper:]' '[:lower:]')
        projectsRoot="hdfs:///projects/application"
	dataRoot="hdfs:///data/application/regressionTestResults"
	keytab="hdfs:///projects/application/lib/applicationUser.keytab"
else
	echo "No indicator on whether to use the user's sandbox or not. Please make sure you are passing in two arguments to the script!"
	exit 1
fi
oozie job -oozie http://$OOZIEHOST:11000/oozie -run -config scripts/remoteTests.properties -D keytab=$keytab -D email=$email -D useSandbox=$useSandbox -D projectsRoot=$projectsRoot -D nameNode=hdfs://$NAMENODE/ -D jobTracker=$RESOURCEMANAGER:8050 -D env=$ENV -D user=$user

echo "Workflow has started. Results will be posted as *.out files in $dataRoot and you will be emailed once all tests have completed."
