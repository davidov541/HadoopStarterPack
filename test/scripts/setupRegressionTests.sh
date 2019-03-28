#!/bin/bash

hdfs dfs -get /projects/common/environmentVars.sh
chmod +x environmentVars.sh
. environmentVars.sh
rm environmentVars.sh

kinit -kt /etc/security/keytabs/X981138.keytab x981138

hdfs dfs -mkdir /data/application/regressionTestResults > /dev/null 2> /dev/null
hdfs dfs -rm -r /data/application/regressionTestResults/* > /dev/null 2> /dev/null
hdfs dfs -rm -r /projects/application/test

echo "Creating property files which use the custom parameters in order to run tests in your environment."
rm -rf ../oozie/
mkdir ../oozie
for file in ../../*/*.properties; do
        isBundle=$(grep bundle $file | wc -l)
        fileName=$(echo "$file" | grep --only-matching "[^/]*\.properties")
        if [ $isBundle -eq "0" ]; then
                cp $file ../oozie/
                sed -i -e 's/oozie\.coord\.application\.path/oozie\.wf\.application\.path/g' ../oozie/$fileName
        else
                # Copy over the whole file as well as the individual portions, in case we want to test the bundle setup.
                cp $file ../oozie/
                prefix=$(echo $file | sed -rn 's/.*\/(.*)\.properties/\1/p')
                grep WorkflowAppUri $file | while read -r line; do
                        qualifier=$(echo $line | sed -rn 's/(.*)WorkflowAppUri.*/\1/p');
                        fileName="$prefix$qualifier"".properties";
                        cp $file ../oozie/$fileName
                        workflowVarName=$qualifier"WorkflowAppUri"
                        sed -i -e 's/oozie\.bundle\.application\.path=.*/oozie\.wf\.application\.path=\${'$workflowVarName'}/g' ../oozie/$fileName
                        collectionVariable=$qualifier"CollectionName"
			collectionName="\${$collectionVariable}"
                        echo "collectionName=$collectionName" >> ../oozie/$fileName
			legacyCollectionVariable=$qualifier"LegacyCollectionName"
			legacyCollectionName="\${$legacyCollectionVariable}"
			echo "legacyCollectionName=$legacyCollectionName" >> ../oozie/$fileName
                done
        fi
done

sed -i -e "s/email=.*/email=oozie@master.davidmcginnis.net/g" ../oozie/*

hdfs dfs -put -f ../../test/ /projects/application/
