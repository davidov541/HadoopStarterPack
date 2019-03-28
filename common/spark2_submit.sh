#!/bin/bash

file=$1
extraParams=$2
keytab=$3
principal=$4
platform=$5
if [[ -z "$platform" ]]; then
        platform=Unknown
fi
app=$6
if [[ -z "$app" ]]; then
        app=Unknown
fi
workflowId=$7
if [[ -z "$workflowId" ]]; then
        workflowId=Unknown
fi
tableNames=$8
applicationParameters=$9
environmentPath=${10}
loggingTable=${11}
if [[ -z "$loggingTable" ]]; then
	loggingTable=bde_stats.wrkflw_evnt_lg
fi
loggingScriptPath=${12}
if [[ -z "$loggingScriptPath" ]]; then
	loggingScriptPath=hdfs:///projects/common
fi
if [[ -z "$environmentPath" ]]; then
	environmentPath=$loggingScriptPath/envs.zip
fi

mkdir envs
hdfs dfs -get $environmentPath envs/envs.zip
pushd envs
unzip envs.zip
popd

export SPARK_MAJOR_VERSION=2
export PYTHONPATH=`pwd`
export PYSPARK_PYTHON=./envs/bin/python
start=$(date +"%Y-%m-%d %H:%M:%S")
spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/bin/python --archives envs/envs.zip#envs --keytab $keytab --principal $principal $extraParams $file $applicationParameters > stdout 2> stderr
result=$?
end=$(date +"%Y-%m-%d %H:%M:%S")
[[ $result -eq 0 ]] && resultStr="True" || resultStr="False"
stdout=$(cat stdout)
stderr=$(cat stderr)
yarnID=$(echo $stderr | grep --only-matching "Submitting application .* to ResourceManager" | sed -rne 's/Submitting application ([^ ]*) to ResourceManager/\1/gip')
if [[ -z "$yarnID" ]]; then
	yarnID="Unavailable"
fi
hdfs dfs -get $loggingScriptPath/bde_logging.py .
spark-submit --master yarn --deploy-mode client --keytab $keytab --principal $principal bde_logging.py $platform $app $resultStr "$tableNames" "$start" "$end" $workflowId $yarnID "$stdout" "$stderr" $loggingTable > stdout 2> stderr
if [[ $? -ne 0 ]]; then
	cat stdout
	cat stderr
fi
exit $result
