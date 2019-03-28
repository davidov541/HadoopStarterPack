import sys
import subprocess
from subprocess import call
from pyspark.sql import SparkSession
from subprocess import call

tableInfos = sys.argv[1]
savePath = sys.argv[2]
database = sys.argv[3]

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

for tableInfo in tableInfos.split("~"):
	if len(tableInfo) > 0:
		firstSplit = tableInfo.find("|")
		secondSplit = tableInfo.rfind("|")
		tableName = tableInfo[:firstSplit]
		fields = tableInfo[firstSplit+1:secondSplit]
		condition = tableInfo[secondSplit+1:]
		if len(condition) == 0:
			query = "SELECT %s FROM %s.%s" % (fields, database, tableName)
		else:
			query = "SELECT %s FROM %s.%s WHERE %s" % (fields, database, tableName, condition)
		actual = spark.sql(query)
		actual.coalesce(1).write.json(savePath + "/" + tableName, mode="overwrite")
