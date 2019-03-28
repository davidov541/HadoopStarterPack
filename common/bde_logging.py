###########################################################################################
# Name              : bde_logging.py                                                      #
# Description       : This script logs the run of a workflow which modified a hive table, #
#                     including times, rows modified, and output of the job.              #
# Created By        : Vijetha Tummala x538410                                             #
# Last Modified By  : David McGinnis x353324                                              #
# Created Date      : 01/14/2018                                                          #
# Last Modified Date: 08/20/2018                                                          #
###########################################################################################

# Importing necessary packages and modules

import sys
from datetime import datetime
import os
import subprocess
import time
from dateutil.parser import parse
from pyspark.sql import SparkSession

# Argumeints passed to the script

# Main function - This function calls another function and exists if received result is a positivi non-zero number, which means error occured during # the process execution.
def main():
	platform = sys.argv[1]
	application = sys.argv[2]
	success = sys.argv[3]
	tableNames = sys.argv[4]
        startTime = sys.argv[5]
	endTime = sys.argv[6]
	workflowId = sys.argv[7]
	yarnId = sys.argv[8]
	stdout = sys.argv[9]
	stderr = sys.argv[10]
	loggingTable = "bde_stats.wrkflw_evnt_lg"
	if len(sys.argv) >= 12:
		loggingTable = sys.argv[11];

	timezone = time.tzname[0]

	if len(tableNames) > 0:
		spark = SparkSession.builder.appName("Logging Process for " + platform + " - " + application).enableHiveSupport().getOrCreate()
		try:
			tables = tableNames.split(",")
			for tableName in tables:
				#Inserting log record into the hive table
				rowsChanged = getRowsChanged(tableName, startTime, endTime, spark)
				values = [ (str(tableName), True), (str(startTime), True), (str(endTime), True), (str(success), False), (str(rowsChanged), False), (workflowId, True), (yarnId, True), (stdout, True), (stderr, True) ]
				valuesStr = ", ".join(map(lambda x: formatString(x[0], x[1]), values))
				query = "insert into table " + loggingTable + " partition( pltfrm_nm = " + formatString(platform, True) + ", aplctn_nm = " + formatString(application, True) + ") values (" + valuesStr + ")"
				spark.sql(query)
		except :
			print("Exception occured during logging")
			raise

# Retrieves the number of rows in the given table which changed between the given timestamps.
# This function assumes to modifications have been made to the table since the times listed,
# otherwise it may undercount the number of rows affected.
def getRowsChanged(tableName, startTime, endTime, spark):
	pattern = "yyyy-MM-dd HH:mm:ss"
	query = "SELECT updt_ts FROM " + tableName + " WHERE unix_timestamp(updt_ts, '" + pattern + "') > unix_timestamp('" + startTime + "', '" + pattern + "') AND unix_timestamp(updt_ts, '" + pattern + "') < unix_timestamp('" + endTime + "', '" + pattern + "')"
	try:
		numRowsChanged = spark.sql(query).count()
	except:
		numRowsChanged = 0

	return numRowsChanged

# Formats a given string in order to be passed into Hive. This includes removing troublesome characters, and quoting the string, if required.
def formatString(string, shouldQuote):
	# We need to escape this many times since we go through a few different systems that require escaping, such as:
	#   - Python
	#   - Beeline
	#   - Hive (for single quotes)
	# Thus these 8 slashes will turn into just 1 when it is inserted.
	escapeCharacter = "\\\\"
	if (shouldQuote):
		return "'" + string.replace("\n", escapeCharacter + "n").replace("\r", escapeCharacter + "r").replace("'", " ").replace("\"",  " ").replace(";", " ").replace("$", "\\$") + "'"
	else:
		return string

if __name__ == "__main__":
	main()
