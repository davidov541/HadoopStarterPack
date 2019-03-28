from abc import ABCMeta
from copy import deepcopy
from os import path
import csv
import string
import re
import subprocess
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,StructField,StructType

class FileIngester:
	# This makes this class an abstract base class. 
	# This ensures that no one accidentally creates an instance of this class, since it is not the way to work with the infrastructure.
	# See https://docs.python.org/2/library/abc.html for more information.
	__metaclass__ = ABCMeta

	def setCurrentPath(self, currentPath):
		self._currentPath = currentPath

	def getCurrentPath(self):
		return self._currentPath

	def processFile(self, spark):
        	fields = [StructField(name,StringType(),True) for name in self._columns]
	        schema = StructType(fields)
	
        	rawData = spark.read.csv(self._currentPath, schema=schema, sep=self._separator, header=True, mode="FAILFAST", quote='')
	
        	rawData.cache().registerTempTable(self._tableName + "_RAW")
	        currentUser = spark.sparkContext.sparkUser()
	
        	self._currentData = spark.sql(self._query % (currentUser, currentUser))

	def ingestFile(self, spark, databaseName, archiveFolder):
        	self._currentData.write.insertInto(databaseName + "." + self._tableName)
	        cmd = "/usr/bin/hdfs dfs -mv " + self._currentPath + " " + archiveFolder + "/"
        	subprocess.call(cmd, shell=True)
	        cmd = "/usr/bin/hdfs dfs -rm " + self._currentPath[:-4] + "*"
        	subprocess.call(cmd, shell=True)
	
	def moveFilesToFailure(self, failureFolder):
        	cmd = "/usr/bin/hdfs dfs -mv " + self._currentPath + " " + failureFolder + "/"
	        subprocess.call(cmd, shell=True)
	        cmd = "/usr/bin/hdfs dfs -rm " + self._currentPath[:-4] + "*"
        	subprocess.call(cmd, shell=True)

	def canProcessFile(self, filePath):
		pattern = re.compile('.*' + self._pattern + '.*')
		return pattern.match(filePath) is not None

	def checkForErrors(self):
		# Using .cache().count() forces the dataframes to be evaluated, thus ensuring that any issues in these dataframes are found now instead of later.
                # This makes sure that we don't insert into one table but not the other two, whenever possible.
                print "Document Count from " + self._currentPath + ": " + str(self._currentData.cache().count())
		
	def getExtension(self):
		return self._extension

	def checkForNullKeys(self):
		filterQuery = ''
                for key in self._keys:
                	if len(filterQuery) > 0:
                        	filterQuery += " OR "
                        filterQuery += "%s IS NULL OR %s = ''" % (key, key)
		numNullDocuments = self._currentData.filter(filterQuery).count()
		if numNullDocuments > 0:
                	raise ValueError("A key for one of the documents in %s is null. This is the unique ID, so it can't be null!" % self._currentPath)

FileIngester.register(tuple)

def _getFieldArray(line, sep, serializedColumns):
	splitted = csv.reader([line], delimiter=sep).next()
	dictionary = dict()
	columns = serializedColumns.split(',')
	if len(splitted) != len(columns):
		raise ValueError("Incorrect number of columns given in some of the data, assuming this file is corrupt!")

	for i in range(len(splitted)):
		dictionary[columns[i]] = splitted[i]

	return Row(**dictionary)

class QuotedFileIngester(FileIngester):
        # This makes this class an abstract base class.
        # This ensures that no one accidentally creates an instance of this class, since it is not the way to work with the infrastructure.
        # See https://docs.python.org/2/library/abc.html for more information.
        __metaclass__ = ABCMeta

        def processFile(self, spark):
		rawData = spark.read.text(self._currentPath).filter("lower(value) NOT LIKE '%" + self._columns[0].lower() + "%'")
	
		fields = [StructField(names,StringType(),True) for names in self._columns]
		schema = StructType(fields)
		serializedColumns = ','.join(self._columns)
		filtered = spark.createDataFrame(rawData.rdd.map(lambda x: _getFieldArray(x[0], self._separator, serializedColumns)), schema)

                filtered.cache().registerTempTable(self._tableName + "_RAW")
                currentUser = spark.sparkContext.sparkUser()

                self._currentData = spark.sql(self._query % (currentUser, currentUser))

QuotedFileIngester.register(tuple)

def _getVariableFieldArray(line, sep, serializedColumns):
        splitted = csv.reader([line], delimiter=sep, quoting=csv.QUOTE_NONE).next()
        dictionary = dict()
        columns = serializedColumns.split(',')

	if len(splitted) < len(columns):
		raise ValueError("There aren't enough fields in the input file for the number of columns given!")	

	# Set all of the columns before the last one to the corresponding dictionary key.
        for i in range(len(splitted) - 2):
                dictionary[columns[i]] = splitted[i]

	dictionary[columns[-1]] = string.join(splitted[len(columns) - 1:], sep)

        return Row(**dictionary)

# This class processes a delimited file which may have delimiters in the fields themselves.
# While this is not recommended, due to the flimsyness of this approach, this is necessary in some places.
# The first len(self._columns) - 1 fields are assigned to the appropriate column, and then the last column gets everything after that.
class VariableFileIngester(FileIngester):
        # This makes this class an abstract base class.
        # This ensures that no one accidentally creates an instance of this class, since it is not the way to work with the infrastructure.
        # See https://docs.python.org/2/library/abc.html for more information.
        __metaclass__ = ABCMeta

        def processFile(self, spark):
                rawData = spark.read.text(self._currentPath).filter("lower(value) NOT LIKE '%" + self._columns[0].lower() + "%'")

                fields = [StructField(names,StringType(),True) for names in self._columns]
                schema = StructType(fields)
                serializedColumns = ','.join(self._columns)
                filtered = spark.createDataFrame(rawData.rdd.map(lambda x: _getVariableFieldArray(x[0], self._separator, serializedColumns)), schema)

                filtered.cache().registerTempTable(self._tableName + "_RAW")
                currentUser = spark.sparkContext.sparkUser()

                self._currentData = spark.sql(self._query % (currentUser, currentUser))

VariableFileIngester.register(tuple)

class FileClassIngester:
	def __init__(self, *children):
		self._children = list(children)

	def processFileClass(self, spark, topDirectory, databaseName, landingFolder='landing', failureFolder='failure', archiveFolder='archive'):
        	landingDirectory = topDirectory + "/" + landingFolder
	        cmd = "/usr/bin/hdfs dfs -ls " + landingDirectory + "/*.* | awk -F'/' '{print $NF}'| tr '\n' '\n'"
        	files = subprocess.check_output(cmd, shell=True).strip().split('\n')
	        doneFiles = [file for file in files if "_DONE" in file]

		filesToProcess = []
		for child in self._children:
			childrenIngesters = []
			for filePath in files:
				if "_DONE" not in filePath and child.canProcessFile(filePath):
					childIngester = deepcopy(child)
					childIngester.setCurrentPath(landingDirectory + "/" + filePath)
					childrenIngesters.append(childIngester)
			filesToProcess.append(childrenIngesters)

		numFailures = 0

		if len(set([len(files) for files in filesToProcess])) > 1:
			raise ValueError("There are an imbalanced number of dependent input files in the landing space. Please balance these so that the system can properly coorelate files with each other.")

		if len(filesToProcess) == 0:
			return 0

        	for index in range(len(filesToProcess[0])):
			childrenToProcess = []
			for child in filesToProcess:
				childrenToProcess.append(child[index])

			relatedDoneFiles = [path.basename(child.getCurrentPath())[:-4] + "_DONE." + child.getExtension() for child in childrenToProcess]
			if all(doneFile in doneFiles for doneFile in relatedDoneFiles):
                        	try:
					for child in childrenToProcess:
	                                	child.processFile(spark)
	
					for child in childrenToProcess:
						child.checkForErrors()

					for child in childrenToProcess:
						child.checkForNullKeys()
	
					for child in childrenToProcess:
        	        	               	child.ingestFile(spark, databaseName, topDirectory + "/" + archiveFolder + "/")
                	        except Exception as e:
                        	        numFailures += len(childrenToProcess)
					for child in childrenToProcess:
                                		child.moveFilesToFailure(topDirectory + "/" + failureFolder + "/")
                                	print "Error occurred! Details:\n%s" % str(e)
	
		return numFailures
