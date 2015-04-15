#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013 Battelle Memorial Institute.
# This software is licensed under the Battelle "BSD-style" open source license;
# the full text of that license is available in the COPYING file in the root of the repository
import os
import tarfile
import time
import JobStore
import pymongo
from bson.code import Code
import datetime

class MongoJobStore(JobStore.JobStore):
	def __init__(self, pointStore, mongouri = None):
		self.mongouri = mongouri
		self.pointStore = pointStore
		self.fork()
		super(MongoJobStore, self).__init__(pointStore)

	def fork(self):
		if self.mongouri:
			self.db = pymongo.Connection(self.mongouri).nwperf
		else:
			self.db = pymongo.Connection().nwperf

	def jobProcessed(self):
		return self.db.jobs.find(
			{"id": int(self.job["JobID"]),
			"startTime": datetime.datetime.strptime(self.job["Start"], "%Y-%m-%dT%H:%M:%S")}
			).count()

	def processJob(self, job, additionalFields = {}, forceProcessing = False):
		try:
			if self.pointStore.mongouri != self.mongouri:
				super(MongoJobStore, self).processJob(job, additionalFields, forceProcessing)
		except AttributeError:
			super(MongoJobStore, self).processJob(job, additionalFields, forceProcessing)

		self.job = job
		self.graphs = {}
		self.additionalFields = additionalFields
		if forceProcessing or not self.jobProcessed():
			startTime = time.strptime(job["Start"], "%Y-%m-%dT%H:%M:%S")
			endTime = time.strptime(job["End"], "%Y-%m-%dT%H:%M:%S")

			numBuckets = 200
			bucketSize = job["RunTime"] / 60 / numBuckets

			mapFunction = Code( 	'function() {'
						'	var loc = Math.floor((this.time - Start)/1000/numBuckets);'
						'	var retObj = {};'
						'	var host = this.host;'
						'	this.points.forEach(function(point) {'
						'		var pointTime = {};'
						'		pointTime[loc] = [point[1],1];'
						'		retObj[host] = pointTime;'
						'		emit(point[0], retObj);'
						'	});'
						'}',
						Start=startTime,
						numBuckets=numBuckets)
			reduceFunction = Code(	'function(metric, values) {'
						'	//values = [{"g0": {"0": [sum, count]}}, {"g1": {"1": [sum, count]}}]'
						'	var newObj = {};'
						'	values.forEach(function(subValues) {'
						'		for(var host in subValues) {'
						'			for(var time in subValues[host]) {'
						'				if(!(host in newObj)) {'
						'   		 			newObj[host] = {};'
						'				}'
						'				if(time in newObj[host]) {'
						'   	    	     			newObj[host][time] = ['
						'						subValues[host][time][0] + newObj[host][time][0],'
						'						subValues[host][time][1] + newObj[host][time][1]'
						'					];'
						'   	 			} else {'
						'					newObj[host][time] = subValues[host][time];'
						'				}'
						'			}'
						'		}'
						'	});'
						'	return newObj;'
						'}')
		finalizeFunction = Code(	'function(metric, values) {'
						'	//values = [{"g0": {"0": [sum, count]}}, {"g1": {"1": [sum, count]}}]'
						'	//return {"g0": [[1427302901, 0], [1427302961, 1]], "g1": [[1427302901, 0], [1427302961, 1]]}'
						'	var retDoc = {};'
						'	var i;'
						'	var timePoint;'
						'	for(var host in values) {'
						'		retDoc[host] = [];'
						'		for(i = 0; i < numBuckets; i++) {'
						'			timePoint = [Math.floor(Start.getTime()/1000 + bucketSize * i * 60)];'
						'			if( i in values[host]) {'
						'				timePoint[1] = values[host][i][0]/values[host][i][1];'
						'			} else {'
						'				timePoint[1] = null;'
						'			}'
						'			retDoc[host][i] = timePoint;'
						'		}'
						'	}'
						'	return retDoc;'
						'}',
						Start=startTime,
						numBuckets=numBuckets,
						bucketSize=bucketSize
						)
		graphCollection = self.db.points.map_reduce(	mapFunction,
								reduceFunction,
								"job_%s_graphs" % job["JobID"],
								finalize=finalizeFunction,
								query={"time": {"$gte": startTime, "$lte": endTime}, "host": {"$in": self.job["Nodes"]}})
		for graph in graphCollection:
			self.graphs[graph["_id"]] = graph["values"]
		self.storeJob()
		graphCollection.drop()

	def storeJob(self):
		self.job["JobID"] = int(self.job["JobID"])
		endTime = datetime.datetime.strptime(self.job["End"], "%Y-%m-%dT%H:%M:%S")
		startTime = datetime.datetime.strptime(self.job["Start"], "%Y-%m-%dT%H:%M:%S")
		submitTime = datetime.datetime.strptime(self.job["Submit"], "%Y-%m-%dT%H:%M:%S")
		metadata = self.job
		metadata["NumNodes"]	= len(metadata["Nodes"])
		metadata["NCPUS"]	= int(self.job["NCPUS"])
		metadata["Submit"]	= submitTime
		metadata["End"]		= endTime
		metadata["Start"]	= startTime
		metadata["RunTime"]	= (endTime - startTime).seconds
		metadata["Graphs"]	= []
		metadata.update(self.additionalFields)
		print "Inserting", metadata
		_id = self.db.jobs.insert(metadata)
		graphs=[]
		for graph in self.graphs:
			self.graphs[graph]["job"] = _id
			graphs.append({"name": graph, "graph": self.db.graphs.insert(self.graphs[graph])})
		print "Appending Graphs", ",".join([graph["name"] for graph in graphs])
		self.db.jobs.update({"_id": _id}, {"$set": {"Graphs": graphs}})
