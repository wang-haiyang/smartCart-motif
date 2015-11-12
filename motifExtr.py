# -*- coding:utf-8 -*-

from pyspark import SparkConf,SparkContext
import time
from operator import add


def to_timestamp(time_point):
	d = time.strptime(time_point,"%Y-%m-%d %H:%M:%S")
	return time.mktime(d)

def to_datetime(time_interval):
	x = time.localtime(float(time_interval))
	return time.strftime('%Y-%m-%d %H:%M:%S',x)

def location_to_motif(locations):
	locations = locations.split(',')
	dateFlag = ''
	motifList = []
	for i in xrange(0,len(locations)):
		_date = locations[i].split(' ')[0]
		_time = locations[i].split(' ')[1]
		station = locations[i].split(' ')[2].split(u'号线')[1]
		if locations[i].split(' ')[3]!='':
			fare = float(locations[i].split(' ')[3])
		else:
			fare = float(locations[i].split(' ')[4])


		if dateFlag != _date:#if another day, initialize
			motifList.append({})
			nodeDict = {}
		if not nodeDict.has_key(station):#map station name to node num
			nodeDict[station] = len(nodeDict)
		if not motifList[-1].has_key(nodeDict[station]):#add the appeared node to today's motif graph
			motifList[-1][nodeDict[station]] = []
		if fare==0:#if enter a station, add a directed edge to next station(if it's an exit)
			if (i+1)<len(locations):
				if locations[i+1].split(' ')[0]==_date:#yet is today's
					if locations[i+1].split(' ')[3]!='':
						n_fare = float(locations[i+1].split(' ')[3])
					else:
						n_fare = float(locations[i+1].split(' ')[4])
					if float(n_fare)!=0:#exit from next station
						n_station = locations[i+1].split(' ')[2].split(u'号线')[1]
						if not nodeDict.has_key(n_station):
							nodeDict[n_station] = len(nodeDict)
						motifList[-1][nodeDict[station]].append(nodeDict[n_station])
		dateFlag = _date

	return motifList

def style_conv(list_dict):
	dicts = str(list_dict).strip('[]')
	dicts = dicts.split(', {')
	rsl = []
	for i in xrange(0, len(dicts)):
		if i ==0:
			rsl.append(dicts[i])
		else:
			rsl.append('{' + dicts[i])
	return rsl

conf = SparkConf().setMaster('yarn-client') \
                  .setAppName('userMotif') \
                  .set('spark.driver.maxResultSize', "10g")
sc = SparkContext(conf=conf)

userLogs = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/wanghaiyang/SODA/smartCard/rslts/userLocations/*')
motifs = userLogs.map(lambda x: '|'.join(style_conv(location_to_motif(x.split('\t')[1])))) \
				 .flatMap(lambda x: x.split('|'))
total = motifs.count()

motifs.map(lambda x: (x,1)) \
	  .reduceByKey(add) \
	  .sortBy(lambda x: x[1], ascending=False) \
	  .map(lambda x: x[0] + '|' + str(x[1]) + '|' + str(float(x[1])/total)) \
	  .saveAsTextFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/wanghaiyang/SODA/smartCard/rslts/motifStat')

print total