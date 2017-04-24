from __future__ import print_function

import sys

from pyspark import SparkContext

def getgeo(partID, records):
    if partID == 0:
        records.next()
    
    reader = csv.reader(records)
    lon_lat={}

    for row in reader:
        if (row[3].split(' ')[0] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave') :
            lon_lat[(float(row[7]),float(row[8]))] =1
    geo = lon_lat.keys()
    return geo

def biketime(partID, records):
    if partID == 0:
        records.next()
    
    reader = csv.reader(records)

    starttime=[]
    for row in reader:
        if (row[3].split(' ')[0] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave') :
            starttime.append(row[3].split(' ')[1])

    starttime = map(lambda x: (datetime.strptime(x.split('+')[0], '%H:%M:%S') ,
                           datetime.strptime(x.split('+')[0], '%H:%M:%S')+timedelta(minutes = 10)) , starttime)
    starttime=map(lambda x: (x[0].time(),x[1].time()), starttime)
    return starttime


def findpair(partID, records):
    if partID == 0:
        records.next()
    
    reader = csv.reader(records)
    
    from geopy.distance import great_circle

    taxitime = []
    for row in reader:
        if (row[0].split(' ')[0] == '2015-02-01')&(row[4] != 'NULL') & (row[5] != 'NULL'):
            end = ( float(row[4]) , float(row[5]))
            if great_circle(end, geo).miles <= 0.25:
                taxitime.append(row[0].split(' ')[1])
    taxitime = map(lambda x: datetime.strptime(x[:7], '%H:%M:%S').time() ,taxitime)
    count = 0
    for i in taxitime:
        for j in starttime:
            if (i>= j[0]) & (i<= j[1]):
                count +=1
    return [count]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit sort <inputfile> <outputfile>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonSort")

    import csv
    from datetime import datetime, timedelta


    rdd = sc.textFile('/user/ys2808/citibike.csv')
    geo = rdd.mapPartitionsWithIndex(getgeo).collect()

    rdd = sc.textFile('/user/ys2808/citibike.csv')
    starttime = rdd.mapPartitionsWithIndex(biketime).collect()

    def findpair(partID, records):
        if partID == 0:
            records.next()
    
        reader = csv.reader(records)
    
        from geopy.distance import great_circle

        taxitime = []
        for row in reader:
            if (row[0].split(' ')[0] == '2015-02-01')&(row[4] != 'NULL') & (row[5] != 'NULL'):
                end = ( float(row[4]) , float(row[5]))
            if great_circle(end, geo).miles <= 0.25:
                taxitime.append(row[0].split(' ')[1])
        taxitime = map(lambda x: datetime.strptime(x[:7], '%H:%M:%S').time() ,taxitime)
        count = 0
        for i in taxitime:
            for j in starttime:
                if (i>= j[0]) & (i<= j[1]):
                    count +=1
        return [count]

    rdd = sc.textFile('/user/ys2808/yellow.csv.gz')
    count = rdd.mapPartitionsWithIndex(findpair)
    count.saveAsTextFile('/user/ys2808/hw7')
    print (count.collect())

    sc.stop()
