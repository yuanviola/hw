
#sc
import csv
from datetime import datetime, timedelta
def getgeo(partID, records):
    if partID == 0:
        records.next()
        
    reader = csv.reader(records)
    lon_lat={}

    for row in reader:
        if (row[3].split(' ')[0] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave') :
            lon_lat[(row[7],row[8])] =1 
    return lon_lat.keys()

rdd = sc.textFile('citibike.csv')
geo = rdd.mapPartitionsWithIndex(getgeo).collect()
geo=geo[0]
geo=(float(geo[0]), float(geo[1]))

def getgeo(partID, records):
    if partID == 0:
        records.next()
        
    reader = csv.reader(records)

    starttime=[]
    for row in reader:
        if (row[3].split(' ')[0] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave') :
            starttime.append(row[3].split(' ')[1])
    return starttime 
            

rdd = sc.textFile('citibike.csv')
biketime = rdd.mapPartitionsWithIndex(getgeo).collect()

def findpair(partID, records):
    if partID == 0:
        records.next()
        
    reader = csv.reader(records)
    
    from geopy.distance import great_circle

    taxitime []
    for row in reader:
        if (row[0].split(' ')[0] == '2015-02-01')&(row[4] != 'NULL') & (row[5] != 'NULL'):
            end = ( float(row[4]) , float(row[5]))
            if great_circle(end, geo).miles <= 0.25:
                taxitime.append(row[0].split(' ')[1])
                
    return taxitime


rdd = sc.textFile('yellow.csv.gz')
taxitime = rdd.mapPartitionsWithIndex(findpair).collect()


starttime = map(lambda x: (datetime.strptime(x.split('+')[0], '%H:%M:%S') ,
                           datetime.strptime(x.split('+')[0], '%H:%M:%S')+timedelta(minutes = 10)) , biketime)
starttime=map(lambda x: (x[0].time(),x[1].time()), starttime)

taxitime = map(lambda x: datetime.strptime(x[:7], '%H:%M:%S').time() ,taxitime)

count = 0
for i in taxitime:
    for j in starttime:
        if (i>= j[0]) & (i<= j[1]):
            count +=1
            
print count

