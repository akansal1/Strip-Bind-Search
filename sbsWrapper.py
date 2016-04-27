#!/usr/bin/env python2
"""

Librairies needed: 
 -numpy
 -scipy
 -mongopy

Requirements:
 -At least one month of data to do the bootstrap

Notice:
  -Data are sent to SBS by chunks of 1 day long
"""

import sys;
import socket;
import urllib;
import datetime;

#import Mymongodb as mdb;
from pymongo import MongoClient;
import bson;
import pickle;
import sbs;
import pdb;
#import sendEmail;

#Remove the alarms that are "symmetric"
def rmSymetricAlarms(alarms):
  res = []

  if alarms == None:
    return res

  for i, alarm1 in enumerate(alarms):
    sym = False
    for alarm2 in alarms[i+1:]:
      if alarm1["label"]==alarm2["peer"]:
        sym = True
        
    if not sym:
      res.append(alarm1)
      
  return res

# Aggregate consecutive anomalies for the same device
def aggConsecAlarms(alarms):
  res = []

  if alarms == None:
    return res

  for i, alarm1 in enumerate(alarms):
    consec = False;
    for alarm2 in alarms[i+1:]:
      # if the alarms report the same devices and their timestamps overlap
      if (alarm1["label"]==alarm2["label"] and alarm1["peer"]==alarm2["peer"]) or (alarm1["label"]==alarm2["peer"] and alarm1["peer"]==alarm2["label"]) and ((alarm1["end"]>=alarm2["start"] and alarm1["start"]<=alarm2["end"]) or (alarm2["end"]>=alarm1["start"] and alarm2["start"]<=alarm1["end"])):
        consec=True;
        alarm2["start"] = min(alarm1["start"],alarm2["start"]);
        alarm2["end"] = max(alarm1["end"],alarm2["end"]);
        alarm2["dev"] += alarm1["dev"];
        break
        
    if not consec:
        res.append(alarm1)
        
  return res

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def getMongoSample(mongoserver, mongoport, start, end):
  client = MongoClient(mongoserver, mongoport)
  db = client.abnormal_energy
  coll = db.sbs
  cursor = coll.find({"timestamp":{"$gt":start, "$lt":end, "$mod":[300, 0]}})

  ret = []
  for item in cursor:
    ret += [(item["sensor_id"], item["timestamp"], item["power"])]
  cursor.close()
  return ret

### Run SBS with data from a Mongo server
def Mongo2SBS(mongoserver, mongoport, dbname, start, end):
  sys.stdout.write("[{0}] 0%, Start SBS: timeStart={1}, timeEnd={2}\n".format(datetime.datetime.now(),start,end))
  sys.stdout.flush()
  #Initialization of SBS
  detector = sbs.SBS()


  client = MongoClient("mongodb://"+mongoserver+":"+str(mongoport))
  db = client[dbname]
  

  print("Initialize SBS state")
  filteredSensors = dict()


  streams = db.streams.find()
  i=0
  for sensor in streams:
    #print(sensor)                                                                                                                                                                                         
    name = str(sensor).split("name': u'")[1].split("'}")[0]
    filteredSensors.update({name:i})
    i+=1
  #print(filteredSensors)
  
# TODO_END  
  detector.filteredSensors = filteredSensors
  allAlarms = []
  one_day = 24 * 60 * 60
  start_day = int(start)
  for curr_day in range(start_day, int(end), one_day):
    next_day = curr_day + one_day
    sys.stdout.write("[{0}] {1}%, Analyzing data from {2} to {3}\n".format(datetime.datetime.now(),int(100*(curr_day - start)/(end-start)),datetime.datetime.fromtimestamp(curr_day),datetime.datetime.fromtimestamp(next_day)))
    sys.stdout.flush()
    mongoData = []


# Power value of sensor stores in table 'sbs'
    for fs in filteredSensors:
      time_vals = db.sbs.find({'sensor_id':fs,'timestamp': { '$gt': curr_day, '$lt': next_day } } )
      for line in time_vals:
        #print(line)
        sensor_id = str(line).split("sensor_id': u'")[1].split("', u'po")[0]#could probably optimize these
        timestamp = int(str(line).split("u'timestamp': ")[1].split(", u'_id'")[0])
        power = float(str(line).split("'power': ")[1].split("}")[0])
        #print(sensor_id+"----"+timestamp+"----"+power)
        mongoData.append((sensor_id,timestamp,power))

    alarms = detector.addSample(mongoData,tsdb=False)

    #Filter out the symmetric alarms
    alarms = rmSymetricAlarms(alarms)

    allAlarms.extend(alarms)
    
  #Aggregate consecutive alarms
  allAlarms = aggConsecAlarms(allAlarms)

  sys.stdout.write("[{0}] 100%, SBS found {1} anomalies in total\n".format(datetime.datetime.now(),len(allAlarms)))



  print("Alarms: ")
  for i in allAlarms:
    print(i) #print to log, for comparison with pickle file
  results = open('alarm.pkl','w')
  pickle.dump(allAlarms,results)
  results.close()
  sys.stdout.write("Results written to alarm.pkl, process finished.")
# TODO_END
  return


if __name__ == "__main__":
  if len(sys.argv) < 6:
    print("usage: {0} mongoserver mongoport dbname timeStart timeEnd".format(sys.argv[0]))
    default = raw_input("..or enter 1 to run it using default args [localhost, 27017, abnormal_energy, 1301842810.0, 1307890810.0]:\t")
    if(default=="1"):
      mongoserver = "localhost" #change this to daisy ip                                                                                                                                                    
      mongoport = int(27017)
      dbname = "abnormal_energy"
      start = float(1301842810.0)
      end = float(1307890810.0)
    else:
      exit()

  else:
    ## Initialisation                                                                                                                                                                                       
    mongoserver = sys.argv[1]
    mongoport = int(sys.argv[2])
    dbname = sys.argv[3]
    start = float(sys.argv[4])
    end = float(sys.argv[5])

  Mongo2SBS(mongoserver, mongoport, dbname, start, end)
