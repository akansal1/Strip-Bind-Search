"""
The Strip, Search and Bind method
=================================

The Strip, Bind and Search (SBS) method uncovers abnormalities in equipment behavior and in-concert usage.
SBS uncovers relationships between devices and constructs a model for their in-concert usage.
It then flags deviations from the model as abnormal.
"""


#Notice:
#   - the number of sensors shouldn't change over time..

###!!! NEEDED for python 2.x ?
#from __future__ import division

import scipy as sp;
import numpy as np;
#import matplotlib.pyplot as plt;

from scipy import interpolate;
from scipy import linspace;
from scipy.signal import butter;
from scipy.signal import filtfilt;

from collections import deque

import pdb


class SBS:
  
  def __init__(self):
    """
    Initialize the SBS object
    """
    ###### Parameters ######
    # Parameters for the sliding window (the unit is seconds)

    self.windowSize = 7*24*3600; # Size of the sliding time window 
    self.windowTail = -1;
    self.windowStep = 24*3600; #self.windowSize; #Size of each sliding step
    self.samplingRate = 300;
    self.ratioFilledBuffer = 0.7;  # Detect abnormalities when at least ratioFilledBuffer*100% of the sensors provided enough data
    
    # used only for log
    self.nbIter = 0;

   
    # Data structures
    ## strip (signals)
    self.buffer = dict();
    self.filteredData = None;
    self.filteredSensors = None;      # Name of the sensors for each entry in filteredData
    # Parameters for the filter bank
    self.cutoffHigh = 20*60;
    self.cutoffLow  = 360*60;
    self.butterOrder = 5;
    
    
    ## bind (correlation matrix)
    self.currBehavior = None;
    
    
    ## search (normal behavior of the devices)
    self.bootstrapDetection = True;
    self.histBehavior = deque();
    self.histBehaviorChange = dict();
    self.histBehaviorSize=14; #5
    self.lnorm = 4.0;
    self.detectionThreshold = 5.0;
    
    ## peak detector
    self.peakDetectionThreshold = 3.0
  
  ############### STRIP ############### 
  def strip(self):
    """
    Filter the data in order to uncover the intrinsic usages of the sensors.
    The filtered signals are stored in the filteredData attribute.
    """
    
    ### clean the buffer? remove all sensors without data /// No! it might mess up the "search"...?
    
    #print("strip...")
    
    self.filteredData = np.zeros( (len(self.filteredSensors), self.windowSize/float(self.samplingRate)) )
    _nullFct = lambda x : np.zeros(len(x))
    
    # Get the right sampling rate
    # And filter the data
    xSample = np.arange(self.windowTail, self.windowTail+self.windowSize, self.samplingRate);
    nyq = 0.5*1.0/self.samplingRate;
    lowCut  = 1.0/self.cutoffLow;
    highCut = 1.0/self.cutoffHigh;
    Wn = [lowCut/nyq, highCut/nyq]

    for sen, ind in self.filteredSensors.items():
      ##sample the buffer
      if sen in self.buffer:
        rawData = np.array(self.buffer[sen])
      else:
        rawData = np.array([])
        
      if np.size(rawData,0)<2: # If the buffer is empty
        f = _nullFct
        print("Warning: buffer is empty for point: "+sen)
      else:
        f = interpolate.interp1d(rawData[:,0],rawData[:,1],copy=False,bounds_error=False,fill_value=0)

      ##filter the sampled buffer
      #[b1,a1] = butter(butterOrder,highCut/nyq,btype="highpass");
      #dataFiltered1 = filtfilt(b1,a1,dataSample); # high pass filter

      [b2,a2] = butter(self.butterOrder,Wn,btype="bandpass");
      self.filteredData[ind,:] = filtfilt(b2,a2,f(xSample))[:]; # band pass filter

      #[b3,a3] = butter(butterOrder,lowCut/nyq,btype="lowpass");
      #dataFiltered3 = filtfilt(b3,a3,dataSample); # low pass filter

      
    
  ############### BIND ############### 
  def bind(self):
    """
    Reveal the inter-devices usages by computing the correlation of filtered data.
    The correlation matrix is stored in the attribute currBehavior.
    """
    #print("bind...{0}".format(np.shape(self.filteredData)))
    
    #Completely flat signals (no data at all...) are
    #troublesome for the computation of the correlation...
    #Workaround: add one random point
    for sen in range(0,len(self.filteredSensors)):
      if not np.any(self.filteredData[sen,:]):
        self.filteredData[sen,int(np.random.rand()*(len(self.filteredData[sen,:])-1))]=np.random.rand();
    
    self.currBehavior = np.corrcoef(self.filteredData);
    self.currBehavior[np.isnan(self.currBehavior)]=0
    self.currBehavior[np.diag_indices_from(self.currBehavior)]=1
    
    
  ############### SEARCH ############### 
  def search(self):
    """
    Detect abnormal behaviors
    """
    alarms = []
    if np.size(self.histBehavior,0) < self.histBehaviorSize:
      return alarms
    else:
      
      #print("search...")
      # Compute the normal behavior
      R = np.median(self.histBehavior,axis=0) 	# Reference matrix
      
      if len(self.histBehaviorChange) == 0:
        # Bootstrap: for the first histBehaviorSize bins the behavior change is computed witht the same reference matrix
        for sen in self.filteredSensors.keys():
          self.histBehaviorChange[sen] = deque();
        
        for t in range(self.histBehaviorSize):
          for sen, i in self.filteredSensors.items():
            #Compute the behavior change (l_it) for the current correlation matrix
            l_it=.0
            Ri = float(sum(R[i,:]))
            peerLabel = ""
            peerDev  = 0
            for jLabel, j in self.filteredSensors.items():
              tmp = np.power((R[i,j]/ Ri)*(self.histBehavior[t][i,j]-R[i,j]),self.lnorm)
              l_it += tmp
          
              if peerDev < tmp and i!=j:
                peerLabel = jLabel
                peerDev = tmp
            
            l_it = np.power(l_it,1.0/self.lnorm)
            self.histBehaviorChange[sen].append(l_it)
        
        #Detection in the bootstrap data
        if self.bootstrapDetection:
          for sen, i in self.filteredSensors.items():
            c=0.6745
            l_i = self.histBehaviorChange[sen]
            thres = np.median(l_i)+self.detectionThreshold*(np.median(abs(l_i-np.median(l_i)))/c)
            for t in range(self.histBehaviorSize):
              l_it = self.histBehaviorChange[sen][t]
              # Compare the behavior change with past behaviors
              if l_it > thres:
                #print("Time bin {3}: {0}: from {1} to {2}".format(sen,self.windowTail+(t-self.histBehaviorSize)*self.windowSize,self.windowTail+(1+t-self.histBehaviorSize)*self.windowSize,t))
                alarms.append({"label":sen, "start":self.windowTail+(t-self.histBehaviorSize)*self.windowSize, "end":self.windowTail+(1+t-self.histBehaviorSize)*self.windowSize, "dev":abs(l_it-np.median(l_i))/float(np.median(abs(l_i-np.median(l_i)))/c), "peer":peerLabel})
           
            
        # print("Bootstrap done!")
        # End of Bootstrap, note that no alarm will be raised for the histBehaviorSize first time bins
      
      for sen, i in self.filteredSensors.items():
        #Compute the behavior change (l_it) for the current correlation matrix
        l_it=.0
        Ri = float(sum(R[i,:]))
        peerLabel = ""
        peerDev  = 0
        for jLabel, j in self.filteredSensors.items():
          tmp = np.power((R[i,j]/ Ri)*(self.currBehavior[i,j]-R[i,j]),self.lnorm)
          l_it += tmp
          
          if peerDev < tmp and i!=j:
            peerLabel = jLabel
            peerDev = tmp
            
        
        l_it = np.power(l_it,1/self.lnorm)
        
        # Compare the behavior change with past behaviors
        c=0.6745
        l_i = self.histBehaviorChange[sen]
        if l_it > np.median(l_i)+self.detectionThreshold*(np.median(abs(l_i-np.median(l_i)))/c):
          #print("Time bin {3}: {0}: from {1} to {2}".format(sen,self.windowTail,self.windowTail+self.windowSize,self.nbIter))
          alarms.append({"label":sen, "start":(self.windowTail+self.windowSize)-self.windowStep, "end":self.windowTail+self.windowSize, "dev":abs(l_it-np.median(l_i))/float(np.median(abs(l_i-np.median(l_i)))/c), "peer":peerLabel})
        # Store the behavior change
        self.histBehaviorChange[sen].append(l_it)    
      
    #print("detection done")
    return alarms
    
  ############### OTHERS  ############### 
  def addSample(self, sample,tsdb=False):
    """
    Add the given samples to the current time window.
    If the time window is completed the SBS method is executed.
    
    Parameters
    ----------
    sample : list or OpenTSDB result
      List: List of tuples containing the new samples. Each tuple is in the form (sensorID, timestamp, value).
      OpenTSDB data: timestamp, value, label

    Returns
    -------
    alarm : list
      List of the alarms reported by SBS. 
      Each alarm is a tuple in the form (sensorID, start-time, end-time, divergence); the timestamp is the begining of the last analyzed time window and the divergence correspond to the distance from the normal behavior of the sensor sensorID. Larger distance means severe misbehavior.
    """
    
    
    # Add the new samples to the buffer
    if tsdb:
      for line in sample:
        point = line.split()
        time = int(point[1])
        val = float(point[2])
        sen =point[3].split("=")[1]
        if not sen in self.buffer:
          if self.windowTail == -1:
            self.windowTail = time
          self.buffer[sen] = deque()
        self.buffer[sen].append( (time,val) )
    else:
      for sen, time, val in sample:
        if not sen in self.buffer:
          # Initialize the window tail for the first added sample 
          if self.windowTail == -1:
            self.windowTail = min([samp[1] for samp in sample])
          self.buffer[sen] = deque()
        self.buffer[sen].append( (time,val) )
      
    nbFilledBuffer=0
    for sen, dat in self.buffer.items():
      if len(dat)>0 and dat[-1][0] > self.windowTail+self.windowSize:
        nbFilledBuffer+=1
    
    # Execute Strip, Bind and Search if the buffer is full
    res = None
     
    if nbFilledBuffer > len(self.buffer)*self.ratioFilledBuffer:
      self.nbIter += 1
     
      
      # Peak detection: remove high values (error, obvious anomalies...)
      res = self.peakDetec()

      #STRIP
      self.strip()
      #BIND
      self.bind()
      #SEARCH
      res.extend(self.search())
      
      # Slide the time window
      self.windowSlide()
      
    return res
   
   
  def windowSlide(self):
    """
    Slide the time window (the step size is given by the attribute windowStep), clean the buffers and store the last correlation matrix in histBehavior.
    """
    
    #print("window slide")
    
    # Shrink the buffer
    for sen, dat in self.buffer.items():
      while len(dat) and dat[0][0]< self.windowTail:
          #TODO the buffer is twice larger than the time bin (make it tunable?)
          dat.popleft()
      
      
    # Store the correlation matrix (devices behavior)
    self.histBehavior.append(self.currBehavior)
    if np.size(self.histBehavior,0) > self.histBehaviorSize:
      self.histBehavior.popleft()
      for sen in self.histBehaviorChange.keys():
         self.histBehaviorChange[sen].popleft()
    
    # Slide the window
    self.windowTail += self.windowStep
  
 
### Simple peak detection used both for cleaning the data and reporting peaks
  def peakDetec(self):
    alarms = []
    for sen, dat in self.buffer.items():
      ##sample the buffer
      rawData = np.array(dat)
      if not np.size(rawData,0)<2: # If the buffer is empty there is nothing to do
            c=0.6745
            med = np.median(rawData[:,1])
            mad = np.median(abs(rawData[:,1]-med))/c
            thresMin = med-(self.peakDetectionThreshold*mad)
            thresMax = med+(self.peakDetectionThreshold*mad)
            ano = (rawData[:,1]<thresMin) | (rawData[:,1]>thresMax)

            if np.any(ano) and mad!=0:
               # Smooth the peaks
               anoInd = np.where(ano)

               # Find the starting and ending point of each peak
               anoInterval = []
               start = None
               prev = None
               for i in range(len(anoInd)):
                 if start == None:
                   start = i

                 elif i != prev+1 :
                   anoInterval.append([start,prev])
                   start = i

                 prev = i

               anoInterval.append([start,prev])

	       # Get rid of the peaks using linear interpolation
               for peak in anoInterval:
                 if peak[0] == 0 :
                   #s = rawData[peak[1],1]+1
                   #e = rawData[peak[1],1]+1
                   continue
                 if peak[1] == len(rawData)-1:
                   #s = rawData[peak[0],1]-1
                   #e = rawData[peak[0],1]-1
                   continue
                 else:
                   s = rawData[peak[0],1]-1
                   e = rawData[peak[1],1]+1
                 print("remove peak for "+sen+" from "+str(self.windowTail)+" to "+str(self.windowTail+self.windowSize))
                 # Report anomalous peaks
                 alarms.append({"label":sen, "start":rawData[peak[0],0], "end":rawData[peak[1],0], "dev":9999, "peer":""})
                 rawData[peak[0]:peak[1]+1,1] = linspace(s,e,(peak[1]+1)-peak[0])
    return alarms
           
