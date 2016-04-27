# SBS
Implementation of [String, Bind and Search](http://www.eecs.berkeley.edu/~jortiz/papers/p129-fontugne.pdf).

A Method for Identifying Abnormal Energy Consumption in Buildings. Uses Python to read MongoDB of building sensor data and applies SBS to identify abnormal sensors and incident timestamps.


#Strip and Bind using two raw signals standing for one week of data from two different HVACs. 
(1) Decomposition of the signals in IMFs using EMD (top to bottom: c1 to cn); 
(2) aggregation of the IMFs based on their time scale; 
(3) comparison of the partial signals (aggregated IMFs) using correlation coefficient.

![Algorithm](http://i.imgur.com/iepSwgc.png)


# Sample Report

![Report](http://i.imgur.com/48y4yNf.png)

