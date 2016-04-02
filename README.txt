#Modified by: Julia Signell
#Date modified: 2015-10-05

#Modified by: Kelly Caylor
#Date modified: 2016-04-02

The files and scripts in this folder are used to extract data from tower computer files (in /TowerData) for use in NASA’s Soil Moisture Active Passive (SMAP) satellite validation project.

At this time, process_smap_data_for_NASA.py is the only script being called by the server and it runs everything else. 

This script will need to run on mpala@africa.princeton.edu from a cronjob every week at midnight on Sunday.
 
This folder contains:
 - A folder containing SMAP output for the NASA project (SMAP_output)
 - functions for generating SMAP output including:
	
-> downscalers.py: downscales 10-min data into 60 minute entries, based on either summation over the hour, or sampling at the top of each hour.

-> smap_basin_functions.py and smap_tower_functions.py: These two libraries contain functions that build the SMAP upload files from the smapdata files that are generated in process_smap_data_for_NASA.py. 



