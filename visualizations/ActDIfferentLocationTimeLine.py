#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import csv
loc = 'Asgard' #location - Location
con = None
Vol = [] #Array To Store Volume Of information
try:
    con = mdb.connect('localhost', 'root', 'vnyk', 'network_db');
    cursor = con.cursor()
    cursor.execute ("SELECT COUNT(TOPIC) from CURRENTDAY t1, NODES t2  where ((t1.NODE1 = t2.NODE_ID) or (t1.NODE2 = t2.NODE_ID)) and t2.LOCATION = 'Asgard' " )
    Vol.append(int(cursor.fetchone()[0]))
    print Vol
    '''
    for i in range(6):
    	cursor.execute ("SELECT COMM_COUNT from 7DAYS where (LOCATION1 = 'Asgard' or LOCATION2 = 'Asgard') and DAY = i+1" )
    	Vol.append(int(cursor.fetchone()[0]))
    
    for i in range(4):
    	
    	
    #csv_writer = csv.writer(open("temp.csv","wb"))
    #csv_writer.writerow([i[0] for i in cursor.description]) # write headers  
    #csv_writer.writerows(cursor)  
    #del csv_writer
    #f.close()
    '''
    
except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()
