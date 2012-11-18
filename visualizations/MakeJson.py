#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import csv
import json
import collections

con = None

try:
    con = mdb.connect('localhost', 'root', 'vnyk', 'network_db');
    cur = con.cursor()
    # Total Topic Volume Current DAY
    cur.execute("SELECT TOPIC, COUNT(TOPIC) as 'Frequency' from CURRENTDAY GROUP BY(TOPIC)")
    rows = cur.fetchall()
    objects_list = []
    for row in rows:
    	d = collections.OrderedDict()
    	d['Topic'] = row[0]
    	d['Frequency'] = row[1]
    	objects_list.append(d)
    j = json.dumps(objects_list)
    objects_file = 'student_objects.js'
    f = open(objects_file,'w')
    print >> f, j
	 
#	conn.close()
#    for row in rows:
#    	c.writerow(row)
#    f.close()
    
    
    # Topic Communication at Various Location
    '''
    cursor.execute ("select t2.LOCATION, t1.Topic,COUNT(t2.LOCATION) as 'Frequency' from CURRENTDAY t1, NODES t2 where (t1.NODE1 = t2.NODE_ID) or (t1.NODE2 = t2.NODE_ID)  GROUP BY t2.LOCATION,t1.TOPIC ORDER BY t2.LOCATION, COUNT(t2.LOCATION) DESC")
    csv_writer = csv.writer(open("temp.csv","wb"))
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers  
    csv_writer.writerows(cursor)  
    del csv_writer
    
    rows = cur.fetchall()
    for row in rows:
    	c.writerow(row)
    f.close()
    
    f.write( "Location,Topic1,Topic2,Topic3,Topic4,Topic5\n" ) #How may times Topic X
    i = 0
    for row in rows:
    	
    	f.write( str(row[1]) + "," + str(row[0]) + "\n" )'''
#    f.close()
    
except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()
