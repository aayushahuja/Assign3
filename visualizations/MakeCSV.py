#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import csv

con = None

try:

    con = mdb.connect('localhost', 'root', 'vnyk', 'network_db');
    cursor = con.cursor()
    f = open('TopicWise.csv' ,'w')
    
    # Total Topic Volume Current DAY
    cursor.execute ("SELECT TOPIC, COUNT(TOPIC) from CURRENTDAY GROUP BY(TOPIC)")
    csv_writer = csv.writer(open("temp.csv","wb"))
    csv_writer.writerow([i[0] for i in cursor.description]) # write headers  
    csv_writer.writerows(cursor)  
    del csv_writer
    
    f.close()
    
    
except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()
