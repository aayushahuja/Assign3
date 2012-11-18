#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys

months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']	

#tokenises the log record: returns topic, node1, node2, date, time
def tokenise_logrecord(comm_log_record):
	print "tokenise"
	comm = comm_log_record[28:]
	parts = comm.split(', ')
	ts = parts[0].split(' ')
	date = ts[4] + '-' +  str(months.index(ts[0])+1) + '-' + ts[1]
	
	nodes = parts[1].split('-')
	#arrange nodes in ascending order
	if(nodes[0]>nodes[1]):
		nodes[1], nodes[0] = nodes[0], nodes[1]
	topic = parts[2].strip()
	
	return topic, nodes[0], nodes[1], date, ts[2] 	

#adds a new entry to CURRENTDAY table; delete all entries more than 24 hrs old
def update_currday(topic, node1, node2, date, time, cursor, aggregate, day_no, week_no, set_no):
	print "update_currentday"
	'''
	#convert date into correct format
	comm = comm_log_record[28:]
	parts = comm.split(', ')
	ts = parts[0].split(' ')
	date = ts[4] + '-' +  str(months.index(ts[0])+1) + '-' + ts[1]
	
	nodes = parts[1].split('-')
	#arrange nodes in ascending order
	if(nodes[0]>nodes[1]):
		nodes[1], nodes[0] = nodes[0], nodes[1]
	topic = parts[2].strip()
	#print ts, nodes, topic, date	
	'''

	if(aggregate):
		day_no, week_no, set_no = currday2week(date, cursor, day_no, week_no, set_no)

	#print "INSERT INTO CURRENTDAY VALUES ( "+ topic + "," + nodes[0] + "," + nodes[1] + "," + date + "," + ts[2] + ")"
	cursor.execute("INSERT INTO CURRENTDAY VALUES ( '"+ topic + "' ," + node1 + "," + node2 + ", '" + date + "' , '" + time + "' )")
	cursor.execute("DELETE FROM CURRENTDAY WHERE COMM_DATE < '" + date + "' AND COMM_TIME <'" + time + "'")
	cursor.execute("DELETE FROM CURRENTDAY WHERE DATEDIFF ( '" + date + "' , COMM_DATE ) > 1")
	return day_no, week_no, set_no





#called immediately after 12:00 midnight everyday
def currday2week(currdate, cursor, day_no, week_no, set_no):
	print "day2week"
	if(day_no>7):
		day_no, week_no, set_no = week2month(currdate, cursor, day_no, week_no, set_no)
		day_no = day_no-7

	cursor.execute("DELETE FROM 7DAYS WHERE DAY_NO = " + str(day_no))
	#print "INSERT INTO CURRENTDAY VALUES ( "+ topic + "," + nodes[0] + "," + nodes[1] + "," + date + "," + ts[2] + ")"
	cursor.execute("INSERT INTO 7DAYS (TOPIC, LOC1, LOC2, COMM_COUNT) (SELECT CURRENTDAY.TOPIC, NODES1.LOCATION, NODES2.LOCATION, COUNT(*) FROM CURRENTDAY, NODES AS NODES1, NODES AS NODES2 WHERE CURRENTDAY.COMM_DATE < '" + currdate + "' AND CURRENTDAY.NODE1=NODES1.NODE_ID AND CURRENTDAY.NODE2=NODES2.NODE_ID GROUP BY CURRENTDAY.TOPIC, NODES1.LOCATION, NODES2.LOCATION)")
	cursor.execute("UPDATE 7DAYS SET DAY_NO = " + str(day_no) + " WHERE DAY_NO IS NULL")		
	day_no = day_no + 1
	return day_no, week_no, set_no




	
#called immediately after 12:00 midnight everyday
def week2month(currdate, cursor, day_no, week_no, set_no):
	print "week2month"
	if(week_no>4):
		day_no, week_no, set_no = set_no = month2set(currdate, cursor, day_no, week_no, set_no)
		week_no = week_no-4

	cursor.execute("DELETE FROM 4WEEKS WHERE WEEK_NO = " + str(week_no))
	#print "INSERT INTO CURRENTDAY VALUES ( "+ topic + "," + nodes[0] + "," + nodes[1] + "," + date + "," + ts[2] + ")"
	cursor.execute("INSERT INTO 4WEEKS (TOPIC, LOC1, LOC2, COMM_COUNT) (SELECT TOPIC, LOC1, LOC2, SUM(COMM_COUNT) FROM 7DAYS GROUP BY TOPIC, LOC1, LOC2)")
	cursor.execute("UPDATE 4WEEKS SET WEEK_NO = " + str(week_no) + " WHERE WEEK_NO IS NULL")		
	week_no = week_no + 1
	return day_no, week_no, set_no




	
#called immediately after 12:00 midnight everyday
def month2set(currdate, cursor, day_no, week_no, set_no):
	print "month2set"
	if(set_no>3):
		set_no = set_no-3

	cursor.execute("DELETE FROM 12WEEKS WHERE SET_NO = " + str(set_no))
	#print "INSERT INTO CURRENTDAY VALUES ( "+ topic + "," + nodes[0] + "," + nodes[1] + "," + date + "," + ts[2] + ")"
	cursor.execute("INSERT INTO 12WEEKS (TOPIC, LOC1, LOC2, COMM_COUNT) (SELECT TOPIC, LOC1, LOC2, SUM(COMM_COUNT) FROM 4WEEKS GROUP BY TOPIC, LOC1, LOC2)")
	cursor.execute("UPDATE 12WEEKS SET SET_NO = " + str(set_no) + " WHERE SET_NO IS NULL")		
	set_no = set_no + 1
	return day_no, week_no, set_no



'''
#the external function passes current date and time to this function, which makes necessary calls to update the database
#takes in the current timestamp of function call, and the last timestamp of function call: updates DB with activity during this period.
def update_database(curr_date, curr_time, prev_date, prev_time, filename):
	f = open(filename)
	s = f.readline()
	while(s<>''):
		topic, node1, node2, date, time = tokenise_logrecord(s)
		if (prev_date < date < curr_date or (date == curr_date and time <= curr_time) or (date == prev_date and time >= prev_time)):
			update_currday(topic, node1, node2, date, time) 
'''

def update(f, curr_date, curr_time, prev_date, prev_time, conn, cursor, day_no, week_no, set_no):
	print "update"
	p_date, p_time = prev_date, prev_time
	p=f.tell()
	s=f.readline()
	date = ""
	time = ""
	while(s<>''):
		topic, node1, node2, date, time = tokenise_logrecord(s)
		print p_date, p_time, date, time
		if ((date<curr_date) or (date == curr_date and time <= curr_time)):
			day_no, week_no, set_no = update_currday(topic, node1, node2, date, time, cursor, p_date < date, day_no, week_no, set_no)
			p_date, p_time = date, time
		else:
			f.seek(p)
			return f, False, "", "", day_no, week_no, set_no
		p=f.tell()
		s = f.readline()

	return f, True, date, time, day_no, week_no, set_no





