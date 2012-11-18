#maintains the log file pointer and the timer

import MySQLdb as mdb
import sys
import subprocess
import update_database

day_no=1
week_no=1
set_no=1

month = [31,28,31,30,31,30,31,31,30,31,30,31]

final_date = "2012-10-24"
final_time = "15:43:12"

log_no=1

prev_date = "2012-10-10"
prev_time = "13:10:12"
curr_date = "2012-10-12"
curr_time = "12:00:01"

filename = "log" + str(log_no) + ".txt"
f = open(filename)

#s = f.readline()

def incr(curr_date, curr_time):
	yy,mm,dd = curr_date.split('-')
	h,m,s = curr_time.split(':')

	#convert from string to numeric
	yy_num = int(yy)	
	mm_num = int(mm)	
	dd_num = int(dd)	
	h_num = int(h)
	m_num = int(m)
	s_num = int(s)

	#updates
	h_num = h_num + 1

	if (s_num > 59):
		s_num = s_num - 59
		m_num = m_num + 1
	if (m_num > 59):
		m_num = m_num - 59
		h_num = h_num + 1
	if (h_num > 23):
		h_num = h_num - 23
		dd_num = dd_num + 1
	if (dd_num > month[mm_num-1]):
		dd_num = dd_num - month[mm_num-1]
		mm_num = mm_num + 1
	if (mm_num > 12):
		mm_num = mm_num - 12
		yy_num = yy_num + 1

	print yy,mm,dd,h,m,s

	#convert from numeric to string
	yy = str(yy_num)	
	mm = str(mm_num)	
	dd = str(dd_num)	
	h = str(h_num)
	m = str(m_num)
	s = str(s_num)

	while(len(mm)<2):
		mm = '0' + mm 
	while(len(dd)<2):
		dd = '0' + dd 
	while(len(h)<2):
		h = '0' + h 
	while(len(m)<2):
		m = '0' + m 
	while(len(s)<2):
		s = '0' + s 

	curr_date = yy + "-" + mm + "-" + dd
	curr_time = h + ":" + m + ":" + s
	return curr_date, curr_time	

try:
	conn = mdb.connect('localhost', 'root', '123456', 'network_db');

	cursor = conn.cursor()

	#print "INSERT INTO CURRENTDAY VALUES ( "+ topic + "," + nodes[0] + "," + nodes[1] + "," + date + "," + ts[2] + ")"
	while(True):
		if ((curr_date > final_date) or (curr_date == final_date and curr_time > final_time)):
			curr_date = final_date
			curr_time = final_time
		f, eof, last_date, last_time, day_no, week_no, set_no = update_database.update(f,curr_date,curr_time,prev_date,prev_time,conn,cursor, day_no, week_no, set_no)
		if(curr_date == final_date and curr_time == final_time):
			break
		if (eof):
			log_no = log_no + 1
			filename = "log" + str(log_no) + ".txt"
			f = open(filename)
			prev_date, prev_time = last_date, last_time
		else:
			prev_date, prev_time = curr_date, curr_time
			curr_date, curr_time = incr(curr_date, curr_time)
		#break

		conn.commit()

except mdb.Error, e:
  
	conn.rollback()
	print "Error %d: %s" % (e.args[0],e.args[1])
	sys.exit(1)

cursor.close()
conn.close()
f.close()

'''
#update date and time

print curr_time.split(':')
[HH,MM,SS] = curr_time.split(':')
MM = str(int(MM)+10)
curr_time = HH+":"+MM+":"+SS
print curr_time
'''

