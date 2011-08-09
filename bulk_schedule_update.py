#!/usr/bin/env python

"""bulk_schedule_update.py: Simple Python script to update a SimpleDB domain that contains VNC instance schedules."""
__author__ = "dcarney / http://github.com/vnc"
__email__ = "chris.castle@vivaki.com"

import codecs
import sys
import time
from time import strftime
import traceback

import argparse
import boto

SCHEDULE_TYPE_NONE = "none"
SCHEDULE_TYPE_OFF = "off"
SCHEDULE_TYPE_ONOFF = "onoff"
DAYS_WEEKDAYS = "weekdays"
DAYS_WEEKENDS = "weekends"
DAYS_ALL = "all"

def parse_args():
	# Command-line argument/option parsing
	parser = argparse.ArgumentParser(description="Update the instance schedules in VNC's SimpleDB domain",
									 formatter_class=argparse.RawDescriptionHelpFormatter)
	parser.add_argument('accessKey', help="AWS Access Key")
	parser.add_argument('secretKey', help="AWS Secret Key")
	parser.add_argument('-f', required=True, dest='file', type=argparse.FileType('r'), default=sys.stdin, help="File containing list of instance IDs for which schedules will be updated -- one on each line.")
	parser.add_argument('-d', required=True, dest='domain', help="SimpleDB domain to update")
	parser.add_argument('-r', required=True, dest='region', help="A tz database compliant string that describes the region to " +
									   "use for timezone calculations. See: http://en.wikipedia.org/wiki/Tz_database " +
									   "for more information about tz-compliant regions.")
	parser.add_argument('--schedule', required=True, dest='schedule_type', help="A string specifying the schedule type.  Can be one of: none, off, or onoff. " +
											  "none: No schedule. off: Turn off only schedule. onoff: Turn on and turn off schedule.")
	parser.add_argument('--days', dest='days', help="One of weekdays, weekends, all.")
	parser.add_argument('--start', dest="start_time", help="Time (in the specified region), given in 24-hour HH format, at which the schedule start time will be set.")
	parser.add_argument('--stop', dest="stop_time", help="Time (in the specified region), given in 24-hour HH format, at which the schedule stop time will be set.")
	
	arguments = parser.parse_args()
		
	if (arguments.schedule_type == SCHEDULE_TYPE_OFF and (arguments.stop_time == None or
														  arguments.days == None)):
		print 'error: Both --stop and --days must be defined when "off" is the schedule_type'
		sys.exit(1)
	elif (arguments.schedule_type == SCHEDULE_TYPE_ONOFF and (arguments.start_time == None or
															  arguments.stop_time == None or
															  arguments.days == None)):
		print 'error: All of --start, --stop, and --days must be defined when "onoff" is the schedule_type'
		sys.exit(1)
	
	return parser.parse_args()

def update_schedule(accessKey, secretKey, domain, region, instance_id, schedule_type, days, start_time, stop_time):
	if (start_time < 10):
		start_time = '0'+str(start_time)
	if (stop_time < 10):
		stop_time = '0'+str(stop_time)
	
	try:
		sdb_conn = boto.connect_sdb(accessKey, secretKey)
		if (schedule_type == SCHEDULE_TYPE_NONE):
			put_attr = {'ScheduleModel':'AlwaysOnSchedule',
						'ReasonAlwaysOn':'prod ops bulk setting update',
					}
			del_attr = ['TurnOffLocalTime',
						'TurnOnLocalTime',
						'DaysOfTheWeekSchedule',
						'UserTimeZone'
						]
			#print "Adding attributes " + str(put_attr)
			#print "Removing attributes " + str(del_attr)
			sdb_conn.put_attributes(domain, instance_id, put_attr, True)
			sdb_conn.delete_attributes(domain, instance_id, del_attr)
			print "Schedule for " + instance_id + " updated"
		elif (schedule_type == SCHEDULE_TYPE_OFF):
			put_attr = {'ScheduleModel':'TurnOffOnlySchedule',
						'UserTimeZone':region,
						'TurnOffLocalTime':str(stop_time)+':00:00',
						'DaysOfTheWeekSchedule':days
						}
			del_attr = ['TurnOnLocalTime',
						'ReasonAlwaysOn',
						]
			#print "Adding attributes " + str(put_attr)
			#print "Removing attributes " + str(del_attr)
			sdb_conn.put_attributes(domain, instance_id, put_attr, True)
			sdb_conn.delete_attributes(domain, instance_id, del_attr)
			print "Schedule for " + instance_id + " updated"
		elif (schedule_type == SCHEDULE_TYPE_ONOFF):
			put_attr = {'ScheduleModel':'SpecificTimeSchedule',
						'UserTimeZone':region,
						'TurnOnLocalTime':str(start_time)+':00:00',
						'TurnOffLocalTime':str(stop_time)+':00:00',
						'DaysOfTheWeekSchedule':days
					}
			del_attr = [
						'ReasonAlwaysOn'
						]
			#print "Adding attributes " + str(put_attr)
			#print "Removing attributes " + str(del_attr)
			sdb_conn.put_attributes(domain, instance_id, put_attr, True)
			sdb_conn.delete_attributes(domain, instance_id, del_attr)
			print "Schedule for " + instance_id + " updated"
		else:
			sys.exit("Invalid schedule_type.")
	except Exception as ex:
		print type(ex), ex
		traceback.print_exc(file=sys.stdout)
		sys.exit(-1)

def print_args(*args):
	for a in args:
		print a

def main():
	try:
		arguments = parse_args()
		
		# some simple input cleansing
		try:
			if (arguments.schedule_type == SCHEDULE_TYPE_ONOFF):
				arguments.start_time = time.strptime(arguments.start_time, "%H").tm_hour
				arguments.stop_time = time.strptime(arguments.stop_time, "%H").tm_hour
			elif (arguments.schedule_type == SCHEDULE_TYPE_OFF):
				arguments.stop_time = time.strptime(arguments.stop_time, "%H").tm_hour
			elif (arguments.schedule_type != SCHEDULE_TYPE_NONE):
				sys.exit("Invalid schedule type. Execute bulk_schedule_update.py -h")
		except ValueError as ve:
			print type(ve), ve
			traceback.print_exc(file=sys.stdout)
			sys.exit(-1)
		
		# more input cleansing
		if (arguments.days == DAYS_WEEKENDS):
			arguments.days = "Saturday,Sunday"
		elif (arguments.days == DAYS_WEEKDAYS):
			arguments.days = "Monday,Tuesday,Wednesday,Thursday,Friday"
		elif (arguments.days == DAYS_ALL):
			arguments.days = "Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday"
		elif (arguments.days != None):
			sys.exit("Invalid days. Execute bulk_schedule_update.py -h")
		
		# setup Unicode support for stdout and stderr
		sys.stderr = codecs.getwriter('utf8')(sys.stderr)
		sys.stdout = codecs.getwriter('utf8')(sys.stdout)
		
		print "Reading in instance ID file..."
		try:
			instance_ids = arguments.file.read().split()
		except ValueError as ve:
			print type(ve), ve
			traceback.print_exc(file=sys.stdout)
			sys.exit(-1)
		
		print "Updating instance schedules..."
		for instance in instance_ids:
			update_schedule(arguments.accessKey,
			#print_args(arguments.accessKey,
							arguments.secretKey,
							arguments.domain,
							arguments.region,
							instance,
							arguments.schedule_type,
							arguments.days,
							arguments.start_time,
							arguments.stop_time
							)
	
	except Exception as ex:
		print type(ex), ex
		traceback.print_exc(file=sys.stdout)
		sys.exit(-1)

if __name__ == "__main__":
	main()

