__author__ = 'grokrz'

from datetime import datetime, date, time

now = date.today()

freshness_interval = 4

tc_last_modified = datetime.combine(now, time(10, 11, 35))
tc = datetime.combine(now, time(10, 12, 3))

ts = datetime.combine(now, time(10, 11, 36))

t_access = datetime.combine(now, time(10, 12, 25))

print "T-Tc < t -> {}".format((t_access - tc).seconds < freshness_interval)
print "Tmc == Tms -> {}".format(tc_last_modified == ts)
