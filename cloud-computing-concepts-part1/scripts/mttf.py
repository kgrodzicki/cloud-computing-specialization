__author__ = 'grokrz'

nr_of_machines_in_DC = 20000.00
mttf_of_single_server = 48

next_failure = mttf_of_single_server * 30 * 24 / nr_of_machines_in_DC

print "MTTF (mean time to failure) until next failure = " + "{0:.2f}".format(next_failure)
