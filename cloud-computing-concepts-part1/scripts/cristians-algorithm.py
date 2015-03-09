__author__ = 'grokrz'

app_network_delay, network_app_delay = (5, 4)
app_network_delay_server, network_app_delay_server = (2, 3)

min1 = app_network_delay + network_app_delay_server
min2 = network_app_delay + app_network_delay_server
RTT = 26
t = 50

accuracy = (RTT - min1 - min2) / 2
range_t = [t + min2, t + RTT - min1]

print "Range: " + str(range_t)
print "Accuracy: " + str(accuracy)