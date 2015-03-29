__author__ = 'grokrz'

availability_of_single_replica = 0.85
server_failure_probability = 1 - availability_of_single_replica
max_number_of_servers = 8

print server_failure_probability


def get_availability_of_object(nr_of_servers):
    if nr_of_servers == 1:
        return 1 - server_failure_probability
    return 1 - pow(server_failure_probability, nr_of_servers)


for i in range(1, max_number_of_servers):
    print "{} server(s): {}%".format(i, get_availability_of_object(i))
