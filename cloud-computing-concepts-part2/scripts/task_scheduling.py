__author__ = 'grokrz'


def get_lowest_average_processing_time(list_of_tasks):
    buf = list()
    list_of_tasks.sort()
    sum_last = 0
    for task in list_of_tasks:
        sum_last += task
        buf.append(sum_last)
    return sum(buf) / float(len(buf))


print "Lowest processing time: {}".format(get_lowest_average_processing_time([10, 15, 4, 15, 23]))
print "Lowest processing time: {}".format(get_lowest_average_processing_time([5, 4, 4, 3, 2, 1, 1]))
