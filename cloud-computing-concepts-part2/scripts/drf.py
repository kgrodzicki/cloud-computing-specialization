__author__ = 'grokrz'

job_0 = (1, 3)
job_1 = (3, 2)
cluster = (20, 40)


def check_dominant(job):
    cpu = job[0] / float(cluster[0])
    ram = job[1] / float(cluster[1])
    if cpu > ram:
        dominant = 'cpu'
    else:
        dominant = 'ram'
    return cpu, ram, dominant


job1_details = check_dominant(job_0)
print "Job: {} CPU:{} RAM:{} DOMINANT:{}".format(job_0, job1_details[0], job1_details[1], job1_details[2])

job2_details = check_dominant(job_1)
print "Job: {} CPU:{} RAM:{} DOMINANT:{}".format(job_1, job2_details[0], job2_details[1], job2_details[2])

det = (12, 8)
print det[0] * 1 / 18.0 == det[1] * 1 / 12.0
